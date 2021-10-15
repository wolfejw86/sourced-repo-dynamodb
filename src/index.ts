import { Entity } from 'sourced';
import debug from 'debug';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  TransactWriteCommandInput,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';

const log = debug('sourced-repo-dynamodb');

interface TEntityType<TEntity extends Entity> {
  new (snapshot?: any, events?: any[]): TEntity;
}

interface RepositoryOptions {
  dynamoTable: string;
  hashKey: string;
  sortKey: string;
  awsRegion: string;
  snapshotFrequency: number;
  dynamoIndex?: string;
  endpoint?: string;
}

/**
 * TODO
 * - [ ] figure out how to fit with generic single table design - possibly accept hash and sort key as config params
 *       NOTE: right now the old package uses 2 separate tables for events and snapshots, this is bad
 * - [ ] figure out how to access latest events for an entity by timestamp with starts_with() query generically - I think you'll need an ID no matter what
 * - [ ] figure out how to access latest snapshots for entity by timestamp with starts_with() query generically
 */

export class Repository<TEntity extends Entity & { id: string }> {
  private snapshotFrequency: number;
  private snapshotPrefix: string;
  private eventPrefix: string;

  private options: RepositoryOptions;
  private entityName: string;
  private client: DynamoDBDocumentClient;

  constructor(
    private entityType: TEntityType<TEntity>,
    options: Partial<RepositoryOptions> & { dynamoTable: string },
  ) {
    this.options = Object.assign(
      {
        snapshotFrequency: 10,
        awsRegion: 'us-east-1',
        hashKey: 'PK',
        sortKey: 'SK',
      },
      options,
    );

    this.snapshotFrequency = this.options.snapshotFrequency;
    this.entityName = entityType.name.toLowerCase();
    this.eventPrefix = `${this.entityName}events#`;
    this.snapshotPrefix = `${this.entityName}snapshots#`;

    this.client = DynamoDBDocumentClient.from(
      new DynamoDBClient({
        region: this.options.awsRegion,
        endpoint: this.options.endpoint,
      }),
      {
        marshallOptions: {
          // Whether to automatically convert empty strings, blobs, and sets to `null`.
          convertEmptyValues: false, // false, by default.
          // Whether to remove undefined values while marshalling.
          removeUndefinedValues: true, // false, by default.
          // Whether to convert typeof object to map attribute.
          convertClassInstanceToMap: true, // false, by default.
        },
      },
    );
  }

  async init() {
    log('init');
  }

  /**
   * TODO - figure out how to allow consumers to get by a global secondary index
   * and make it easy to use
   */
  async get(id: string) {
    const [snapshotResult, eventsResult] = await Promise.all([
      this.client.send(this.createGetSnapshotCommand(id)),
      this.client.send(this.createGetEventCommand(id)),
    ]);

    const { data: snapshot } = snapshotResult.Items?.[0] || {};
    const events = (eventsResult.Items || [])
      .map((event) => event.data)
      .filter((event) => event.version > (snapshot?.version || 0))
      .sort((eventA, eventB) => (eventA > eventB ? 1 : -1));

    // merge snapshot and events with new entity
    return new this.entityType(snapshot, events);
  }

  async getAll(ids: string[]) {
    return Promise.all(ids.map((id) => this.get(id)));
  }

  async commit(
    entity: TEntity,
    options: { forceSnapshot: boolean } = {
      forceSnapshot: false,
    },
  ) {
    if (!entity.id) {
      new Error(
        `Cannot commit an entity of type [${this.entityType.name}] without an [id] property.`,
      );
    }

    // TODO - abstract formatting of these commands
    const writeItems: TransactWriteCommandInput['TransactItems'] =
      entity.newEvents.map((event) => {
        return {
          Put: {
            TableName: this.options.dynamoTable,
            Item: {
              PK: `${this.entityName}#${entity.id}`,
              SK: `${this.eventPrefix}${Repository.getPaddedVersion(
                event.version,
              )}`,
              data: event,
            },
          },
        };
      });

    if (
      options.forceSnapshot ||
      entity.version >= entity.snapshotVersion + this.snapshotFrequency
    ) {
      const snapshot = entity.snapshot();
      log('writing snapshot', snapshot);

      writeItems.push({
        Put: {
          TableName: this.options.dynamoTable,
          Item: {
            PK: `${this.entityName}#${entity.id}`,
            SK: `${this.snapshotPrefix}${Repository.getPaddedVersion(
              entity.snapshotVersion,
            )}`,
            data: entity.snapshot(),
          },
        },
      });
    }

    const writeTxn = new TransactWriteCommand({
      TransactItems: writeItems,
    });

    const res = await this.client.send(writeTxn);

    this.emitEvents(entity);

    entity.newEvents = [];

    return res;
  }

  async commitAll(
    entities: TEntity[],
    options: { forceSnapshots: boolean } = {
      forceSnapshots: false,
    },
  ) {
    // TODO - should I make a big transaction or do something else?

    log(options);

    return;
  }

  private emitEvents(entity: TEntity) {
    const eventsToEmit = entity.eventsToEmit;
    eventsToEmit.forEach((event) => {
      const args = Array.from(event);

      // apply is necessary to allow outside control of event names
      this.entityType.prototype.emit.apply(entity, args);
    });
    entity.eventsToEmit = [];
  }

  private static getPaddedVersion(version: number) {
    return version.toString().padStart(15, '0');
  }

  private createGetEventCommand(id: string) {
    return new QueryCommand({
      ...this.createBaseGetQuery(),
      ExpressionAttributeValues: {
        ':pk': `${this.entityName}#${id}`,
        ':sk': this.eventPrefix,
      },
      Limit: this.snapshotFrequency,
    });
  }

  private createGetSnapshotCommand(id: string) {
    return new QueryCommand({
      ...this.createBaseGetQuery(),
      ExpressionAttributeValues: {
        ':pk': `${this.entityName}#${id}`,
        ':sk': this.snapshotPrefix,
      },
      Limit: 1,
    });
  }

  private createBaseGetQuery() {
    return {
      TableName: this.options.dynamoTable,
      ScanIndexForward: false,
      KeyConditionExpression: '#pk = :pk AND begins_with(#sk, :sk)',
      ExpressionAttributeNames: {
        '#pk': 'PK',
        '#sk': 'SK',
      },
    };
  }
}
