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
  awsRegion: string;
  snapshotFrequency: number;
  dynamoIndex?: string;
}

interface RepositoryInitOptions {
  dynamoTable: string;
  awsRegion?: string;
  snapshotFrequency?: number;
  dynamoIndex?: string;
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

  /**
   * TODO - figure out if it makes sense to keep these as any or to type
   * as reference objects to ddb actions
   */
  private events: any;
  private snapshots: any;
  private options: RepositoryOptions;
  private entityName: string;
  private client: DynamoDBDocumentClient;

  constructor(
    private entityType: TEntityType<TEntity>,
    options: RepositoryInitOptions,
  ) {
    this.options = Object.assign(
      {
        snapshotFrequency: 10,
        awsRegion: 'us-east-1',
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

  async get(id: string) {
    const [snapshotResult, eventsResult] = await Promise.all([
      this.client.send(
        new QueryCommand({
          TableName: this.options.dynamoTable,
          KeyConditionExpression: '#pk = :pk AND begins_with(#sk, :sk)',
          ExpressionAttributeNames: {
            '#pk': 'PK',
            '#sk': 'SK',
          },
          ExpressionAttributeValues: {
            ':pk': `${this.entityName}#${id}`,
            ':sk': this.snapshotPrefix,
          },
          ScanIndexForward: false,
          Limit: 1,
        }),
      ),
      this.client.send(
        new QueryCommand({
          TableName: this.options.dynamoTable,
          KeyConditionExpression: '#pk = :pk AND begins_with(#sk, :sk)',
          ExpressionAttributeNames: {
            '#pk': 'PK',
            '#sk': 'SK',
          },
          ExpressionAttributeValues: {
            ':pk': `${this.entityName}#${id}`,
            ':sk': this.eventPrefix,
          },
          ScanIndexForward: false,
          Limit: this.snapshotFrequency,
        }),
      ),
    ]);

    const { data: snapshot } = snapshotResult.Items?.[0] || {};
    const events = (eventsResult.Items || [])
      .map((event) => event.data)
      .filter((event) => event.version > (snapshot?.version || 0));

    // merge snapshot and events with new entity
    return new this.entityType(snapshot, events);
  }

  async getAll(ids: string[]) {
    // get latest snapshots for each id
    // get latest events after timestamp of latest snapshot for each entity's respective id

    log(ids);

    // merge snapshot and events with new entity
    return new this.entityType();
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

    const writeItems: TransactWriteCommandInput['TransactItems'] =
      entity.newEvents.map((event) => {
        return {
          Put: {
            TableName: this.options.dynamoTable,
            Item: {
              PK: `${this.entityName}#${entity.id}`,
              SK: `${this.eventPrefix}${event.version}`,
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
            SK: `${this.snapshotPrefix}${entity.snapshotVersion}`,
            data: entity.snapshot(),
          },
        },
      });
    }

    const writeTxn = new TransactWriteCommand({
      TransactItems: writeItems,
    });

    const res = await this.client.send(writeTxn);

    entity.newEvents = [];

    return res;
  }

  async commitAll(
    entities: TEntity[],
    options: { forceSnapshots: boolean; withTransaction: boolean } = {
      forceSnapshots: false,
      withTransaction: true,
    },
  ) {
    // write events for all entities
    // write snapshots for all entities
    // throw on error

    log(options);

    return;
  }

  private async commitEvents(entity: TEntity) {
    if (entity.newEvents.length === 0) {
      return;
    }

    if (!entity.id) {
      new Error(
        `Cannot commit an entity of type [${this.entityType.name}] without an [id] property`,
      );
    }

    const events = entity.newEvents;

    events.forEach((event: any) => {
      event.id = entity.id;
    });

    log('Inserting events for %s', this.entityType.name);

    /**
     *  TODO - use batch put operations with retries OR a dynamodb transaction
     */
    // await Promise.all(events.map((event: any) => this.events.put(event)));

    log('Successfully committed events for %s', this.entityType.name);

    entity.newEvents = [];
  }

  private async commitSnapshots(
    entity: TEntity,
    options = { forceSnapshot: false },
  ) {
    if (
      options.forceSnapshot ||
      entity.version >= entity.snapshotVersion + this.snapshotFrequency
    ) {
      const snapshot = entity.snapshot();

      log('Inserting snapshot for %s', this.entityType.name);

      /**
       * TODO - map to new snapshot put dynamodb operation
       */
      await this.snapshots.put(snapshot);

      log('Successfully committed snapshot for %s', this.entityType.name);
    }
  }

  private emitEvents(entity: TEntity) {
    const eventsToEmit = entity.eventsToEmit;
    entity.eventsToEmit = [];
    eventsToEmit.forEach((event) => {
      const args = Array.from(event);

      // apply is necessary to allow outside control of event names
      this.entityType.prototype.emit.apply(entity, args);
    });
  }
}
