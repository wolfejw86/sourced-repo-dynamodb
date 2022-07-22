import { Entity } from 'sourced';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  TransactWriteCommandInput,
  QueryCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

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
  client?: DynamoDBClient | DynamoDBDocumentClient;
}

export class Repository<TEntity extends Entity & { id: string }> {
  private snapshotFrequency: number;
  private snapshotPrefix: string;
  private eventPrefix: string;
  private hashKey: string;
  private sortKey: string;
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
    this.hashKey = this.options.hashKey;
    this.sortKey = this.options.sortKey;

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

  /**
   * Retrieves all entities for each of their respective ids
   */
  async getAll(ids: string[]) {
    return Promise.all(ids.map((id) => this.get(id)));
  }

  async commit(
    entity: TEntity,
    options: { forceSnapshots: boolean } = {
      forceSnapshots: false,
    },
  ) {
    if (!entity.id) {
      throw new Error(
        `Cannot commit an entity of type [${this.entityType.name}] without an [id] property.`,
      );
    }

    if (
      !this.shouldWriteSnapshot(entity) &&
      !options.forceSnapshots &&
      entity.newEvents.length === 1
    ) {
      const event = entity.newEvents.pop();

      await this.client.send(
        new PutCommand({
          TableName: this.options.dynamoTable,
          Item: {
            [this.hashKey]: `${this.entityName}#${entity.id}`,
            [this.sortKey]: `${this.eventPrefix}${Repository.getPaddedVersion(
              event.version,
            )}`,
            data: event,
          },
        }),
      );

      this.emitEvents(entity);

      return;
    }

    await this.client.send(
      new TransactWriteCommand({
        TransactItems: this.buildTransactWriteItemsForCommit(entity, options),
      }),
    );

    this.emitEvents(entity);

    entity.newEvents = [];
  }

  async commitAll(
    entities: TEntity[],
    options: { forceSnapshots: boolean } = {
      forceSnapshots: false,
    },
  ) {
    if (!entities.every((e) => !!e.id)) {
      throw new Error(
        `Cannot commit an entity of type [${this.entityType.name}] without an [id] property.`,
      );
    }

    const writeItems = entities
      .map((e) => this.buildTransactWriteItemsForCommit(e, options))
      .flat();

    await this.client.send(
      new TransactWriteCommand({
        TransactItems: writeItems,
      }),
    );

    entities.forEach((e) => {
      this.emitEvents(e);
      e.newEvents = [];
    });
  }

  private buildTransactWriteItemsForCommit(
    entity: TEntity,
    options: { forceSnapshots: boolean },
  ) {
    const writeItems: TransactWriteCommandInput['TransactItems'] =
      entity.newEvents.map((event) => {
        return {
          Put: {
            TableName: this.options.dynamoTable,
            Item: {
              [this.hashKey]: `${this.entityName}#${entity.id}`,
              [this.sortKey]: `${this.eventPrefix}${Repository.getPaddedVersion(
                event.version,
              )}`,
              data: event,
            },
          },
        };
      });

    if (options.forceSnapshots || this.shouldWriteSnapshot(entity)) {
      writeItems.push({
        Put: {
          TableName: this.options.dynamoTable,
          Item: {
            [this.hashKey]: `${this.entityName}#${entity.id}`,
            [this.sortKey]: `${
              this.snapshotPrefix
            }${Repository.getPaddedVersion(entity.snapshotVersion)}`,
            data: entity.snapshot(),
          },
        },
      });
    }

    return writeItems;
  }

  private shouldWriteSnapshot(entity: TEntity) {
    return entity.version >= entity.snapshotVersion + this.snapshotFrequency;
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
        '#pk': this.hashKey,
        '#sk': this.sortKey,
      },
    };
  }
}
