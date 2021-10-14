import { Entity } from 'sourced';
import { Repository } from '../src/index';
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  paginateScan,
} from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  TransactWriteCommandInput,
  QueryCommand,
  DeleteCommand,
  ScanCommand,
} from '@aws-sdk/lib-dynamodb';

const TEST_ENDPOINT = 'http://localhost:8000';
const TEST_REGION = 'us-east-1';
const TEST_TABLE_NAME = 'EntityTesting';

const baseClient = new DynamoDBClient({
  region: TEST_REGION,
  endpoint: TEST_ENDPOINT,
});
const client = DynamoDBDocumentClient.from(baseClient, {
  marshallOptions: {
    // Whether to automatically convert empty strings, blobs, and sets to `null`.
    convertEmptyValues: false, // false, by default.
    // Whether to remove undefined values while marshalling.
    removeUndefinedValues: true, // false, by default.
    // Whether to convert typeof object to map attribute.
    convertClassInstanceToMap: true, // false, by default.
  },
});

// create table we will use
describe('sourced-repo-dynamodb tests', () => {
  beforeAll(async () => {
    await client.send(
      new CreateTableCommand({
        TableName: TEST_TABLE_NAME,
        KeySchema: [
          {
            AttributeName: 'PK',
            KeyType: 'HASH',
          },
          {
            AttributeName: 'SK',
            KeyType: 'RANGE',
          },
        ],
        AttributeDefinitions: [
          {
            AttributeName: 'PK',
            AttributeType: 'S',
          },
          {
            AttributeName: 'SK',
            AttributeType: 'S',
          },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1,
        },
      }),
    );
  });

  // delete table at the end
  afterAll(async () => {
    await client.send(
      new DeleteTableCommand({
        TableName: TEST_TABLE_NAME,
      }),
    );
  });

  // clear items from test table after each test - brute force but works
  afterEach(async () => {
    const scanner = paginateScan(
      {
        client: baseClient,
      },
      { TableName: TEST_TABLE_NAME },
    );

    for await (const scanResult of scanner) {
      const itemsToDelete = scanResult.Items || [];
      await Promise.all(
        itemsToDelete.map((item) => {
          return client.send(
            new DeleteCommand({
              TableName: TEST_TABLE_NAME,
              Key: {
                PK: item.PK.S,
                SK: item.SK.S,
              },
            }),
          );
        }),
      );
    }
  });

  class TestEntity extends Entity {
    id: string;
    total: number;
    stuff: any[];

    constructor(snapshot?: any, events?: any) {
      super();

      this.total = 0;
      this.id = '';
      this.stuff = [];

      this.rehydrate(snapshot, events);
    }

    init() {
      this.id = 'test-id';
      this.digest('init', {});
    }

    addOne() {
      this.total += 1;
      this.digest('addOne', {});
    }

    add(params: { amount: number }) {
      this.total += params.amount;
      this.digest('add', params);
    }

    subtract(params: { amount: number }) {
      this.total -= params.amount;
      this.digest('subtract', params);
    }

    emitTest(params: any) {
      this.stuff.push(params);
      this.enqueue('emitTest', params);
      this.digest('emitTest', params);
    }
  }

  const repo = new Repository(TestEntity, {
    dynamoTable: TEST_TABLE_NAME,
    endpoint: TEST_ENDPOINT,
  });

  it('should create a Repository', () => {
    const repo = new Repository(TestEntity, { dynamoTable: 'TestTable' });

    expect(repo).toBeInstanceOf(Repository);
  });

  it('should commit events and snapshot when forceSnapshot: true', async () => {
    const testEntity = new TestEntity();
    testEntity.init();
    testEntity.addOne();
    testEntity.addOne();
    testEntity.subtract({ amount: 1 });

    await repo.commit(testEntity, { forceSnapshot: true });

    const fetchedEntity = await repo.get(testEntity.id);

    expect(fetchedEntity.snapshotVersion).toBe(4);
    expect(fetchedEntity.snapshot().total).toBe(1);
    expect(fetchedEntity.snapshot().version).toBe(4);
  });

  it('should only commit events when snapshot frequency is not met', async () => {
    const testEntity = new TestEntity();
    testEntity.init();
    testEntity.addOne();
    testEntity.addOne();

    await repo.commit(testEntity);

    const fetchedEntity = await repo.get(testEntity.id);

    expect(fetchedEntity.snapshotVersion).toBe(0);
    expect(fetchedEntity.version).toBe(3);
    expect(fetchedEntity.total).toBe(2);
  });

  it('should fire enqueued events after successful commit', (done) => {
    const testEntity = new TestEntity();

    testEntity.init();
    testEntity.addOne();
    testEntity.addOne();
    testEntity.emitTest({ someStuff: true });

    testEntity.on('emitTest', () => {
      // it will only get here if it emits correctly
      done();
    });

    expect(testEntity.eventsToEmit.length).toBe(1);

    repo.commit(testEntity);
  });

  it('should be able to retrieve latest snapshot and events when there is a larger amount of events', async () => {
    const entity = new TestEntity();

    entity.init();

    for (let i = 0; i < 20; i++) {
      entity.add({ amount: i });
    }

    await repo.commit(entity);

    const fetchedEntity = await repo.get(entity.id);

    expect(fetchedEntity.id).toBe(entity.id);
  });
});
