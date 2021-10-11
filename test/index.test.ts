import { Entity } from 'sourced';
import { Repository } from '../src/index';

const mockSavedTestEntityEvents = {
    Items: [
        {
            method: 'init',
            data: undefined,
            timestamp: 1606703172298,
            version: 1,
        },
        {
            method: 'addOne',
            data: undefined,
            timestamp: 1606703172298,
            version: 2,
        },
        {
            method: 'addOne',
            data: undefined,
            timestamp: 1606703172298,
            version: 3,
        },
    ],
};

const ArcTablesDynamoMock = {
    testentityevents: {
        put: jest.fn().mockResolvedValue(undefined),
        get: jest.fn().mockResolvedValue(mockSavedTestEntityEvents),
        query: jest.fn().mockResolvedValue(mockSavedTestEntityEvents),
    },
    testentitysnapshots: {
        put: jest.fn().mockResolvedValue(undefined),
        query: jest.fn().mockResolvedValue({ Items: [] }),
        get: jest.fn().mockResolvedValue({
            _eventsCount: 0,
            snapshotVersion: 3,
            timestamp: 1606703254404,
            version: 3,
            total: 2,
            id: 'test-id',
        }),
    },
};

class TestEntity extends Entity {
    id: string;
    total: number;

    constructor(snapshot?: any, events?: any) {
        super();

        this.total = 0;
        this.id = '';

        this.rehydrate(snapshot, events);
    }

    init() {
        this.id = 'test-id';
        this.digest('init', {});
    }

    addOne() {
        this.total += 1;
        this.digest('addOne', {});
        this.enqueue('oneAdded');
    }
}

describe('Repository tests', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    it('should create a Repository', () => {
        const repo = new Repository(TestEntity);

        expect(repo).toBeInstanceOf(Repository);
    });

    it('should commit events and snapshot when forceSnapshot: true', async () => {
        const testEntity = new TestEntity();
        testEntity.init();
        testEntity.addOne();
        testEntity.addOne();

        const repo = new Repository(TestEntity);

        const oneAdded = new Promise((resolve) =>
            testEntity.once('oneAdded', resolve),
        );

        await repo.init();
        await repo.commit(testEntity as any, { forceSnapshot: true });

        await oneAdded;

        expect(
            ArcTablesDynamoMock.testentityevents.put.mock.calls[0][0].method,
        ).toEqual('init');
        expect(
            ArcTablesDynamoMock.testentityevents.put.mock.calls[1][0].method,
        ).toEqual('addOne');
        expect(
            ArcTablesDynamoMock.testentityevents.put.mock.calls[2][0].method,
        ).toEqual('addOne');
        expect(
            ArcTablesDynamoMock.testentitysnapshots.put.mock.calls[0][0].total,
        ).toEqual(2);
    });

    it('should only commit events when snapshot frequency is not met', async () => {
        const testEntity = new TestEntity();
        testEntity.init();
        testEntity.addOne();
        testEntity.addOne();

        const repo = new Repository(TestEntity);

        const oneAdded = new Promise((resolve) =>
            testEntity.once('oneAdded', resolve),
        );

        await repo.init();
        await repo.commit(testEntity);

        await oneAdded;

        expect(
            ArcTablesDynamoMock.testentityevents.put.mock.calls[0][0].method,
        ).toEqual('init');
        expect(
            ArcTablesDynamoMock.testentityevents.put.mock.calls[1][0].method,
        ).toEqual('addOne');
        expect(
            ArcTablesDynamoMock.testentityevents.put.mock.calls[2][0].method,
        ).toEqual('addOne');
        expect(
            ArcTablesDynamoMock.testentitysnapshots.put,
        ).not.toHaveBeenCalled();
    });

    it('should fire enqueued events after successful commit', async () => {
        const testEntity = new TestEntity();
        const repo = new Repository(TestEntity);
        await repo.init();

        testEntity.init();
        testEntity.addOne();
        testEntity.addOne();

        let emitted = 0;

        testEntity.on('oneAdded', () => {
            emitted++;
        });

        expect(testEntity.eventsToEmit.length).toBe(2);

        await repo.commit(testEntity);

        expect(emitted).toBe(2);
    });

    it('should be able to retrieve entity that only has events', async () => {
        const repo = new Repository(TestEntity);
        await repo.init();

        const testEntity = await repo.get('test-id');

        expect(testEntity.id).toBe('test-id');
        expect(testEntity.total).toBe(2);
    });
    it.todo(
        'should retrieve latest snapshot and events for entity and merge together into model',
    );
    it.todo('should throw error when commit fails for events');
    it.todo('should throw when commit fails for events');
});
