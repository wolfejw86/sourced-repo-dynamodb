import { Entity } from 'sourced';
import debug from 'debug';

const log = debug('sourced-repo-dynamodb');

interface TEntityType<TEntity extends Entity> {
    new (snapshot?: any, events?: any[]): TEntity;
}

interface RepositoryOptions {
    snapshotFrequency?: number;
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

    private entityName: string;

    constructor(
        private entityType: TEntityType<TEntity>,
        private opts: RepositoryOptions = {
            snapshotFrequency: 10,
        },
    ) {
        this.entityName = entityType.name.toLowerCase();
        this.eventPrefix = `${this.entityName}events`;
        this.snapshotPrefix = `${this.entityName}snapshots`;
    }

    init() {
        log('init');
    }

    async get(id: string) {
        // get latest snapshot by id
        // get latest events after latest snapshot by entity's id

        log(id);

        // merge snapshot and events with new entity
        return new this.entityType();
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
        options: { forceSnapshot: boolean; withTransaction?: boolean } = {
            forceSnapshot: false,
            withTransaction: true,
        },
    ) {
        // write events
        // write snapshot
        // throw on error

        log(options);

        return;
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
