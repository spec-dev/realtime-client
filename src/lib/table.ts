import {
    TableOptions,
    Operation,
    PendingEvent,
    Event,
    EventCallback,
    StringKeyMap,
    TableOperationSubs,
} from './types'
import config from './config'
import debounce from './utils/debounce'
import uid from 'short-uuid'
import logger from './logger'
import { formatRelation, formatTablePath } from './utils/formatters'
import { Pool } from 'pg'

const DEFAULT_OPTIONS = {
    schema: config.defaults.TABLE_SCHEMA,
    bufferInterval: config.defaults.BUFFER_INTERVAL,
    maxBufferSize: config.defaults.MAX_BUFFER_SIZE,
    onError: (err: Error) => {},
}

/**
 * A table that can be subscribed to.
 */
export default class Table {
    name: string

    pool: Pool

    options: TableOptions

    buffer: PendingEvent[]

    processBuffer: any

    subs: { [key: string]: TableOperationSubs }

    get schema(): string {
        return this.options.schema!
    }

    get tablePath(): string {
        return formatTablePath(this.schema, this.name)
    }

    get bufferInterval(): number {
        return this.options.bufferInterval!
    }

    get maxBufferSize(): number {
        return this.options.maxBufferSize!
    }

    /**
     * Create a new Table instance.
     */
    constructor(name: string, pool: Pool, options?: TableOptions) {
        this.name = name
        this.pool = pool
        this.options = { ...DEFAULT_OPTIONS, ...(options || {}) }
        this.subs = {}
        this.buffer = []
        this.processBuffer = debounce(() => this._processBuffer(), this.bufferInterval)
    }

    on(operation: Operation | string, cb: EventCallback): string | null {
        if (!cb) return null
        switch (operation) {
            case Operation.INSERT:
                return this.onInsert(cb)
            case Operation.UPDATE:
                return this.onUpdate(cb)
            case Operation.DELETE:
                return this.onDelete(cb)
            case Operation.ALL:
                return this.onAll(cb)
            default:
                logger.error(`Unknown subscription event: ${operation}`)
                return null
        }
    }

    off(subscriptionId: string) {
        let done = false
        for (const operation in this.subs) {
            const opSubs = this.subs[operation] || {}
            for (const id in opSubs) {
                if (id === subscriptionId) {
                    delete this.subs[operation][id]
                    done = true
                    break
                }
            }
            if (done) break
        }
    }

    onInsert(cb: EventCallback): string {
        return this._newSub(Operation.INSERT, cb)
    }

    onUpdate(cb: EventCallback): string {
        return this._newSub(Operation.UPDATE, cb)
    }

    onDelete(cb: EventCallback): string {
        return this._newSub(Operation.DELETE, cb)
    }

    onAll(cb: EventCallback): string {
        return this._newSub(Operation.ALL, cb)
    }

    _newSub(operation: Operation, cb: EventCallback): string {
        this.subs[operation] = this.subs[operation] || {}
        const subscriptionId = uid.generate()
        this.subs[operation][subscriptionId] = cb
        return subscriptionId
    }

    _newPendingEvent(event: PendingEvent) {
        // If debouncing isn't configured, handle event immediately.
        if (!this.bufferInterval) {
            this._processPendingEvents([event])
            return
        }

        this.buffer.push(event)

        // Immediately process events if buffer hits max capacity.
        if (this.buffer.length >= this.maxBufferSize) {
            this.processBuffer.flush()
            return
        }

        // Debounce.
        this.processBuffer()
    }

    async _processBuffer() {
        const pendingEvents = [...this.buffer]
        this.buffer = []
        await this._processPendingEvents(pendingEvents)
    }

    async _processPendingEvents(pendingEvents: PendingEvent[]) {
        // Get subscriptions for each operation type and use those
        // to figure out which events to process.
        const insertCallbacks = Object.values(this.subs[Operation.INSERT] || {})
        const updateCallbacks = Object.values(this.subs[Operation.UPDATE] || {})
        const deleteCallbacks = Object.values(this.subs[Operation.DELETE] || {})
        const allCallbacks = Object.values(this.subs[Operation.ALL] || {})
        const processInserts = !!insertCallbacks.length || !!allCallbacks.length
        const processUpdates = !!updateCallbacks.length || !!allCallbacks.length
        const processDeletes = !!deleteCallbacks.length || !!allCallbacks.length

        // Split events into those that are already resolved (have full record data),
        // and those that need resolution via given primary key data.
        const [events, eventsNeedingResolution] = this._filterAndIndexPendingEvents(
            pendingEvents,
            processInserts,
            processUpdates,
            processDeletes
        )

        // Resolve records for the events that only provided primary keys.
        if (eventsNeedingResolution.length) {
            const resolvedEvents = await this._resolveEventRecords(eventsNeedingResolution)
            resolvedEvents.length && events.push(...resolvedEvents)
        }

        // Resort events now that all are fully resolved.
        const allEvents = events
            .sort((a, b) => a.i - b.i)
            .map((event) => {
                delete event.i
                return event as Event
            })
        if (!allEvents.length) return

        // Split final events by operation.
        const [inserts, updates, deletes] = this._splitEventsByOperation(allEvents)

        // Invoke all event callbacks.
        if (inserts.length && insertCallbacks.length) {
            const arg = this.bufferInterval > 0 ? inserts : inserts[0]
            insertCallbacks.forEach((cb) => cb && cb(arg))
        }
        if (updates.length && updateCallbacks.length) {
            const arg = this.bufferInterval > 0 ? updates : updates[0]
            updateCallbacks.forEach((cb) => cb && cb(arg))
        }
        if (deletes.length && deleteCallbacks.length) {
            const arg = this.bufferInterval > 0 ? deletes : deletes[0]
            deleteCallbacks.forEach((cb) => cb && cb(arg))
        }
        if (allCallbacks.length) {
            const arg = this.bufferInterval > 0 ? allEvents : allEvents[0]
            allCallbacks.forEach((cb) => cb && cb(arg))
        }
    }

    _filterAndIndexPendingEvents(
        pendingEvents: PendingEvent[],
        processInserts: boolean,
        processUpdates: boolean,
        processDeletes: boolean
    ): StringKeyMap[][] {
        const events = []
        const eventsNeedingResolution = []
        let i = 0
        for (const pendingEvent of pendingEvents) {
            // Filter events based on which operations actually have subscriptions.
            if (
                (pendingEvent.operation === Operation.INSERT && !processInserts) ||
                (pendingEvent.operation === Operation.UPDATE && !processUpdates) ||
                (pendingEvent.operation === Operation.DELETE && !processDeletes)
            )
                continue

            // Split events into those already fully resolved and those that need resolution.
            if (pendingEvent.data) {
                events.push({ i, ...pendingEvent })
            } else if (pendingEvent.primaryKeyData) {
                eventsNeedingResolution.push({ i, ...pendingEvent })
            }
            i++
        }
        return [events, eventsNeedingResolution]
    }

    async _resolveEventRecords(eventsNeedingResolution: StringKeyMap[]): Promise<StringKeyMap[]> {
        // Sort primary key column names.
        const sortedPrimaryKeyColNames = Object.keys(
            eventsNeedingResolution[0].primaryKeyData
        ).sort()

        const eventsByPrimaryKeys: StringKeyMap = {}
        const eventPrimaryKeyData = []
        for (const event of eventsNeedingResolution) {
            const primaryKeyData = event.primaryKeyData || {}
            eventPrimaryKeyData.push(primaryKeyData)

            // Map events by their sorted primary key values.
            const uniqueRecordKey = sortedPrimaryKeyColNames.map((k) => primaryKeyData[k]).join(':')
            eventsByPrimaryKeys[uniqueRecordKey] = eventsByPrimaryKeys[uniqueRecordKey] || []
            eventsByPrimaryKeys[uniqueRecordKey].push(event)
        }

        // Resolve records by primary keys.
        let records
        try {
            records = await this._getRecordsWhere(this.tablePath, eventPrimaryKeyData)
        } catch (err) {
            logger.error(
                `Error resolving records for realtime events in table ${this.tablePath}: ${err}`
            )
            this._onError(err as Error)
            return []
        }

        // Map the found records back to their associated events.
        for (const record of records) {
            const uniqueRecordKey = sortedPrimaryKeyColNames.map((k) => record[k]).join(':')
            const eventsAssociatedWithRecord = eventsByPrimaryKeys[uniqueRecordKey] || []
            if (!eventsAssociatedWithRecord.length) continue
            for (let i = 0; i < eventsAssociatedWithRecord.length; i++) {
                eventsByPrimaryKeys[uniqueRecordKey][i].data = record
                delete eventsByPrimaryKeys[uniqueRecordKey][i].primaryKeyData
            }
        }

        return Object.values(eventsByPrimaryKeys).flat()
    }

    async _getRecordsWhere(
        tablePath: string,
        whereGroups: StringKeyMap[]
    ): Promise<StringKeyMap[]> {
        const bindings = []
        const orStatements = []
        let i = 1

        // Build the query -- where (A and B) or (C and D) or ...
        for (const params of whereGroups) {
            if (!params) continue
            const andStatements = []
            for (const colName in params) {
                andStatements.push(`${formatRelation(colName)} = $${i}`)
                bindings.push(params[colName])
                i++
            }
            orStatements.push(andStatements.join(' and '))
        }
        const conditions = orStatements.map((s) => `(${s})`).join(' or ')
        const query = `select * from ${formatRelation(tablePath)} where ${conditions}`

        // Acquire a connection from the pool.
        let conn
        try {
            conn = await this.pool.connect()
        } catch (err) {
            conn && conn.release()
            throw `Error acquiring a pg connection from the pool: ${err}`
        }

        // Perform that shit.
        let result, error
        try {
            result = await conn.query(query, bindings)
        } catch (err) {
            error = err
        } finally {
            conn.release()
        }
        if (error) throw `Error performing query: ${error}`
        if (!result) throw `Empty query result`

        return result.rows || []
    }

    _splitEventsByOperation(allEvents: Event[]): Event[][] {
        const inserts = []
        const updates = []
        const deletes = []
        for (const event of allEvents) {
            switch (event.operation) {
                case Operation.INSERT:
                    inserts.push(event)
                    break
                case Operation.UPDATE:
                    updates.push(event)
                    break
                case Operation.DELETE:
                    deletes.push(event)
                    break
                default:
                    break
            }
        }
        return [inserts, updates, deletes]
    }

    _onError(err: Error) {
        const handler = this.options.onError
        handler && handler(err)
    }
}
