import { PendingEvent, SpecRealtimeClientOptions, StringKeyMap, TableOptions } from './lib/types'
import Table from './lib/table'
import config from './lib/config'
import logger from './lib/logger'
import createSubscriber, { Subscriber } from 'pg-listen'
import { Pool } from 'pg'
import { formatTablePath } from './lib/utils/formatters'

const DEFAULT_OPTIONS = {
    user: config.defaults.DB_USER,
    password: config.defaults.DB_PASSWORD,
    host: config.defaults.DB_HOST,
    port: config.defaults.DB_P0RT,
    database: config.defaults.DB_NAME,
    minPoolConnections: config.defaults.MIN_POOL_CONNECTIONS,
    maxPoolConnections: config.defaults.MAX_POOL_CONNECTIONS,
    channel: config.defaults.CHANNEL,
    bufferInterval: config.defaults.BUFFER_INTERVAL,
    maxBufferSize: config.defaults.MAX_BUFFER_SIZE,
    onError: (err: Error) => {},
}

/**
 * Spec Realtime Client.
 *
 * A Javascript client for subscribing to realtime changes in
 * your Postgres tables broadcasted by the triggers Spec adds.
 */
export default class SpecRealtimeClient {
    options: SpecRealtimeClientOptions

    pool: Pool

    subscriber: Subscriber

    tables: { [key: string]: Table }

    get connectionConfig(): StringKeyMap {
        const { user, password, host, port, database } = this.options
        return { user, password, host, port, database }
    }

    get defaultTableOptions(): TableOptions {
        const { bufferInterval, maxBufferSize, onError } = this.options
        return {
            schema: config.defaults.TABLE_SCHEMA,
            bufferInterval,
            maxBufferSize,
            onError,
        }
    }

    get channel(): string {
        return this.options.channel!
    }

    /**
     * Create a new client instance.
     */
    constructor(options?: SpecRealtimeClientOptions) {
        this.options = { ...DEFAULT_OPTIONS, ...(options || {}) }
        this.pool = this._createConnectionPool()
        this.subscriber = this._createSubscriber()
        this.tables = {}
    }

    /**
     * Subscribe to the configured Postgres notification channel.
     */
    async listen() {
        try {
            await this.subscriber.connect()
            await this.subscriber.listenTo(this.channel)
        } catch (err) {
            logger.error(`Realtime connection error: ${err}`)
            this._onError(err as Error)
        }
    }

    /**
     * Create and return a new table reference.
     */
    table(name: string, options?: TableOptions): Table {
        options = { ...this.defaultTableOptions, ...(options || {}) }
        const path = formatTablePath(options.schema!, name)

        let table = this.tables[path]
        if (table) return table

        table = new Table(name, this.pool, options)
        this.tables[path] = table

        return table
    }

    _createSubscriber(): Subscriber {
        const subscriber = createSubscriber(this.connectionConfig)

        // Register error handler.
        subscriber.events.on('error', (err) => {
            logger.error(`Realtime table event error: ${err}`)
            this._onError(err)
        })

        // Register event handler.
        subscriber.notifications.on(this.channel, (event) => event && this._onEvent(event))

        return subscriber
    }

    _createConnectionPool(): Pool {
        // Create new connection pool with min/max config.
        const pool = new Pool({
            ...this.connectionConfig,
            min: this.options.minPoolConnections!,
            max: this.options.maxPoolConnections!,
        })

        // Register error handler.
        pool.on('error', (err) => {
            logger.error(`Realtime pg pool error: ${err}`)
            this._onError(err)
        })

        return pool
    }

    _onEvent(event: PendingEvent) {
        const path = formatTablePath(event.schema, event.table)
        const table = this.tables[path]
        table && table._newPendingEvent(event)
    }

    _onError(err: Error) {
        const handler = this.options.onError
        handler && handler(err)
    }
}
