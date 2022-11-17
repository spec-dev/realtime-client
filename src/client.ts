import { SpecRealtimeClientOptions, StringKeyMap } from './lib/types'
import logger from './lib/logger'

const DEFAULT_OPTIONS = {
    user: 'postgres',
    password: 'postgres',
    host: 'localhost',
    port: 5432,
    database: 'postgres',
    channel: 'spec_data_change',
    bufferInterval: 30,
    maxBufferSize: 1000,
}

/**
 * Spec Realtime Client.
 *
 * A Javascript client for subscribing to realtime changes in 
 * your Postgres tables broadcasted by the triggers Spec adds.
 */
export default class SpecRealtimeClient {

    options: SpecRealtimeClientOptions

    /**
     * Create a new client instance.
     */
    constructor(options?: SpecRealtimeClientOptions) {
        this.options = { ...DEFAULT_OPTIONS, ...(options || {}) }
    }
}
