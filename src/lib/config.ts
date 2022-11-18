import { ev } from './utils/env'

export default {
    DEBUG: ['true', true].includes(ev('DEBUG')),
    defaults: {
        DB_USER: 'postgres',
        DB_PASSWORD: 'postgres',
        DB_HOST: 'localhost',
        DB_P0RT: 5432,
        DB_NAME: 'postgres',
        MIN_POOL_CONNECTIONS: 2,
        MAX_POOL_CONNECTIONS: 10,
        CHANNEL: 'spec_data_change',
        BUFFER_INTERVAL: 0, // ms
        MAX_BUFFER_SIZE: 1000,
        TABLE_SCHEMA: 'public',
    },
}
