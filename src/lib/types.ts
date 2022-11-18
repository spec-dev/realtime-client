export type StringKeyMap = { [key: string]: any }

export interface SpecRealtimeClientOptions {
    user?: string
    password?: string
    host?: string
    port?: number
    database?: string
    minPoolConnections?: number
    maxPoolConnections?: number
    channel?: string
    bufferInterval?: number
    maxBufferSize?: number
    onError?: (error: Error) => void
}

export interface TableOptions {
    schema?: string
    bufferInterval?: number
    maxBufferSize?: number
    onError?: (error: Error) => void
}

export enum Operation {
    INSERT = 'INSERT',
    UPDATE = 'UPDATE',
    DELETE = 'DELETE',
    ALL = '*',
}

export type TableOperationSubs = { [key: string]: EventCallback }

export interface PendingEvent {
    timestamp: string
    operation: Operation
    schema: string
    table: string
    data?: StringKeyMap
    primaryKeyData?: StringKeyMap
    columnNamesChanged?: string[]
}

export interface Event {
    timestamp: string
    operation: Operation
    schema: string
    table: string
    data: StringKeyMap
    columnNamesChanged?: string[]
}

export type EventCallback = (event: Event | Event[]) => void
