export type StringKeyMap = { [key: string]: any }

export interface SpecRealtimeClientOptions {
    user?: string
    password?: string
    host?: string
    port?: number
    database?: string
    channel?: string
    bufferInterval?: number
    maxBufferSize?: number
}

// export type EventCallback =
//     | ((event: SpecEvent<StringKeyMap | StringKeyMap[]>) => void)
//     | ((events: SpecEvent<StringKeyMap | StringKeyMap[]>[]) => void)
//     | ((data: any) => void)
