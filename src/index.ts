import SpecRealtimeClient from './client'
import { SpecRealtimeClientOptions } from './lib/types'

/**
 * Creates a new Spec Realtime Client.
 */
const createRealtimeClient = (options?: SpecRealtimeClientOptions): SpecRealtimeClient => {
    return new SpecRealtimeClient(options)
}

export { createRealtimeClient, SpecRealtimeClient }
export * from './lib/types'
