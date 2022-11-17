import { ev } from './utils/env'

export default {
    DEBUG: ['true', true].includes(ev('DEBUG')),
}