export function formatTablePath(schema: string, name: string): string {
    return [schema, name].join('.')
}

export function formatRelation(relation: string): string {
    return relation
        .split('.')
        .map((v) => `"${v}"`)
        .join('.')
}
