/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export interface JsDeltaTableOptions {
  version?: number
  timestamp?: number
  withoutFiles?: boolean
}
export class DeltaTable {
  /**
   * Create the Delta table from a path with an optional version.
   * Multiple StorageBackends are currently supported: AWS S3 and local URI.
   * Depending on the storage backend used, you could provide options values using the `options` parameter.
   *
   * This will not load the log, i.e. the table is not initialized. To get an initialized
   * table use the `load` function.
   *
   * # Arguments
   *
   * * `table_uri` - Path of the Delta table
   * * `options` - an object of the options to use for the storage backend
   */
  constructor(tableUri: string, options?: JsDeltaTableOptions | undefined | null)
  /** Build the DeltaTable and load its state */
  load(): Promise<unknown>
  /** Get the version of the Delta table. */
  version(): number
  /** Get the current schema of the Delta table. */
  schema(): string
  query(query: string): Promise<Buffer>
}
