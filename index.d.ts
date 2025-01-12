/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export interface DeltaTableOptions {
  /** Specify the version to load either as an integer or an ISO-8601/RFC-3339 timestamp. */
  version?: number | string
  /**
   * Indicates whether DeltaTable should track files.
   * This defaults to `true`
   *
   * Some append-only applications might have no need of tracking any files.
   * Hence, DeltaTable will be loaded with significant memory reduction.
   */
  withoutFiles?: boolean
  /** Set options used to initialize storage backend. */
  storageOptions?: AWSConfigKeyCredentials | AWSConfigKeyProfile
}
export interface AWSConfigKeyCredentials {
  awsRegion: string
  awsAccessKeyId: string
  awsSecretAccessKey: string
  awsSessionToken?: string
}
export interface AWSConfigKeyProfile {
  awsRegion: string
  awsProfile: string
}
export class QueryBuilder {
  constructor()
  /**
   * Register the given [DeltaTable] into the [SessionContext] using the provided `table_name`
   *
   * Once called, the provided `delta_table` will be referenceable in SQL queries so long as
   * another table of the same name is not registered over it.
   */
  register(tableName: string, deltaTable: DeltaTable): QueryBuilder
  /** Prepares the sql query to be executed. */
  sql(sqlQuery: string): QueryResult
}
export class QueryResult {
  constructor(queryBuilder: QueryBuilder, sqlQuery: string)
  /** Print the first 25 rows returned by the SQL query */
  show(): Promise<void>
  stream(): ReadableStream<Buffer>
  fetchAll(): Promise<Buffer>
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
   * * `tableUri` - Path of the Delta table
   * * `options` - an object of the options to use for the storage backend
   */
  constructor(tableUri: string, options?: DeltaTableOptions | undefined | null)
  /**
   * Currently it'll fail if the first entry in your _delta_log is a CRC file.
   * See https://github.com/delta-io/delta-rs/issues/3115
   * Fix here: https://github.com/delta-io/delta-rs/pull/3122
   */
  static isDeltaTable(tableUri: string, storageOptions?: AWSConfigKeyCredentials | AWSConfigKeyProfile | undefined | null): Promise<boolean>
  /** Build the DeltaTable and load its state */
  load(): Promise<void>
  /** Get the version of the Delta table. */
  version(): number
  /** Get the current schema of the Delta table. */
  schema(): string
}
