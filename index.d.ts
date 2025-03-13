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
export interface DeltaTableMetadata {
  id: string
  name?: string
  description?: string
  partitionColumns: Array<string>
  createdTime?: number
  configuration: Record<string, string | undefined | null>
}
export interface DeltaTableProtocolVersions {
  minReaderVersion: number
  minWriterVersion: number
  readerFeatures?: Array<string>
  writerFeatures?: Array<string>
}
export interface CommitProperties {
  customMetadata?: Record<string, string>
  maxCommitRetries?: number
  appTransactions?: Array<Transaction>
}
export interface PostCommitHookProperties {
  createCheckpoint: boolean
  cleanupExpiredLogs?: boolean
}
export class QueryBuilder {
  constructor()
  /**
   * Register the given [DeltaTable] into the [SessionContext] using the provided `table_name`
   *
   * Once called, the provided `delta_table` will be referenceable in SQL queries so long as
   * another table of the same name is not registered over it.
   */
  register(tableName: string, deltaTable: JsDeltaTable): QueryBuilder
  /** Prepares the sql query to be executed. */
  sql(sqlQuery: string): QueryResult
}
export class QueryResult {
  constructor(queryBuilder: QueryBuilder, sqlQuery: string)
  /** Print the first 25 rows returned by the SQL query */
  show(): Promise<void>
  /**
   * Execute the given SQL command within the [SessionContext] of this instance
   *
   * **NOTE:** The function returns the rows as a continuous, newline delimited, stream of JSON strings
   * it is especially suited to deal with large results set.
   */
  stream(): ReadableStream<Buffer>
  /**
   * Execute the given SQL command within the [SessionContext] of this instance
   *
   * **NOTE:** Since this function returns a materialized JS Buffer,
   * it may result unexpected memory consumption for queries which return large data
   * sets.
   */
  fetchAll(): Promise<Buffer>
}
export type JsDeltaTable = DeltaTable
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
  tableUri(): string
  version(): number
  getLatestVersion(): Promise<number>
  getEarliestVersion(): Promise<number>
  getNumIndexCols(): number
  getStatsColumns(): Array<string> | null
  hasFiles(): boolean
  metadata(): DeltaTableMetadata
  protocolVersions(): DeltaTableProtocolVersions
  /** Get the current schema of the Delta table. */
  schema(): string
  /** Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table and are older than the retention threshold. */
  vacuum(dryRun: boolean, enforceRetentionDuration: boolean, commitProperties?: JsCommitProperties | undefined | null, postCommithookProperties?: JsPostCommitHookProperties | undefined | null): Promise<Array<string>>
}
export type JsTransaction = Transaction
export class Transaction {
  appId: string
  version: number
  lastUpdated?: number
  constructor(appId: string, version: number, lastUpdated?: number | undefined | null)
}
