import * as arrow from "apache-arrow";

import { RawDeltaTable } from "./native";
import { CommitProperties, PostCommitHookProperties } from "./transaction";
import { Optional } from "./types";
import { WriterProperties } from "./writer/properties";

export interface AWSConfigKeyCredentials {
  awsRegion: string;
  awsAccessKeyId: string;
  awsSecretAccessKey: string;
  awsSessionToken?: string;
}

export interface AWSConfigKeyProfile {
  awsRegion: string;
  awsProfile: string;
}

export type StorageBackendOptions =
  | AWSConfigKeyCredentials
  | AWSConfigKeyProfile;

export interface DeltaTableOptions {
  /** Specify the version to load either as an integer or a date. */
  version?: number | Date;

  /**
   * Indicates whether DeltaTable should track files.
   * This defaults to `true`
   *
   * Some append-only applications might have no need of tracking any files.
   * Hence, DeltaTable will be loaded with significant memory reduction.
   */
  withoutFiles?: boolean;

  /** Set options used to initialize storage backend. */
  storageOptions?: StorageBackendOptions;
}

export interface Metadata {
  /** Unique identifier of the DeltaTable. */
  id: string;
  /** User-provided identifier of the DeltaTable */
  name?: string;
  /** User-provided description of the DeltaTable. */
  description?: string;
  /** Array containing the names of the partitioned columns of the DeltaTable. */
  partitionColumns: string[];
  /** Time when this metadata action is created, in milliseconds since the Unix epoch of the DeltaTable. */
  createdTime?: number;
  /** DeltaTable properties. */
  configuration: Record<string, Optional<string>>;
}

export interface ProtocolVersions {
  /** The minimum version of the Delta read protocol that a client must implement in order to correctly read this table. */
  minReaderVersion: number;
  /** The minimum version of the Delta write protocol that a client must implement in order to correctly write this table. */
  minWriterVersion: number;
  /** Array containing the list of features that a client must implement in order to correctly read this table (exist only when minReaderVersion is set to 3). */
  readerFeatures?: string[];
  /** Array containing the list of features that a client must implement in order to correctly write this table (exist only when minWriterVersion is set to 7) */
  writerFeatures?: string[];
}

export interface VacuumOptions {
  /**
   * When true only list the files, delete otherwise.
   * @defaultValue `false`
   */
  dryRun?: boolean;

  /**
   * When falsed, accepts retention hours smaller than the value from
   * `delta.deletedFileRetentionDuration`.
   * @defaultValue `true`
   */
  enforceRetentionDuration?: boolean;

  /**
   * the retention threshold in hours, if none then the value from
   * `delta.deletedFileRetentionDuration` is used or default of 1 week otherwise.
   */
  retentionHours?: number;

  /** Properties of the transaction commit. If null, default values are used. */
  commitProperties?: CommitProperties;

  /** Properties for the post commit hook. If null, default values are used. */
  postCommithookProperties?: PostCommitHookProperties;
}

/** How to handle existing data when writing a table */
export enum WriteMode {
  /** Will error if table already exists */
  Error = "error",
  /** Will add new data to the table content */
  Append = "append",
  /** Will replace table content with new data */
  Overwrite = "overwrite",
  /** Will not write anything if table already exists */
  Ignore = "ignore",
}

/** Specifies how to handle schema drifts */
export enum SchemaMode {
  /** Append the new schema to the existing schema */
  Merge = "merge",
  /** Overwrite the schema with the new schema */
  Overwrite = "overwrite",
}

export interface WriteOptions {
  /** Specifies how to handle schema drifts. By default, schema differences will raise an error. */
  schemaMode?: SchemaMode;

  /** List of columns to partition the table by. Only required when creating a new table. */
  partitionBy?: string[];

  /** When using `overwrite` mode, replace data that matches a predicate. Only used in rust engine. */
  predicate?: string;

  /** Override for target file size for data files written to the delta table. If not passed, it's taken from `delta.targetFileSize`. */
  targetFileSize?: number;

  /** User-provided identifier for this table. */
  name?: string;

  /** User-provided description for this table.  */
  description?: string;

  /** Configuration options for the metadata action. */
  configuration?: Record<string, Optional<string>>;

  /** Pass writer properties to the Rust parquet writer. */
  writerProperties?: WriterProperties;

  /** Properties of the transaction commit. If null, default values are used. */
  commitProperties?: CommitProperties;

  /** Properties for the post commit hook. If null, default values are used. */
  postCommithookProperties?: PostCommitHookProperties;
}

export class DeltaTable {
  /** @internal */
  readonly _table: RawDeltaTable;

  /**
   * Create the Delta table from a path with an optional version.
   * Multiple StorageBackends are currently supported: AWS S3 and local URI.
   * Depending on the storage backend used, you could provide options values using the `options` parameter.
   *
   * This will not load the log, i.e. the table is not initialized. To get an initialized
   * table use the `load` function.
   *
   * @param tableUri Path of the Delta table
   * @param options an object of the options to use when loading the table
   */
  constructor(tableUri: string, options?: DeltaTableOptions) {
    const innerOptions: Omit<DeltaTableOptions, "version"> & {
      version?: number | string;
    } = { ...options, version: undefined };

    if (options?.version && options.version instanceof Date) {
      innerOptions.version = options.version.toISOString();
    }

    this._table = new RawDeltaTable(tableUri, innerOptions);
  }

  /**
   * Returns true if a Delta Table exists at specified path.
   * Returns false otherwise.
   *
   * @param tableUri Path of the Delta table
   * @param storageOptions an object of the options to use for the storage backend
   */
  static isDeltaTable(
    tableUri: string,
    storageOptions?: StorageBackendOptions,
  ): Promise<boolean> {
    return RawDeltaTable.isDeltaTable(tableUri, storageOptions);
  }

  /** Build the DeltaTable and load its state. */
  load(): Promise<void> {
    return this._table.load();
  }

  /** Get the loaded version of the DeltaTable. */
  version(): number {
    return this._table.version();
  }

  /** Return the URI (including protocols such as `s3://`) of the table. */
  tableUri(): string {
    return this._table.tableUri();
  }

  /** Get the current schema of the Delta table. */
  schema(): string {
    return this._table.schema();
  }

  /** Get the current metadata of the DeltaTable. */
  metadata(): Metadata {
    return this._table.metadata();
  }

  /** Get the reader and writer protocol versions of the DeltaTable. */
  protocol(): ProtocolVersions {
    return this._table.protocol();
  }

  /**
   * Run the Vacuum command on the Delta Table: list and delete files no longer
   * referenced by the Delta table and are older than the retention threshold.
   */
  vacuum(options?: VacuumOptions): Promise<string[]> {
    return this._table.vacuum(options);
  }

  /**
   * Write data to Delta Table. Table will be created if it does not exists.
   *
   * @param data Array of rows - will be converted to Arrow IPC buffer
   * @param mode How to handle existing data. Default is to error if table already exists.
   * @param options
   */
  write(
    data: Record<string, unknown>[],
    mode: WriteMode,
    // TODO: We'll want to specify the schema manually here
    options?: WriteOptions,
  ): Promise<void> {
    const arrowData = arrow.tableFromJSON(data);
    return this._table.write(arrow.tableToIPC(arrowData), mode, options);
  }

  /**
   * Write data to Delta Table. Table will be created if it does not exists.
   *
   * @param data Arrow data as IPC buffer
   * @param mode How to handle existing data. Default is to error if table already exists.
   * @param options
   */
  writeArrowIPC(
    data: Uint8Array,
    mode: WriteMode,
    options?: WriteOptions,
  ): Promise<void> {
    return this._table.write(data, mode, options);
  }
}
