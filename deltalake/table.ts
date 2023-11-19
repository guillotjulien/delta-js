abstract class DeltaTable {
    /**
     * Create the Delta table from a path with an optional version.
     * Multiple StorageBackends are currently supported: AWS S3 and and local URI.
     * Depending on the storage backend used, you could provide options values using the `storageOptions` parameter.
     * 
     * @param tableUri Path of the Delta table
     * @param version Version of the Delta table
     * @param storageOptions an object of the options to use for the storage backend
     */
    constructor(tableUri: string, version?: number, storageOptions?: Record<string, any>) {}

    /**
     * Get the version of the Delta table.
     */
    public abstract version(): number;

    /**
     * Load a Delta table with a specified version.
     */
    public abstract loadVersion(version: number): void;

    /**
     * Get the current schema of the Delta table.
     */
    public abstract schema(): Schema;

    /**
     * Get the current metadata of the Delta table.
     */
    public abstract metadata(): Metadata;

    /**
     * Get the reader and writer protocol versions of the Delta table.
     */
    public abstract protocol(): ProtocolVersions;

    /**
     * Run the history command on the Delta table.
     * The operations are returned in reverse chronological order.
     * 
     * @param limit the commit info limit to return
     */
    public abstract history(limit?: number): Record<string, any>[];

    /**
     * Get the .parquet files of the DeltaTable.
     * 
     * The paths are as they are saved in the delta log, which may either be
     * relative to the table root or absolute URIs.
     * 
     * Use the partition_filters parameter to retrieve a subset of files that match the
     * given filters.
     * 
     * Predicates are expressed in disjunctive normal form (DNF), like [["x", "=", "a"], ...].
     * DNF allows arbitrary boolean logical combinations of single partition predicates.
     * The innermost tuples each describe a single partition predicate. The list of inner
     * predicates is interpreted as a conjunction (AND), forming a more selective and
     * multiple partition predicates. Each tuple has format: (key, op, value) and compares
     * the key with the value. The supported op are: `=`, `!=`, `in`, and `not in`. If
     * the op is in or not in, the value must be a collection such as a list, a set or a tuple.
     * The supported type for value is str. Use empty string `''` for Null partition value.
     * 
     * Examples:
     * ```
     * ["x", "=", "a"]
     * ["x", "!=", "a"]
     * ["y", "in", ["a", "b", "c"]]
     * ["z", "not in", ["a","b"]]
     * ```
     */
    public abstract files(): string[];

    /**
     * Get the list of files as absolute URIs, including the scheme (e.g. "s3://").
     * 
     * Local files will be just plain absolute paths, without a scheme. (That is,
     * no 'file://' prefix.)
     * 
     * Use the partition_filters parameter to retrieve a subset of files that match the
     * given filters.
     * 
     * Predicates are expressed in disjunctive normal form (DNF), like [["x", "=", "a"], ...].
     * DNF allows arbitrary boolean logical combinations of single partition predicates.
     * The innermost tuples each describe a single partition predicate. The list of inner
     * predicates is interpreted as a conjunction (AND), forming a more selective and
     * multiple partition predicates. Each tuple has format: (key, op, value) and compares
     * the key with the value. The supported op are: `=`, `!=`, `in`, and `not in`. If
     * the op is in or not in, the value must be a collection such as a list, a set or a tuple.
     * The supported type for value is str. Use empty string `''` for Null partition value.
     * 
     * Examples:
     * ```
     * ["x", "=", "a"]
     * ["x", "!=", "a"]
     * ["y", "in", ["a", "b", "c"]]
     * ["z", "not in", ["a","b"]]
     * ```
     */
    public abstract fileUris(): string[];

    /**
     * Build an array using data from the Delta table.
     * 
     * @param filters A disjunctive normal form (DNF) predicate for filtering rows.
     * @param columns The columns to project. This can be a list of column names to include (order and duplicates will be preserved).
     */
    public abstract toArray(filters?: FilterType, columns?: string[]): Record<string, any>[];

    // TODO: Can apparently use datafusion for sending SQL queries. See here: https://docs.rs/deltalake/latest/deltalake/#querying-delta-tables-with-datafusion

    // TODO: Those will come later. It'll be useful if s/o is working with Polars (https://github.com/pola-rs/polars/tree/main) that is compatible with Javascript.
    // e.g. you convert to an Arrow Dataset, pass it to Polars and then you can filter the way you want.
    // public abstract toArrowDataset();
    // public abstract toArrowTable();

    // TODO: write ops. Only care about read ops for now.
    // vacuum
    // update
    // optimize
    // merge
    // restore
    // delete
    // repair
    // createCheckpoint
    // updateIncremental
}

type FilterType = FilterConjunctionType|FilterDNFType;
type FilterLiteralType = any; // TODO: How to return a tuple in JS? Tuple[str, str, Any]
type FilterConjunctionType = FilterLiteralType[];
type FilterDNFType = FilterConjunctionType[];

abstract class Schema {
    constructor(fields: Field[]) {}

    /**
     * Get the JSON string representation of the Schema.
     * A schema has the same JSON format as a StructType.
     * 
     * new Schema([new Field("x", "integer")]).toJSON()
     * Returns '{"type":"struct","fields":[{"name":"x","type":"integer","nullable":true,"metadata":{}}]}'
     */
    public abstract toJSON(): string;

    // /**
    //  * Create a new Schema from a JSON format as a StructType.
    //  * 
    //  * Schema.fromJSON('{ "type": "struct", "fields": [{"name": "x", "type": "integer", "nullable": true, "metadata": {}}] }')
    //  * Returns Schema([Field("x", PrimitiveType("integer"), nullable=True)])
    //  */
    // public static fromJSON(schema: string): Schema {
    //     return;
    // };

    /**
     * Return the equivalent Arrow schema.
     */
    // public abstract toArrow(): arrow.Schema;

    /**
     * Create a Schema from an Arrow Schema type
     */
    // public abstract fromArrow(schema: arrow.Schema): Schema;
}

type DataType = 'PrimitiveType'|'MapType'|'StructType'|'ArrayType'; // TODO: will need to make a class for all of those

/**
 * A field in a Delta StructType or Schema.
 */
abstract class Field {
    constructor(name: string, type: DataType, nullable = true, metadata?: Record<string, any>) {}

    // TODO: Add methods
}

class Metadata {
    private readonly metadata: RawDeltaTableMetadata;

    constructor(table: RawDeltaTable) {
        this.metadata = table.metadata();
    }

    public get id(): number {
        return this.metadata.id;
    }

    public get name(): string {
        return this.metadata.name;
    }

    public get description(): string {
        return this.metadata.description;
    }

    public get partitionColumns(): string[] {
        return this.metadata.partitionColumns;
    }

    public get createdTime(): Date {
        return this.metadata.createdTime;
    }

    public get configuration(): Record<string, string> {
        return this.metadata.configuration;
    }

    public toString(): string {
        return `Metadata(id: ${this.id}, name: ${this.name}, description: ${this.description}, partitionColumns: ${this.partitionColumns}, createdTime: ${this.createdTime.toISOString()}, configuration: ${JSON.stringify(this.configuration)})`;
    }
}

type ProtocolVersions = { minReaderVersion: number; minWriterVersion: number };

// FIXME: Those will be internal only and we'll bind Rust code to them.
type RawDeltaTableMetadata = any;
type RawDeltaTable = any;