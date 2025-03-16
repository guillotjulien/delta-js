import { RawCursor, RawQueryBuilder } from "../native";
import { DeltaTable } from "../table";

export class QueryBuilder {
  /** @internal */
  private readonly qb: RawQueryBuilder;

  constructor() {
    this.qb = new RawQueryBuilder();
  }

  /**
   * Register the given [DeltaTable] into the [SessionContext] using the provided `tableName`
   *
   * Once called, the provided `deltaTable` will be referenceable in SQL queries so long as
   * another table of the same name is not registered over it.
   */
  register(tableName: string, deltaTable: DeltaTable): this {
    this.qb.register(tableName, deltaTable._table);
    return this;
  }

  /** Prepares the sql query to be executed. */
  sql(sqlQuery: string): Cursor {
    const rawCursor = this.qb.sql(sqlQuery);
    return new CursorInternal(rawCursor);
  }
}

export interface Cursor {
  /** Print the first 25 rows returned by the SQL query */
  show(): Promise<void>;

  /**
   * Execute the given SQL command within the [SessionContext] of this instance
   *
   * **NOTE:** The function returns the rows as a continuous, newline delimited, stream of JSON strings
   * it is especially suited to deal with large results set.
   */
  stream(): ReadableStream<Buffer>;

  /**
   * Execute the given SQL command within the [SessionContext] of this instance
   *
   * **NOTE:** Since this function returns a materialized JS Buffer,
   * it may result unexpected memory consumption for queries which return large data
   * sets.
   */
  fetchAll(): Promise<Buffer>;
}

/** @internal */
class CursorInternal implements Cursor {
  constructor(private readonly rawCursor: RawCursor) {}

  show(): Promise<void> {
    return this.rawCursor.show();
  }

  stream(): ReadableStream<Buffer> {
    return this.rawCursor.stream();
  }

  fetchAll(): Promise<Buffer> {
    return this.rawCursor.fetchAll();
  }
}
