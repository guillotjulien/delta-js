import * as arrow from "apache-arrow";

import { DeltaTable, QueryBuilder } from "../build/index.js";

const table = new DeltaTable("./test-table");

// Roughly, we'll have 2 modes:
// 1. Pass an array of objects, easy to use but not efficient
// 2. Pass an Arrow IPC buffer, harder to use but more memory efficient

// To avoid memory issues, we'll need to handle the conversion on the JS layer. When getting an array of objetcs, we'll convert it into an Arrow IPC buffer.
// Since we do support Arrow IPC format, implementing a zero copy fetch might make sense as well for our query runner

// Note: doing that, Arrow will treat all numbers as float
await table.write(
  [
    { id: 1, name: "Alice", age: 30 },
    { id: 2, name: "Bob", age: 25 },
    { id: 3, name: "Charlie", age: 35 },
    { id: 4, name: "Jean", age: 27 },
  ],
  "overwrite",
);

console.log(`Version: ${table.version()}`);

const qb = new QueryBuilder();

qb.register("test", table);

await qb.sql("select * from test").show();

const dataManual = arrow.tableFromArrays({
  id: new Uint8Array([1, 2, 3, 4]),
  name: ["Alice", "Bob", "Charlie", "Jean"],
  age: new Uint8Array([30, 25, 35, 27]),
});

await table.writeArrowIPC(
  arrow.tableToIPC(dataManual),
  "overwrite",
  "overwrite",
);

console.log(`Version: ${table.version()}`);

// FIXME: That's a workaround - we might want to implement a refresh method
qb.register("test2", table);

await qb.sql("select * from test2").show();
