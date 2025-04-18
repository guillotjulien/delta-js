import { Uint8, Utf8 } from "apache-arrow";
import { Schema, Field } from "apache-arrow/schema";

import { DeltaTable, WriteMode } from "../build/index.js";

const schema = new Schema([
  Field.new("id", new Uint8(), false),
  Field.new("name", new Utf8(), false),
  Field.new("age", new Uint8(), false),
]);

const data = [
  { id: 1, name: "Alice", age: 30 },
  { id: 2, name: "Bob", age: 25 },
  { id: 3, name: "Charlie", age: 35 },
  { id: 4, name: "Jean", age: 27 },
];

const table = new DeltaTable("./test-table");

// FIXME: Why does this throw an error in JS, but not in TS even though the generated code is similar???
await table.write(data, WriteMode.Overwrite, schema);

console.log(`Version: ${table.version()}`);
