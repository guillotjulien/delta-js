import { DeltaTable } from "../build/index.js";

const path = "./tests/resources/test-table";
if (!(await DeltaTable.isDeltaTable(path))) {
  throw new Error("Not a Delta table");
}

const table = new DeltaTable(path);

await table.load();

console.log(table.version());
console.log(table.schema());
console.log(table.metadata());
console.log(table.protocol());
