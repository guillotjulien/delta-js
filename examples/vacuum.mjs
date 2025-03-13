import { DeltaTable, QueryBuilder } from "../index.js";

const table = new DeltaTable("./test/resources/covid-19");

await table.load();

console.log(`Current version: ${table.version()}`);
console.log(await table.vacuum(false, false));
console.log(`New version: ${table.version()}`);
