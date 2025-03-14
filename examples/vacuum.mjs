import { DeltaTable, QueryBuilder } from "../index.js";

const table = new DeltaTable("./test/resources/test-table");

await table.load();

console.log(`Current version: ${table.version()}`);
console.log(
  await table.vacuum({
    dryRun: true,
    enforceRetentionDuration: false,
    retentionHours: 0,
  }),
);
console.log(`New version: ${table.version()}`);
