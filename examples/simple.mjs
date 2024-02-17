import { DeltaTable } from '../index.js';

const table = new DeltaTable('../test-64-files');
await table.load();

console.log(table.version());
console.log(table.schema());

await table.query("select _c0, _c3 from delta_table where _c0 = '100088'");