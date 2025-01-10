import { DeltaTable, QueryBuilder } from '../index.js';

const table = new DeltaTable('./test/resources/test-table');

await table.load();

console.log(table.version());
console.log(table.schema());

const qb = new QueryBuilder();

qb.register('test', table);

await qb.sql('select * from test').show();