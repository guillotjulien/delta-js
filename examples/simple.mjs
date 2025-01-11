import { DeltaTable, QueryBuilder } from '../index.js';

const table = new DeltaTable('./test/resources/covid-19');

await table.load();

console.log(table.version());
console.log(table.schema());

const qb = new QueryBuilder();

qb.register('test', table);

const query = qb.sql("select Entity, count(1) as total from test where Entity in ('France', 'Germany') group by Entity");

await query.show();

const result = await query.fetchAll();

console.log(JSON.parse(result));