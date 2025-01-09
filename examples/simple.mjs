import { DeltaTable, QueryBuilder } from '../index.js';

const table = new DeltaTable('./test/resources/test-table');

await table.load();

console.log(table.version());
console.log(table.schema());

const qb = new QueryBuilder();

qb.register('test', table);

await qb.sql('select * from test').show();

// let stream = qb.sql('select * from test').stream();

// stream.on('data', (chunk) => {
//     console.log(chunk.toString());
// });
  
// stream.on('end', () => {
//     console.log('Stream ended.');
// });