import { DeltaTable, QueryBuilder } from '../index.js';

const table = new DeltaTable('s3://...', { storageOptions: { awsRegion: 'eu-west-1', awsProfile: 'default' } });

await table.load();

console.log(table.version());
console.log(table.schema());

const qb = new QueryBuilder();

qb.register('test', table);

await qb.sql('select * from test').show();
