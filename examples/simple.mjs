import { DeltaTable } from '../index.js';

const table = new DeltaTable('s3://...', {
    storageOptions: {
        awsRegion: '',
        awsAccessKeyId: '',
        awsSecretAccessKey: '',
    },
});

await table.load();

console.log(table.version());
console.log(table.schema());

console.log(JSON.parse(await table.query("select _c0, _c3 from delta_table where _c0 = '100088'")));
