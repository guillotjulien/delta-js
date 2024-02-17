const { rawDeltaTableNew, rawDeltaTableVersion, rawDeltaTableFileUris, rawDeltaTableStats, rawDeltaTableSchema } = require("./index.node");
const duckdb = require('duckdb');

const table = rawDeltaTableNew(
    './test-64-files',
    {
        // version: 0,
        // withoutFiles: true,
        // storageOptions: {
        //     AWS_ACCESS_KEY_ID: '',
        //     AWS_SECRET_ACCESS_KEY: '',
        //     AWS_SESSION_TOKEN: '',
        // },
    },
);

console.log("version:", rawDeltaTableVersion.call(table));

console.log("schema:", rawDeltaTableSchema.call(table));

// const fileUris = rawDeltaTableFileUris.call(table, /*[{ key: "a", op: "=", value: "test" }]*/);
// // console.log("file uris:", fileUris);

// const stats = rawDeltaTableStats.call(table);
// // console.log(stats);

// const indexes = [];
// stats.forEach((s, i) => {
//     if ("100088".localeCompare(s.minValues._c0) >= 0) { // Our value is w/in boundaries
//         indexes.push(i);
//     }
// });

// const matchingFiles = indexes.map((i) => fileUris[i]);

// console.log("matching files: ", matchingFiles); // Can (& most likely will) be multiple files

// // Can then read the Parquet file using DuckDB
// const db = new duckdb.Database(':memory:');
// // db.all(`select * from parquet_metadata('${matchingFiles[0]}')`, (err, res) => {
// //     if (err) {
// //         throw err;
// //     }

// //     console.log(res);
// // });

// db.all(`select * from read_parquet([${matchingFiles.map((f) => `'${f}'`).join(',')}]) where _c0 = '100088'`, (err, res) => {
//     if (err) {
//         throw err;
//     }

//     console.log(res);
// });
