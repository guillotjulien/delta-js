const { rawDeltaTableNew, rawDeltaTableVersion, rawDeltaTableFileUris, rawDeltaTableStats } = require("./index.node");
const duckdb = require('duckdb');

const table = rawDeltaTableNew('./test-64-files');

console.log("version:", rawDeltaTableVersion.call(table));

const fileUris = rawDeltaTableFileUris.call(table, /*[{ key: "a", op: "=", value: "test" }]*/);
// console.log("file uris:", fileUris);

const stats = rawDeltaTableStats.call(table);
// console.log(stats);

const indexes = [];
stats.forEach((s, i) => {
    if ("100088".localeCompare(s.minValues._c0) >= 0) { // Our value is w/in boundaries
        indexes.push(i);
    }
});

console.log("matching files: ", indexes.map((i) => fileUris[i])); // Can (& most likely will) be multiple files

// Can then read the Parquet file using DuckDB
const db = new duckdb.Database(':memory:');
db.all(`select * from '${fileUris[0]}' where _c0 = '100088'`, (err, res) => {
    if (err) {
        throw err;
    }

    console.log(res);
});
