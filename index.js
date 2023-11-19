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
    console.log("Min match?", s.minValues._c0, s.minValues._c0.localeCompare("100088") >= 0);
    console.log("Max match?", s.maxValues._c0, s.maxValues._c0.localeCompare("100088") <= 0);

    if (s.minValues._c0.localeCompare("100088") >= 0) {
        indexes.push(i);
    }
});

console.log("matching files: ", indexes.map((i) => fileUris[i])); // Can (& most likely will) be multiple files

// Can then read the Parquet file using DuckDB
const db = new duckdb.Database(':memory:');
db.all(`select * from '${fileUris[0]}'`, (err, res) => {
    if (err) {
        throw err;
    }

    console.log(res);
});