import { Readable } from "stream";
import { DeltaTable, QueryBuilder } from "../index.js";

const table = new DeltaTable("./tests/resources/covid-19");

await table.load();

const qb = new QueryBuilder();

qb.register("test", table);

const stream = Readable.fromWeb(qb.sql("select * from test").stream());

stream.on("data", (chunk) => {
  console.log("chunk:", chunk.toString());
});

stream.on("end", () => {
  console.log("stream ended");
});
