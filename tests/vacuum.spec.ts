import { join } from "node:path";
import { mkdtemp, readdir, rmdir } from "node:fs/promises";
import { tmpdir } from "node:os";

import { DeltaTable, WriteMode } from "../delta";

let tmpDir: string;

beforeEach(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "vacuum-"));
});

afterEach(async () => {
  await rmdir(tmpDir, {
    recursive: true,
  });
});

test("it returns deleted files", async () => {
  const dt = new DeltaTable("tests/resources/simple_table");
  await dt.load();

  const tombstones = await dt.vacuum({
    dryRun: true,
    retentionHours: 169,
  });

  const expected = [
    "part-00000-a72b1fb3-f2df-41fe-a8f0-e65b746382dd-c000.snappy.parquet",
    "part-00121-d8bc3e53-d2f2-48ce-9909-78da7294ffbd-c000.snappy.parquet",
    "part-00068-90650739-6a8e-492b-9403-53e33b3778ac-c000.snappy.parquet",
    "part-00003-508ae4aa-801c-4c2c-a923-f6f89930a5c1-c000.snappy.parquet",
    "part-00058-b462c4cb-0c48-4148-8475-e21d2a2935f8-c000.snappy.parquet",
    "part-00003-53f42606-6cda-4f13-8d07-599a21197296-c000.snappy.parquet",
    "part-00069-c78b4dd8-f955-4643-816f-cbd30a3f8c1b-c000.snappy.parquet",
    "part-00164-bf40481c-4afd-4c02-befa-90f056c2d77a-c000.snappy.parquet",
    "part-00140-e9b1971d-d708-43fb-b07f-975d2226b800-c000.snappy.parquet",
    "part-00045-332fe409-7705-45b1-8d34-a0018cf73b70-c000.snappy.parquet",
    "part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet",
    "part-00143-03ceb88e-5283-4193-aa43-993cdf937fd3-c000.snappy.parquet",
    "part-00190-8ac0ae67-fb1d-461d-a3d3-8dc112766ff5-c000.snappy.parquet",
    "part-00154-4630673a-5227-48fb-a03d-e356fcd1564a-c000.snappy.parquet",
    "part-00007-94f725e2-3963-4b00-9e83-e31021a93cf9-c000.snappy.parquet",
    "part-00000-f17fcbf5-e0dc-40ba-adae-ce66d1fcaef6-c000.snappy.parquet",
    "part-00112-07fd790a-11dc-4fde-9acd-623e740be992-c000.snappy.parquet",
    "part-00049-d3095817-de74-49c1-a888-81565a40161d-c000.snappy.parquet",
    "part-00004-95c9bc2c-ac85-4581-b3cc-84502b0c314f-c000.snappy.parquet",
    "part-00000-a922ea3b-ffc2-4ca1-9074-a278c24c4449-c000.snappy.parquet",
    "part-00001-bb70d2ba-c196-4df2-9c85-f34969ad3aa9-c000.snappy.parquet",
    "part-00005-94a0861b-6455-4bd9-a080-73e02491c643-c000.snappy.parquet",
    "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet",
    "part-00011-42f838f9-a911-40af-98f5-2fccfa1b123f-c000.snappy.parquet",
    "part-00128-b31c3b81-24da-4a90-a8b4-578c9e9a218d-c000.snappy.parquet",
    "part-00150-ec6643fc-4963-4871-9613-f5ad1940b689-c000.snappy.parquet",
    "part-00001-c506e79a-0bf8-4e2b-a42b-9731b2e490ae-c000.snappy.parquet",
    "part-00107-3f6c2aa0-fc28-4f4c-be15-135e15b398f4-c000.snappy.parquet",
    "part-00077-2fcb1c7c-5390-48ee-93f6-0acf11199a0d-c000.snappy.parquet",
    "part-00004-80938522-09c0-420c-861f-5a649e3d9674-c000.snappy.parquet",
    "part-00116-bc66759e-6381-4f34-8cd4-6688aad8585d-c000.snappy.parquet",
  ];

  expect(tombstones.sort()).toEqual(expected.sort());
});

test("it returns an error when retentionHours is negative", async () => {
  const dt = new DeltaTable("tests/resources/simple_table");
  await dt.load();

  const promise = dt.vacuum({
    dryRun: true,
    retentionHours: -1,
  });

  await expect(promise).rejects.toThrow("retention hours should be positive");
});

test("it returns an error when retentionHours is lower than delta.deletedFileRetentionDuration", async () => {
  const dt = new DeltaTable("tests/resources/simple_table");
  await dt.load();

  const promise = dt.vacuum({
    dryRun: true,
    retentionHours: 167,
  });

  await expect(promise).rejects.toThrow(
    "Generic error: Invalid retention period, minimum retention for vacuum is configured to be greater than 168 hours, got 167 hours",
  );
});

test("it returns deleted files when retention period is lower than delta.deletedFileRetentionDuration and enforceRetentionDuration is false", async () => {
  const dt = new DeltaTable("tests/resources/simple_table");
  await dt.load();

  const tombstones = await dt.vacuum({
    dryRun: true,
    retentionHours: 167,
    enforceRetentionDuration: false,
  });

  expect(tombstones.length).toBeGreaterThan(0);
});

test("it only keep last version when retentionHours is set to 0 and dryRun is false", async () => {
  const dt = new DeltaTable(tmpDir);

  await dt.write([{ id: 1 }], WriteMode.Overwrite);

  const originalFiles = new Set(await dt.files());

  await dt.write([{ id: 1 }, { id: 2 }], WriteMode.Overwrite);

  const newFiles = new Set(await dt.files());

  const tombstones = await dt.vacuum({
    retentionHours: 0,
    enforceRetentionDuration: false,
  });

  const tableFiles = (await readdir(tmpDir)).filter((path) =>
    path.endsWith(".parquet"),
  );

  expect(originalFiles.intersection(newFiles).size).toEqual(0);
  expect(new Set(tombstones).symmetricDifference(originalFiles).size).toEqual(
    0,
  );
  expect(new Set(tableFiles).symmetricDifference(newFiles).size).toEqual(0);
});

test("it saves the provided metadata in transaction log", async () => {
  // TODO: For that we need the table.history function to be implemented
});
