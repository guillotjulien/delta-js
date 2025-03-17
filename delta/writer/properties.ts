export enum Compression {
  Uncompressed = "UNCOMPRESSED",
  Snappy = "SNAPPY",
  Gzip = "GZIP",
  Brotli = "BROTLI",
  Lz4 = "LZ4",
  Zstd = "ZSTD",
  Lz4Raw = "LZ4_RAW",
}

/** Bloom filter properties for the Rust parquet writer. */
export interface BloomFilterProperties {
  /** If true and no fpp or ndv are provided, the default values will be used. */
  setBloomFilterEnabled?: boolean;

  /** The false positive probability for the bloom filter. Must be between 0 and 1 exclusive. */
  fpp?: number;

  /** The number of distinct values for the bloom filter. */
  ndv?: number;
}

namespace BloomFilterProperties {
  export function validate(value: BloomFilterProperties) {
    if (value.fpp && (value.fpp <= 0 || value.fpp >= 1)) {
      throw new RangeError("fpp must be between 0 and 1 exclusive");
    }
  }
}

/** Column properties for the Rust parquet writer */
export interface ColumnProperties {
  /** Enable dictionary encoding for the column. */
  dictionaryEnabled?: boolean;

  /** Statistics level for the column. */
  statisticsEnabled?: "NONE" | "CHUNK" | "PAGE";

  /** Bloom Filter Properties for the column. */
  bloomFilterProperties?: BloomFilterProperties;
}

/** Writer properties for the Rust parquet writer. */
export interface WriterProperties {
  /** Limit DataPage size to this in bytes. */
  dataPageSizeLimit?: number;

  /** Limit the size of each DataPage to store dicts to this amount in bytes. */
  dictionaryPageSizeLimit?: number;

  /** Limit the number of rows in each DataPage. */
  dataPageRowCountLimit?: number;

  /** Splits internally to smaller batch size. */
  writeBatchSize?: number;

  /** Max number of rows in row group. */
  maxRowGroupSize?: number;

  /** compression type */
  compression?: Compression;

  /**
   * If none and compression has a level, the default level will be used, only relevant for
   *   - GZIP: levels (1-9),
   *   - BROTLI: levels (1-11),
   *   - ZSTD: levels (1-22),
   */
  compressionLevel?: number;

  /** maximum length of truncated min/max values in statistics. */
  statisticsTruncateLength?: number;

  /** Default Column Properties for the Rust parquet writer. */
  defaultColumnProperties?: ColumnProperties;

  /** Column Properties for the Rust parquet writer. */
  columnProperties?: Record<string, ColumnProperties>;
}

namespace WriterProperties {
  export function validate(value: WriterProperties) {
    if (value.compressionLevel && !value.compression) {
      throw new Error(
        "Providing a compression level without the compression type is not possible, please provide the compression as well.",
      );
    }

    if (
      value.compression &&
      [Compression.Gzip, Compression.Brotli, Compression.Zstd].includes(
        value.compression,
      )
    ) {
      if (value.compressionLevel) {
        let min, max: number;
        switch (value.compression) {
          case Compression.Gzip:
            min = 0;
            max = 10;
            break;
          case Compression.Brotli:
            min = 0;
            max = 11;
            break;
          case Compression.Zstd:
            min = 1;
            max = 22;
            break;
          default:
            throw new Error(
              `${value.compression} does not have a compression level`,
            );
        }

        if (value.compressionLevel < min || value.compressionLevel >= max) {
          throw new RangeError(
            `Compression level for ${value.compression} should fall between ${min}-${max}`,
          );
        }
      }
    }
  }
}
