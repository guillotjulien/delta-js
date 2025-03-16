export interface Transaction {
  appId: string;
  version: number;
  lastUpdated?: number;
}

/** The commit properties. Controls the behaviour of the commit. */
export interface CommitProperties {
  /** Custom metadata that will be added to the transaction commit. */
  customMetadata?: Record<string, string>;

  /** Maximum number of times to retry the transaction commit. */
  maxCommitRetries?: number;

  /**  */
  appTransactions?: Transaction[];
}

/**
 * The post commit hook properties, only required for advanced usecases where
 * you need to control this.
 */
export interface PostCommitHookProperties {
  /** Create checkpoints based on checkpoint interval. Defaults to true. */
  createCheckpoint?: boolean;

  /** Clean up logs based on interval. Defaults to null. */
  cleanupExpiredLogs?: boolean;
}
