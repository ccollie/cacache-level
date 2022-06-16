import {
  AbstractLevel,
  AbstractDatabaseOptions,
  AbstractOpenOptions,
  NodeCallback
} from 'abstract-level'

/**
 * Content-addressable, file system {@link AbstractLevel} database for Node.js, based on cacache.
 *
 * @template KDefault The default type of keys if not overridden on operations.
 * @template VDefault The default type of values if not overridden on operations.
 */
export class CacacheLevel<KDefault = string, VDefault = Buffer>
  extends AbstractLevel<Buffer | Uint8Array | string, KDefault, VDefault> {
  /**
   * Database constructor.
   * @param location The data location.
   * @param options Options, of which some will be forwarded to {@link open}.
   */
  constructor (location: string, options?: DatabaseOptions<KDefault, VDefault> | undefined)

  open (): Promise<void>
  open (options: OpenOptions): Promise<void>
  open (callback: NodeCallback<void>): void
  open (options: OpenOptions, callback: NodeCallback<void>): void

  /**
   * Looks up key in the cache index, returning information about the entry if one exists.
   * @param key
   * @param callback
   * @returns {Promise<{EntryMetadata>}
   */
  getInfo (key: string, callback?: (err: Error, info: IndexEntry) => void): Promise<IndexEntry | null>

  /**
   * Destroy all data associated with the database specified at location. Note that a filly qualified path
   * must be specified
   * @param location the path to the db
   * @param callback
   */
  static destroy(location: string, callback?: (err: Error) => void): Promise<void>

  /**
   * Destroy all data associated with the database
   * @param callback
   */
  destroy(callback?: (err: Error) => void): Promise<void>

  /**
   *  Checks out and fixes up the db
   *  - Cleans up corrupted or invalid index entries
   *  - Filters entries by user defined predicate function
   *  - Garbage collects any content entries not referenced by the index.
   *  - Checks integrity for all content entries and removes invalid content.
   *  - Removes the tmp directory in the cache and all its contents.
   *  @param options
   *  @param callback
   *  @returns {VerifyStats} an object with various stats about the verification process
   */
  verify(options: VerifyOptions, callback?: (err: Error, stats: VerifyStats) => void): Promise<VerifyStats>
}

/**
 * Options for the {@link CacacheLevel} constructor.
 */
export interface DatabaseOptions<K, V> extends AbstractDatabaseOptions<K, V> {
  /**
   Concurrency to use for batch operations
   */
  concurrency?: number

  /**
   * Batch size to use in certain operations like `clear`.
   */
  batchSize?: number

  /*
   * base path to use for the cache. If not provided, it will use the system temporary directory.
   */
  rootDirectory?: string
}

/**
 * Options for the {@link CacacheLevel.open} method.
 */
export interface OpenOptions extends AbstractOpenOptions {
  /**
   Concurrency to use for batch operations
   */
  concurrency?: number

  /**
   * Batch size to use in certain operations like `clear`.
   */
  batchSize?: number
}

/**
 * Information associated with a cached value.
 */
export interface IndexEntry {
  /**
   * Key the entry was looked up under. Matches the key argument.
   */
  key: string
  /**
   *  Subresource Integrity hash for the content this entry refers to.
   */
  integrity: string
  /**
   * Filesystem path where content is stored, joined with cache argument.
   */
  path: string
  /**
   * Timestamp the entry was first added on.
   */
  time: number
  /**
   * User-assigned metadata associated with the entry/content.
   */
  metadata: any
}

export interface VerifyOptions {
  /**
   * Number of concurrently read files in the filesystem while doing clean up.
   * Default 20
   */
  concurrency?: number

  filter?: (entry: IndexEntry) => boolean
}

export interface VerifyStats {
  verifiedContent: number
  reclaimedCount: number
  reclaimedSize: number
  badContentCount: number
  keptSize: number
  missingContent: number
  rejectedEntries: number
  totalEntries: number
}
