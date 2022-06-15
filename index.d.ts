import {
  AbstractLevel,
  AbstractDatabaseOptions,
  AbstractOpenOptions,
  NodeCallback
} from 'abstract-level'

/**
 * Cacache based, content-addressable {@link AbstractLevel} database for Node.js.
 *
 * @template KDefault The default type of keys if not overridden on operations.
 * @template VDefault The default type of values if not overridden on operations.
 */
export class CacacheLevel<KDefault = string, VDefault = string>
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
   * Batch size to use in certain operstions like `clear`.
   */
  batchSize?: number
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
   * Batch size to use in certain operstions like `clear`.
   */
  batchSize?: number
}
