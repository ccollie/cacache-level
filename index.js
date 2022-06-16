'use strict'

const {
  AbstractLevel,
  AbstractIterator,
  AbstractKeyIterator,
  AbstractValueIterator
} = require('abstract-level')

const cacache = require('cacache')
const bounds = require('binary-searching')
const { fromPromise } = require('catering')
const os = require('os')
const pMap = require('p-map')

const rangeOptions = new Set(['gt', 'gte', 'lt', 'lte'])
const kNone = Symbol('none')
const kDone = Symbol('done')
const kReverse = Symbol('reverse')
const kNext = Symbol('next')
const kNextv = Symbol('nextv')
const kAll = Symbol('all')
const kAdvance = Symbol('advance')
const kLocation = Symbol('location')
const kInit = Symbol('init')
const kIndexKeys = Symbol('indexKeys')
const kEntries = Symbol('entries')
const kIndexPromise = Symbol('index-promise')
const kCursor = Symbol('cursor')
const kAwaitIndex = Symbol('process-seek')
const kSeekOptions = Symbol('seek-options')
const kConcurrency = Symbol('concurrency')
const kBatchSize = Symbol('compare')
const kHandleSeek = Symbol('handle-seek')
const kGetKeyBatch = Symbol('get-key-batch')

const DEFAULT_BATCH_SIZE = 25
const DEFAULT_CONCURRENCY = os.cpus().length * 4

function compare (a, b) {
  // Only relevant when storeEncoding is 'utf8',
  // which guarantees that b is also a string.
  if (typeof a === 'string') {
    return a < b ? -1 : a > b ? 1 : 0
  }

  const length = Math.min(a.byteLength, b.byteLength)

  for (let i = 0; i < length; i++) {
    const cmp = a[i] - b[i]
    if (cmp !== 0) return cmp
  }

  return a.byteLength - b.byteLength
}

class Iterator extends AbstractIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }

  _next (callback) {
    this[kNext]((err, key, value) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, key, value)
    })
  }

  _nextv (size, options, callback) {
    this[kNextv](size, options, (err, values) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, values)
    })
  }

  _all (options, callback) {
    this[kAll](options, (err, values) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, values)
    })
  }
}

class KeyIterator extends AbstractKeyIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }

  _next (callback) {
    this[kAwaitIndex](err => {
      if (err) return this.nextTick(callback, err)
      const key = this[kAdvance]()
      this.nextTick(callback, null, key)
    })
  }

  _nextv (size, options, callback) {
    this[kAwaitIndex](err => {
      if (err) return this.nextTick(callback, err)
      const keys = this[kGetKeyBatch](size)
      this.nextTick(callback, null, keys)
    })
  }

  _all (options, callback) {
    this[kAwaitIndex](err => {
      if (err) return this.nextTick(callback, err)
      const cursor = this[kCursor]
      const allKeys = this[kIndexKeys]
      const keys = allKeys.slice(cursor)
      this[kCursor] = allKeys.length
      this[kDone] = true
      this.nextTick(callback, null, keys)
    })
  }
}

class ValueIterator extends AbstractValueIterator {
  constructor (db, options) {
    super(db, options)
    this[kInit](db, options)
  }

  _next (callback) {
    this[kNext]((err, key, value) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, value)
    })
  }

  _nextv (size, options, callback) {
    this[kAwaitIndex]((err) => {
      if (err) return this.nextTick(callback, err)
      const location = this[kLocation]
      const concurrency = this[kConcurrency]
      const keys = this[kGetKeyBatch](size)

      getMany(location, keys, concurrency, (err, values) => {
        if (err) return this.nextTick(callback, err)
        this.nextTick(callback, null, values)
      })
    })
  }

  _all (options, callback) {
    this[kAll](options, (err, values) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, values)
    })
  }
}

for (const Ctor of [Iterator, KeyIterator, ValueIterator]) {
  Ctor.prototype[kInit] = function (db, options) {
    this[kLocation] = db[kLocation]
    const reverse = options.reverse

    this[kReverse] = reverse
    this[kBatchSize] = db[kBatchSize]
    this[kConcurrency] = db[kConcurrency]

    this[kIndexPromise] = cacache.ls(this[kLocation]).then(entries => {
      let keys = Object.keys(entries).sort(compare)
      this[kEntries] = entries

      let upperBound
      let lowerBound

      let start
      let end

      this[kCursor] = 0

      if (!reverse) {
        lowerBound = 'gte' in options ? options.gte : 'gt' in options ? options.gt : kNone
        upperBound = 'lte' in options ? options.lte : 'lt' in options ? options.lt : kNone

        if (lowerBound === kNone) {
          start = 0
        } else if ('gte' in options) {
          start = bounds.ge(keys, lowerBound)
        } else {
          start = bounds.gt(keys, lowerBound)
        }

        if (upperBound !== kNone) {
          if ('lte' in options) {
            end = bounds.le(keys, upperBound, compare, start)
          } else {
            end = bounds.lt(keys, upperBound, compare, start)
          }
        } else {
          end = keys.length - 1
        }
      } else {
        lowerBound = 'lte' in options ? options.lte : 'lt' in options ? options.lt : kNone
        upperBound = 'gte' in options ? options.gte : 'gt' in options ? options.gt : kNone

        if (lowerBound === kNone) {
          end = keys.length - 1
        } else if ('lte' in options) {
          end = bounds.le(keys, lowerBound, compare)
        } else {
          end = bounds.lt(keys, lowerBound, compare)
        }

        if (upperBound !== kNone) {
          if ('gte' in options) {
            start = bounds.ge(keys, upperBound, compare)
          } else {
            start = bounds.gt(keys, upperBound, compare)
          }
        } else {
          start = 0
        }
      }

      keys = keys.slice(start, end - start + 1)
      if (reverse) {
        keys = keys.reverse()
      }

      this[kIndexKeys] = keys
    })
  }

  Ctor.prototype[kAdvance] = function () {
    if (this[kDone]) {
      return null
    }
    const keys = this[kIndexKeys]
    let cursor = this[kCursor]
    if (cursor < keys.length) {
      const key = keys[cursor++]
      this[kCursor] = cursor
      this[kDone] = (cursor >= keys.length)
      return key
    } else {
      this[kDone] = true
      return null
    }
  }

  Ctor.prototype[kNext] = function (callback) {
    this[kAwaitIndex](err => {
      if (err) return this.nextTick(callback, err)
      const key = this[kAdvance]()
      if (!key) return this.nextTick(callback)
      getValue(this[kLocation], key, (err, value) => {
        this.nextTick(callback, err, key, value)
      })
    })
  }

  Ctor.prototype[kGetKeyBatch] = function (batchSize) {
    const keys = this[kIndexKeys]
    const cursor = this[kCursor]
    if (cursor > keys.length - 1) {
      return []
    }
    if (!batchSize) {
      batchSize = this[kBatchSize]
    }
    const batch = keys.slice(cursor, cursor + batchSize)
    this[kCursor] += batch.length
    this[kDone] = (this[kCursor] >= keys.length)
    return batch
  }

  Ctor.prototype[kNextv] = function (size, options, callback) {
    this[kAwaitIndex](err => {
      if (err) return this.nextTick(callback, err)
      const location = this[kLocation]
      const concurrency = this[kConcurrency]

      const keys = this[kGetKeyBatch](size)

      getMany(location, keys, concurrency, (err, values) => {
        if (err) return this.nextTick(callback, err)
        const result = []
        for (let i = 0; i < keys.length; i++) {
          result.push([keys[i], values[i]])
        }
        this.nextTick(callback, null, result)
      })
    })
  }

  Ctor.prototype[kAll] = function (options, callback) {
    this[kAwaitIndex](err => {
      if (err) return this.nextTick(callback, err)

      if (this[kDone]) {
        return this.nextTick(callback)
      }

      const result = []
      const location = this[kLocation]
      const concurrency = this[kConcurrency]

      const loop = () => {
        const keys = this[kGetKeyBatch]()

        if (keys.length === 0) {
          return this.nextTick(callback, null, result)
        }

        getMany(location, keys, concurrency, (err, values) => {
          if (err) return this.nextTick(callback, err)
          values.forEach((value, i) => {
            result.push([keys[i], value])
          })
          if (values.length === 0) {
            return this.nextTick(callback, null, result)
          }

          this.nextTick(loop)
        })
      }

      this.nextTick(loop)
    })
  }

  Ctor.prototype._seek = function (target, options) {
    this[kSeekOptions] = {
      ...options,
      target
    }
  }

  Ctor.prototype[kAwaitIndex] = function (callback) {
    const promise = this[kIndexPromise]
    const options = this[kSeekOptions]

    promise.then(() => {
      if (options) {
        this[kSeekOptions] = null
        this[kHandleSeek](options)
      }
      this.nextTick(callback)
    }).catch(err => {
      this.nextTick(callback, err)
    })
  }

  Ctor.prototype[kHandleSeek] = function (options) {
    const target = options.target
    const keys = this[kIndexKeys]
    const reverse = this[kReverse]
    let pos
    if (reverse) {
      pos = bounds.le(keys, target, compare)
    } else {
      pos = bounds.ge(keys, target, compare)
    }
    this[kCursor] = pos
    this[kDone] = (pos < 0) || (pos >= keys.length)
  }
}

class CacacheLevel extends AbstractLevel {
  constructor (location, options) {
    // Take a dummy location argument to align with other implementations
    if (typeof location === 'object' && location !== null) {
      options = location
    }

    const {
      rootDirectory,
      ...forward
    } = options || {}

    super({
      seek: true,
      permanence: true,
      createIfMissing: false,
      errorIfExists: false,
      encodings: {
        buffer: true
      },
      additionalMethods: {
        getInfo: true,
        verify: true,
        destroy: true
      }
    }, forward)

    // todo: validate location is valid path segment
    this[kLocation] = location
    this[kBatchSize] = options.batchSize || DEFAULT_BATCH_SIZE
    this[kConcurrency] = options.concurrency || DEFAULT_CONCURRENCY
  }

  get type () {
    return 'cacache-level'
  }

  getInfo (key, callback) {
    const location = this[kLocation]
    key = encodeKey(key)
    return fromPromise(cacache.get.info(location, key).catch(err => {
      if (err.code === 'ENOENT') {
        return null
      }
      throw err
    }), callback)
  }

  destroy (callback) {
    const location = this[kLocation]
    return fromPromise(cacache.rm.all(location), callback)
  }

  _put (key, value, options, callback) {
    const cachePath = this[kLocation]
    const metadata = options.metadata
    key = encodeKey(key)
    cacache.put(cachePath, key, value, metadata).then(() => {
      this.nextTick(callback)
    }).catch(err => {
      this.nextTick(callback, err)
    })
  }

  _get (key, options, callback) {
    const cachePath = this[kLocation]

    getValue(cachePath, key, (err, entry) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, entry)
    })
  }

  _getMany (keys, options, callback) {
    const cachePath = this[kLocation]
    const concurrency = this[kConcurrency]
    getMany(cachePath, keys, concurrency, (err, values) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, values)
    })
  }

  _del (key, options, callback) {
    const cachePath = this[kLocation]
    key = encodeKey(key)
    // todo: handle removeFully (see cacache docs)
    cacache.rm.entry(cachePath, key).then(() => {
      // todo: mark for compaction
      this.nextTick(callback)
    }).catch(err => {
      this.nextTick(callback, err)
    })
  }

  _batch (operations, options, callback) {
    const cachePath = this[kLocation]
    const concurrency = this[kConcurrency]

    const handlePut = (operations) => {
      return pMap(operations, (operation) => {
        const { key, value, metadata = {} } = operation
        const _key = encodeKey(key)
        return cacache.put(cachePath, _key, value, metadata)
      }, { concurrency })
    }

    const handleDel = (operations) => {
      return pMap(operations, (operation) => {
        const key = encodeKey(operation.key)
        return cacache.rm.entry(cachePath, key)
      }, { concurrency })
    }

    let idx = 0

    const loop = () => {
      const processOp = (opType, ops) => {
        let fn
        if (opType === 'put') {
          fn = handlePut
        } else if (opType === 'del') {
          fn = handleDel
        } else {
          const err = new Error(`Unknown operation type: ${opType}`)
          return this.nextTick(callback, err)
        }

        fn(ops)
          .then(() => this.nextTick(loop))
          .catch((e) => this.nextTick(callback, e))
      }

      if (idx >= operations.length) {
        return this.nextTick(callback)
      }
      const lastOpType = operations[idx].type
      const ops = []
      while (idx < operations.length && operations[idx].type === lastOpType) {
        ops.push(operations[idx++])
      }

      return processOp(lastOpType, ops)
    }

    this.nextTick(loop)
  }

  _clear (options, callback) {
    const cachePath = this[kLocation]
    if (options.limit === Infinity && !Object.keys(options).some(isRangeOption)) {
      // Delete everything.
      cacache.rm.all(cachePath).then(() => {
        this.nextTick(callback)
      }).catch(err => {
        this.nextTick(callback, err)
      })
    }

    const handleClear = (iter) => {
      const location = iter[kLocation]
      const concurrency = iter[kConcurrency]

      const loop = () => {
        const keys = iter[kGetKeyBatch]()
        if (keys.length > 0) {
          pMap(keys, (key) => {
            return cacache.rm.entry(location, key)
              .catch((err) => {
                // ignore not found errors
                if (err.code === 'ENOENT') {
                  return
                }
                throw err
              })
          }, { concurrency })
            .then(() => {
              this.nextTick(loop)
            }).catch(err => {
              this.nextTick(callback, err)
            })
        }

        this.nextTick(callback)
      }

      this.nextTick(loop)
    }

    const iterator = this._keys(options)
    iterator[kAwaitIndex](err => {
      if (err) return this.nextTick(callback, err)
      handleClear(iterator)
    })
  }

  _iterator (options) {
    return new Iterator(this, options)
  }

  _keys (options) {
    return new KeyIterator(this, options)
  }

  _values (options) {
    return new ValueIterator(this, options)
  }

  verify (options, callback) {
    const cachePath = this[kLocation]
    if (typeof options === 'function') {
      callback = options
      options = null
    } else if (typeof options !== 'object') {
      options = null
    }

    return fromPromise(cacache.verify(cachePath, options), callback)
  }
}

CacacheLevel.destroy = function (location, callback) {
  return fromPromise(cacache.rm.all(location), callback)
}

exports.CacacheLevel = CacacheLevel

function isRangeOption (k) {
  return rangeOptions.has(k)
}

function encodeKey (key) {
  // since encoding apply both to key and value, we need to handle key encoding
  // since cacache only handles string keys. For reference, we specified 'buffer' and 'utf8'
  // in the CacacheLevel constructor.
  if (Buffer.isBuffer(key)) {
    return key.toString()
  }
  return key
}

function getValue (location, key, callback) {
  fetch(location, key).then((data) => {
    if (data === undefined) {
      callback(new Error('NotFound'))
    } else {
      callback(null, data)
    }
  }).catch(callback)
}

async function fetch (location, key) {
  try {
    const value = await cacache.get(location, encodeKey(key))
    if (value) {
      return value.data
    }
    return undefined
  } catch (err) {
    if (err.code === 'ENOENT') {
      return undefined
    }
    throw err
  }
}

function getMany (location, keys, concurrency, callback) {
  if (keys.length === 0) {
    process.nextTick(() => callback(null, []))
  }
  pMap(keys, (key) => fetch(location, key), { concurrency })
    .then(() => process.nextTick(callback))
    .catch(err => process.nextTick(callback, err))
}
