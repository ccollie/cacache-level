'use strict'

const {
  AbstractLevel,
  AbstractIterator,
  AbstractKeyIterator,
  AbstractValueIterator
} = require('abstract-level')

const cacache = require('cacache')
const { fromPromise } = require('catering')
const os = require('os')
const pMap = require('p-map')

const rangeOptions = new Set(['gt', 'gte', 'lt', 'lte'])
const kNone = Symbol('none')
const kDb = Symbol('db')
const kDone = Symbol('done')
const kLowerBound = Symbol('lowerBound')
const kUpperBound = Symbol('upperBound')
const kReverse = Symbol('reverse')
const kOptions = Symbol('options')
const kAdvanceNext = Symbol('advanceNext')
const kAdvancePrev = Symbol('advancePrev')
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

const DEFAULT_BATCH_SIZE = 50
const DEFAULT_CONCURRENCY = os.cpus().length * 3

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
    this[kAwaitIndex]().then(() => {
      const key = this[kAdvance]()
      this.nextTick(callback, null, key)
    }).catch(callback)
  }

  _nextv (size, options, callback) {
    this[kAwaitIndex]().then(() => {
      if (this[kDone]) return this.nextTick(callback)
      const keys = []

      while (!this[kDone] && keys.length < size) {
        const key = this[kAdvance]()
        if (key === null) break
        keys.push(key)
      }

      this.nextTick(callback, null, keys)
    }).catch(callback)
  }

  _all (options, callback) {
    this[kAwaitIndex]().then(() => {
      const keys = []

      while (!this[kDone]) {
        const key = this[kAdvance]()
        if (key === null) break
        keys.push(key)
      }

      this.nextTick(callback, null, keys)
    }).catch(callback)
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
    this[kNextv](size, options, (err, values) => {
      if (err) return this.nextTick(callback, err)
      const result = values.map(value => value[1])
      this.nextTick(callback, null, result)
    })
  }

  _all (options, callback) {
    this[kAll](options, (err, values) => {
      if (err) return this.nextTick(callback, err)
      const result = values.map(value => value[1])
      this.nextTick(callback, null, result)
    })
  }
}

for (const Ctor of [Iterator, KeyIterator, ValueIterator]) {
  Ctor.prototype[kInit] = function (db, options) {
    const location = db[kLocation]
    const reverse = options.reverse

    this[kDb] = db
    this[kCursor] = 0
    this[kReverse] = reverse
    this[kOptions] = options
    this[kBatchSize] = db[kBatchSize]
    this[kConcurrency] = db[kConcurrency]

    this[kAdvance] = this[kReverse] ? this[kAdvancePrev] : this[kAdvanceNext]

    this[kIndexPromise] = cacache.ls(location).then(entries => {
      const keys = entries.map(entry => entry.key).sort(compare)

      this[kEntries] = entries
      const { start, end, lowerBound, upperBound } = parseBounds(keys, options)

      this[kCursor] = 0
      this[kLowerBound] = lowerBound
      this[kUpperBound] = upperBound
      this[kIndexKeys] = keys.splice(start, end - start + 1)
    })
  }

  Ctor.prototype[kAdvanceNext] = function () {
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

  Ctor.prototype[kAdvancePrev] = function () {
    if (this[kDone]) {
      return null
    }
    let cursor = this[kCursor]
    if (cursor > 0) {
      const key = this[kIndexKeys][cursor--]
      this[kCursor] = cursor
      this[kDone] = (cursor === 0)
      return key
    } else {
      this[kDone] = true
      return null
    }
  }

  Ctor.prototype[kNext] = function (callback) {
    this[kAwaitIndex]().then(() => {
      const key = this[kAdvance]()
      if (!key) return this.nextTick(callback)
      getValue(this[kLocation], key, (err, value) => {
        this.nextTick(callback, err, key, value.data)
      })
    }).catch(callback)
  }

  function getKeyBatch (iter, batchSize) {
    const slice = []
    do {
      const key = iter[kAdvance]()
      if (!key) break
      slice.push(key)
    } while (slice.length < batchSize)

    return slice
  }

  Ctor.prototype[kNextv] = function (size, options, callback) {
    this[kAwaitIndex]().then(() => {
      const location = this[kLocation]
      const concurrency = this[kConcurrency]

      const keys = getKeyBatch(this, size)

      getMany(location, keys, concurrency, (err, values) => {
        if (err) return this.nextTick(callback, err)
        const result = []
        for (let i = 0; i < keys.length; i++) {
          result.push([keys[i], values[i].data])
        }
        this.nextTick(callback, null, result)
      })
    }).catch(callback)
  }

  Ctor.prototype[kAll] = function (options, callback) {
    this[kAwaitIndex]().then(() => {
      if (this[kDone]) {
        return this.nextTick(callback)
      }

      const result = []
      const location = this[kLocation]
      const concurrency = this[kConcurrency]
      const batchSize = this[kBatchSize]

      const loop = () => {
        const keys = getKeyBatch(this, batchSize)

        if (keys.length === 0) {
          return this.nextTick(callback, null, result)
        }

        getMany(location, keys, concurrency, (err, values) => {
          if (err) return callback(err)
          values.forEach((value, i) => {
            result.push([keys[i], value.data])
          })
          if (values.length === 0) {
            return this.nextTick(callback, null, result)
          }

          this.nextTick(loop)
        })
      }
      this.nextTick(loop)
    }).catch(callback)
  }

  Ctor.prototype._seek = function (target, options) {
    this[kSeekOptions] = {
      ...options,
      target
    }
  }

  Ctor.prototype[kAwaitIndex] = function () {
    const options = this[kSeekOptions]
    if (!options) {
      return this[kIndexPromise]
    }
    this[kSeekOptions] = null

    // handle seek
    return this[kIndexPromise].then(() => {
      const target = options.target
      const keys = this[kIndexKeys]
      const reverse = this[kReverse]
      if (!reverse) {
        const beforeStart = compare(target, keys[0]) < 0
        const afterEnd = compare(target, keys[keys.length - 1]) > 0
        if (beforeStart) {
          if ('gt' in options || 'gte' in options) {
            this[kDone] = false
            this[kCursor] = 0
          } else {
            this[kDone] = true
            this[kCursor] = keys.length
          }
          return
        } else if (afterEnd) {
          // example: keys =  0 1 2 3 4 5 6 7 8 9 and  { target: { lte: 1000 } }
          if ('lt' in options || 'lte' in options) {
            this[kDone] = false
            this[kCursor] = 0
          } else {
            this[kDone] = true
            this[kCursor] = -1
          }
          return
        }

        let pos = binarySearch(keys, target, compare)
        if (pos < 0) {
          // -1 means the target is less than the first key
          if (pos === -1) {
            if (options.gt || options.gte) {
              pos = 0
            } else {
              this[kDone] = true
              pos = keys.length
            }
          } else {
            // pos represents the position of the first key that is greater than target
            pos = -pos
          }
        } else {
          if ('gt' in options) {
            pos++
          }
        }

        this[kCursor] = pos
        this[kDone] = (pos >= keys.length)
      } else {
        const beforeStart = compare(target, keys[keys.length - 1]) > 0
        const afterEnd = compare(target, keys[0]) < 0

        if (beforeStart) {
          // example: keys =  9 8 7 6 5 4 3 2 1 0  and { target: { lte: 1000 } }
          if ('lt' in options || 'lte' in options) {
            this[kDone] = false
            this[kCursor] = keys.length - 1
          } else {
            this[kDone] = true
            this[kCursor] = -1
          }
          return
        } else if (afterEnd) {
          // example: keys =  0 1 2 3 4 5 6 7 8 9 and { target: { gt: -1 } }
          if ('gt' in options || 'gte' in options) {
            this[kDone] = false
            this[kCursor] = keys.length - 1
          } else {
            this[kDone] = true
            this[kCursor] = 0
          }
          return
        }
        let pos = binarySearch(keys, target, compare)
        if (pos < 0) {
          // -1 means the target is greater than the last key
          if (pos === -1) {
            this[kCursor] = keys.length - 1
            if (options.lt || options.lte) {
              this[kDone] = false
              this[kCursor] = keys.length - 1
            } else {
              this[kDone] = true
              this[kCursor] = -1
            }
            return
          } else {
            // pos represents the position of the last key that is less than target
            pos = -pos - 1
          }
        } else {
          if ('gt' in options) {
            pos--
          }
        }
        this[kCursor] = pos
      }
    }).catch(err => {
      this[kDone] = true
      this[kCursor] = -1
      return err
    })
  }
}

class CacacheLevel extends AbstractLevel {
  constructor (location, options, _) {
    // Take a dummy location argument to align with other implementations
    if (typeof location === 'object' && location !== null) {
      options = location
    }

    const opts = {
      ...(options || {}),
      keyEncoding: 'utf8',
      valueEncoding: 'buffer'
    }

    super({
      seek: true,
      permanence: true,
      createIfMissing: false,
      errorIfExists: false,
      encodings: {
        buffer: true,
        utf8: true,
        view: true
      },
      additionalMethods: {
        verify: true
      }
    }, opts)

    // todo: validate location is valid path segment
    this[kLocation] = location
    this[kBatchSize] = options.batchSize || DEFAULT_BATCH_SIZE
    this[kConcurrency] = options.concurrency || DEFAULT_CONCURRENCY
  }

  get type () {
    return 'cacache-level'
  }

  destroy (callback) {
    const location = this[kLocation]
    callback = callback || (() => {})
    cacache.rm.all(location)
      .then(() => this.nextTick(callback))
      .catch(err => this.nextTick(callback, err))
  }

  _put (key, value, options, callback) {
    const cachePath = this[kLocation]

    cacache.put(cachePath, key, value).then(() => {
      this.nextTick(callback)
    }).catch(err => {
      this.nextTick(callback, err)
    })
  }

  _get (key, options, callback) {
    const cachePath = this[kLocation]

    getValue(cachePath, key, (err, entry) => {
      if (err) return this.nextTick(callback, err)
      this.nextTick(callback, null, entry.data)
    })
  }

  _getMany (keys, options, callback) {
    const cachePath = this[kLocation]
    const concurrency = this[kConcurrency]
    getMany(cachePath, keys, concurrency, callback)
  }

  _del (key, options, callback) {
    const cachePath = this[kLocation]
    // todo: handle removeFully (see cacache docs)
    cacache.rm.entry(cachePath, key).then(() => {
      // todo; mark for compaction
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
        const { key, value } = operation
        return cacache.put(cachePath, key, value)
      }, { concurrency })
    }

    const handleDel = (operations) => {
      return pMap(operations, (operation) => {
        const { key } = operation
        return cacache.rm.content(cachePath, key)
      }, { concurrency })
    }

    let idx = 0
    const loop = () => {
      if (idx >= operations.length) {
        return this.nextTick(callback)
      }
      const lastOpType = operations[idx].type
      const ops = []
      while (idx < operations.length && operations[idx].type === lastOpType) {
        ops.push(operations[idx])
        idx++
      }
      if (lastOpType === 'put') {
        return handlePut(ops).then(loop).catch(callback)
      } else if (lastOpType === 'del') {
        return handleDel(ops).then(loop).catch(callback)
      }
      this.nextTick(callback)
    }

    this.nextTick(loop)
  }

  _clear (options, callback) {
    const cachePath = this[kLocation]
    if (options.limit === -1 && !Object.keys(options).some(isRangeOption)) {
      // Delete everything.
      cacache.rm.all(cachePath).then(() => {
        this.nextTick(callback)
      }).catch(err => {
        this.nextTick(callback, err)
      })
    }

    const handleClear = (iter) => {
      const location = iter[kLocation]
      const batchSize = iter[kBatchSize]
      const concurrency = iter[kConcurrency]

      const loop = () => {
        const keys = []
        while (keys.length < batchSize) {
          const key = iter[kAdvance]()
          if (!key) break
          keys.push(key)
        }
        if (keys.length > 0) {
          pMap(keys, (key) => {
            return cacache.rm.entry(location, key)
          }, { concurrency })
            .then(loop)
            .catch(err => {
              this.nextTick(callback, err)
            })
        }

        this.nextTick(callback)
      }

      this.nextTick(loop)
    }

    const iterator = this._keys(options)
    iterator[kAwaitIndex]().then(handleClear).catch(callback)
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
  callback = callback || (() => {})
  cacache.rm.all(location)
    .then(() => process.nextTick(callback))
    .catch(err => process.nextTick(() => callback(err)))
}

exports.CacacheLevel = CacacheLevel

function isRangeOption (k) {
  return rangeOptions.has(k)
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
    const data = await cacache.get(location, key)
    return data || undefined
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
    .then((results) => callback(null, results))
    .catch(callback)
}

/**
 * @param {Array} ar
 * @param {function} compare
 * @returns {number}
 */
function binarySearch (ar, el, compare) {
  let m = 0
  let n = ar.length - 1
  while (m <= n) {
    const k = (n + m) >> 1
    const cmp = compare(el, ar[k])
    if (cmp > 0) {
      m = k + 1
    } else if (cmp < 0) {
      n = k - 1
    } else {
      return k
    }
  }
  return -m - 1
}

function parseBounds (keys, options) {
  let upperBound
  let lowerBound

  let start = 0
  let end = keys.length - 1
  const reverse = !!options.reverse

  if (!reverse) {
    lowerBound = 'gte' in options ? options.gte : 'gt' in options ? options.gt : kNone
    upperBound = 'lte' in options ? options.lte : 'lt' in options ? options.lt : kNone

    if (upperBound === kNone) {
      start = 0
    } else {
      start = binarySearch(keys, upperBound, compare)
      if (start < 0) {
        if (start === -1) {
          // item not found in the list, start at the first item
          start = 0
        } else {
          // start is the -1 * index where item would have been inserted
          start = -start
        }
      } else {
        if ('gt' in options) {
          start++
        }
      }
    }

    if (upperBound !== kNone) {
      end = binarySearch(keys, upperBound, compare)
      if (end < 0) {
        if (end === -1) {
          // item not found in the list, end at the last item
          end = keys.length - 1
        } else {
          end = -end - 1
        }
      } else {
        if ('lt' in options) {
          end--
        }
      }
    }
  } else {
    lowerBound = 'lte' in options ? options.lte : 'lt' in options ? options.lt : kNone
    upperBound = 'gte' in options ? options.gte : 'gt' in options ? options.gt : kNone

    if (lowerBound === kNone) {
      start = keys.length - 1
    } else {
      start = binarySearch(keys, upperBound, compare)
      if (start < 0) {
        if (start === -1) {
          // item not found in the list, start at the last item
          start = keys.length - 1
        } else {
          // start is the -1 * index where item would have been inserted
          start = -start - 1
        }
      }
      if ('lt' in options) {
        start--
      }
    }

    if (upperBound !== kNone) {
      end = binarySearch(keys, upperBound, compare)
      if (end < 0) {
        if (end === -1) {
          end = keys.length - 1
        } else {
          end = Math.abs(end)
        }
      }
      // 0 1 2 3 4 5 6 7 8 9 10
      // suppose we have gt 5, we increment end to 6
      if ('gt' in options) {
        end++
      }
    } else {
      end = keys.length - 1
    }
  }

  return { start, end, lowerBound, upperBound }
}
