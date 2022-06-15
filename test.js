'use strict'
const test = require('tape')
const suite = require('abstract-level/test')
const { CacacheLevel } = require('.')
const mkdirp = require('mkdirp')
const rimraf = require('rimraf')
const { resolve, join } = require('path')

const CACHE_DIR = resolve(__dirname, '.test-cache')
let idx = 0

const location = function () {
  return '__Level_db__' + idx++
}

function createCacheDir (sub) {
  if (!sub) sub = location()
  const me = resolve(join(CACHE_DIR, sub))
  rimraf.sync(me)
  mkdirp.sync(me)
  return me
}

// Test abstract-level compliance
suite({
  test,
  factory: (...args) => {
    const location = createCacheDir()
    test.onFailure(() => rimraf.sync(location))
    return new CacacheLevel(location, { ...args })
  }
})
