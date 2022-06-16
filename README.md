# cacache-level

- [`cacache`](https://github.com/npm/cacache) based [`abstract-level`][abstract-level] database for Node.js.

Requirements:
- node-12.x

[![level badge][level-badge]](https://github.com/Level/awesome)
[![Standard](https://img.shields.io/badge/standard-informational?logo=javascript&logoColor=fff)](https://standardjs.com)
[![Common Changelog](https://common-changelog.org/badge.svg)](https://common-changelog.org)

## Usage

```js
const { CacacheLevel } = require('cacache-level')

// Create a database
const db = new CacacheLevel('/path/to/dir/tokens', {  batchSize: 10 })

// Add an entry with key 'a' and value 1
await db.put('a', 1)

// add optional metadata 
await db.put('user-token', token, { metadata: { machineId: getMachineId(), version: '1.2.3' } })

// Add multiple entries
await db.batch([{ type: 'put', key: 'b', value: 2 }])

// with metadata
await db.batch([{ type: 'put', key: 'b', value: 2, metadata: { version: '1.2.3' } }])

// Get value of key 'a': 1
const value = await db.get('a')

// get metadata about an entry
const info = await db.getInfo('user-token')

/**
 // Output
 {
  key: 'user-token',
  integrity: 'sha256-MUSTVERIFY+ALL/THINGS=='
  path: '.testcache/content/deadbeef',
  time: 12345698490,
  size: 32,
  metadata: {
    machineId: '90n5K$a(81',
    version: '1.2.3',
  }
}
 */

// Iterate entries with keys that are greater than 'a'
for await (const [key, value] of db.iterator({ gt: 'a' })) {
  console.log(value) // 2
}

// Perform a maintenance sweep of the db
await db.verify( { concurrency: 10 })
```

With callbacks:

```js
db.put('example', { hello: 'world' }, (err) => {
  if (err) throw err

  db.get('example', (err, value) => {
    if (err) throw err
    console.log(value) // { hello: 'world' }
  })
})
```


## API
The API of `cacache-level` follows that of [`abstract-level`](https://github.com/Level/abstract-level) with a few additional options and methods specific to `cacache`. The documentation below covers it all except for [Encodings](https://github.com/Level/abstract-level#encodings), [Events](https://github.com/Level/abstract-level#events) and [Errors](https://github.com/Level/abstract-level#errors) which are exclusively documented in `abstract-level`.

### `db = new CacacheLevel(location[, options])`

Returns a new **CacacheLevel** instance. `location` is the file system path of the directory containing the data.

Aside from the basic `abstract-level` options, the optional `options` object may contain:

- `concurrency` concurrency of batch operations (including getMany). Defaults to `os.cpus().length * 4`.
- `batchSize` default batch size for various bulk operations, like `clear()`. Defaults to `25`.

The `createIfMissing` option of `abstract-level` is ignored, as this is the way that `redis` works.


## Install

With [npm](https://npmjs.org) do:

```
npm install cacache-level
```

## Credits

Copyright [clayton collie](https://github.com/ccollie).

## Contributing

[`Level/cacache-level`](https://github.com/ccollie/cacache-level) is an **OPEN Open Source Project**. This means that:

> Individuals making significant and valuable contributions are given commit-access to the project to contribute as they see fit. This project is more like an open wiki than a standard guarded open source project.

See the [Contribution Guide](https://github.com/Level/community/blob/master/CONTRIBUTING.md) for more details.

## License

[MIT](LICENSE)

[abstract-level]: https://github.com/Level/abstract-level

[level-badge]: https://leveljs.org/img/badge.svg
