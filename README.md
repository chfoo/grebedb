# GrebeDB

GrebeDB is a Rust library that provides a lightweight embedded key-value store/database backed by files in a virtual file system interface. It is intended for single-process applications that prefer a key-value database-like interface to data instead operating on file formats directly.

![Crates.io](https://img.shields.io/crates/v/grebedb) ![docs.rs](https://img.shields.io/docsrs/grebedb)

Note: The library is *not* production-ready.

## Design summary (limitations and guarantees)

Since there are too many key-value stores, the design of the database is immediately described upfront for you to decide whether GrebeDB is fit for your use:

* The database is implemented as a B+ tree with each node saved to a file.
  * Both keys and values are treated as binary data. Keys with prefixes are not optimized.
  * Values are stored inline with the leaf nodes.
  * Lazy deletion is performed.
* The size of each file is not fixed and can vary significantly depending on the data stored and configuration options.
* Files are stored using a virtual file system interface. The implementation can be in memory, on a real disk, or your own implementation. Performance and durability is dependent on the file system.
* Operations such as `get`, `put`, `remove`, and `cursor` are provided. There's no support for transactions, but each operation themselves are atomic. Consistency is provided by file copy-on-writes, incrementing revision counters, and atomic file renames.
* Compression of the file with Zstandard can be used.
* Concurrency is not supported. No threads are used for background tasks.

For details about the file format, see [format.md](format.md)

## Getting started

Remember to add the `grebedb` crate dependency to your Cargo.toml.

A GrebeDB database is stored as multiple files in a directory. The following creates a database using the given path and default options:

```rust
let options = Options::default();
let mut db = Database::open_path("path/to/empty/directory/", options)?;
```

Storing, retrieving, and deleting keys is as simple as using the `get()`, `put()`, and `remove()` functions:

```rust
db.put("my_key", "hello world")?;

println!("The value of my_key is {:?}", db.get("my_key")?);

db.remove("my_key")?;

println!("The value of my_key is now {:?}", db.get("my_key")?);
```

To get all the key-values, use `cursor()`:

```rust
for (key, value) in db.cursor() {
    println!("key = {}, value = {}", key, value);
}
```

The database uses an internal cache and automatically delays writing data to the file system. If you want to ensure all internally cached data has been persisted to the file system at a certain point, use `flush()`:

```rust
db.flush()?;
```

For more information, check the [src/library/examples](src/library/examples) directory in the source code repository and the [API reference on docs.rs](https://docs.rs/grebedb).

### Features

By default, the `zstd` crate is enabled for compression, `fslock` is for cross-platform file locking, and `getrandom` is a dependency for `uuid`. To disable them, use `default-features = false` in your Cargo.toml file.

## Tool

A command-line tool to provide basic manipulation (such as import, export) and debugging is available.

Run `cargo install grebedb-tool` to install the tool to `$HOME/.cargo/bin`, then run `grebedb-tool --help` to show program options.

## Contributing

Please use the GitHub Issues, Pull Requests, and Discussions if you have any problems, bug fixes, or suggestions.

## License

Copyright 2021 Christopher Foo. Licensed under Mozilla Public License 2.0.
