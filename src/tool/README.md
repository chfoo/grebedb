# grebedb-tool

A command-line program to provide basic manipulation (such as import, export) and debugging on [GrebeDB](https://github.com/chfoo/grebedb) databases.

Run `cargo install grebedb-tool` to install the tool to `$HOME/.cargo/bin`, then run `grebedb-tool --help` to show program options.

## Quick start

To export a database to a file:

    grebedb-tool export path/to/database/ database.json-seq

To import a database from a file:

    grebedb-tool import path/to/database/ database.json-seq

To export to a compressed file:

    grebedb-tool export path/to/database/ | zstd -o database.json-seq.zst

To import from a compressed file:

    unzstd < database.json-seq.zst | grebedb-tool import path/to/database/
