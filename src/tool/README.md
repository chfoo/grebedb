# grebedb-tool

A command-line program to provide basic manipulation (such as import, export) and debugging on [GrebeDB](https://github.com/chfoo/grebedb) databases.

The tool is currently offered only in source code form, so you will need to install Rust first. Once Rust is installed, run `cargo install grebedb-tool` to install the tool to `$HOME/.cargo/bin`, then run `grebedb-tool --help` to show program options. To update the program, run the install command again. To remove the program, run `cargo uninstall grebedb-tool`.

## Quick start

Example commands for running the tool assuming a Unix-like shell.

### Import and export

The import and export commands can be used to create backups of the database contents to a [JSON sequence file](https://tools.ietf.org/html/rfc7464).

The commands can import/export directly to a given filename or through standard input/output.

Export a database to a file:

    grebedb-tool export path/to/database/ database.json-seq

or with compression:

    grebedb-tool export path/to/database/ | zstd -o database.json-seq.zst

To import a database from a file:

    grebedb-tool import path/to/database/ database.json-seq

or with compression:

    unzstd < database.json-seq.zst | grebedb-tool import path/to/database/

### Verify

The verify command checks that the database has not been corrupted.

    grebedb-tool verify path/to/database/ --verbose

### Inspect

The inspect command launches an interactive session for browsing and editing the database contents.

    grebedb-tool inspect path/to/database/

Inputting `help` will show all available commands. Inputting `help` and then the name of the command will show all options for a given command.

Note that because the format of the contents depends on the application, the inspect command is not intended as a user-friendly way of directly editing application data.
