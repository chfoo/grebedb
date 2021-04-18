# grebedb change log

(This log only contains changes for the library. Changes for the grebedb-tool crate are located in its own file.)

## Unreleased

### General

* When attempting to open a database in read-only or load-only mode, a lock file is no longer written to a directory if does not actually contain a database.

### API

* Added `CompressionLevel::VeryLow`.

## 0.2.0 (2021-04-07)

### General

* Internal file sync_data/sync_all() now only occurs at once during `flush()`, rather than at all the time. This is intended to perform better with OS filesystem buffers.

### API

* `Cursor::set_end_range()` was removed and replaced with `Cursor::set_range()` which accepts a range.
* `Database::cursor_range()` changed to accept a range.
* `Database::cursor()` changed to return a `Result<Cursor>` instead of `Cursor` to better match `Database::cursor_range()`.
* Added `Database::verify()`.
* Added `Vfs::sync_file()`.

### Format

* Added revision 3 for node filenames. (Backwards compatible.)

## 0.1.0 (2021-03-28)

Initial version.
