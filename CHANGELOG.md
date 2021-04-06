# grebedb change log

(This log only contains changes for the library. Changes for the grebedb-tool crate are located in its own file.)

## 0.2.0 (Unreleased)

* `Cursor::set_end_range()` was removed and replaced with `Cursor::set_range()` which accepts a range.
* `Database::cursor_range()` changed to accept a range.
* `Database::cursor()` changed to return a `Result<Cursor>` instead of `Cursor` to better match `Database::cursor_range()`.
* Added `Database::verify()`.

## 0.1.0 (2021-03-28)

Initial version.
