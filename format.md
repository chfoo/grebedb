# GrebeDB file format

This file describes the GrebeDB file format.

Note: This document may not be complete and accurate.

All GrebeDB files use the same format:

1. Magic bytes `0xFE 0xC7 0xF2 0xE5 0xE2 0xE5 0x00 0x00`.
2. Compression flag (1 byte) for the Page.

    * `0x00`: none (no compression)
    * `0x01`: compressed

3. Page: contains encapsulated data.

    * Optionally compressed using the Compression Flag.
    * If compressed, only Zstandard format is supported. Format is detected using magic bytes specified by the format.

## Page

The Page contains the format:

1. Payload size: 8 bytes of a 64-bit big-endian unsigned integer indicating the length of the Payload.
2. Payload: MessagePack encoded data
3. Checksum: CRC-32C (Castagnoli) checksum of the Payload in 4 bytes of a 32-bit big-endian unsigned integer.

## Payload

Page is MessagePack encoded data. The object is always a map with keys as strings.

The metadata page has the key-value pairs:

* `uuid` (16 byte binary): UUID for the instance of the database. This value is used to prevent mix up of files belonging to other instances.
* `revision` (u64): Revision counter.
* `id_counter` (u64): Page ID counter. It is incremented when a new ID is required.
* `free_id_list` (u64 array): Unused page IDs.
* `root_id` (u64, optional): Page ID containing the root node.

The content page has the key-value pairs:

* `uuid` (16 byte binary): Same usage as above.
* `id` (u64): Page ID.
* `revision` (u64): Revision ID. It must be equal or less to the value in the metadata page. A larger value indicates the page was not committed and should be discarded.
* `deleted` (boolean): If true, there is no content and this page ID can be reused.
* `content` (optional): Node data.

## Filename

The lock file uses the filename `grebedb_lock.lock`.

The metadata file uses the filename `grebedb_meta.grebedb`. A copy is saved to `grebedb_meta_bak.grebedb` and a previous copy to `grebedb_meta_prev.grebedb`.

For node files, filenames use the format `ID_PATH/grebedb_ID_REVISION.grebedb` where:

* `ID` (16 character string): lowercase hexadecimal encoded 64-bit big-endian unsigned integer.
* `REVISION` (1 character string): digit `0` or `1`. Implementations use the page that contains the greatest valid revision ID.
* `ID_PATH`: the first 14 characters of ID split into 7 directories (for example, `ab/cd/ef/01/23/45/67`).

## Node

Nodes are an externally tagged enum represented as one of:

* String `empty_root` indicating an empty tree.
* Map with one of the string keys:
  * `internal`: Internal node
  * `leaf`: Leaf node

### Internal node

Internal nodes are a map with key-value pairs:

* `keys` (array of binary): Keys in an B+ tree internal node.
* `children` (array of u64): Array of child node page IDs.

### Leaf node

Leaf nodes are a map with key-value pairs:

* `keys` (array of binary): Keys in a B+ tree leaf node.
* `values` (array of binary): Contains the values.
