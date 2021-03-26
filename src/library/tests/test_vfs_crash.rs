mod common;

use common::CrashingVfs;
use grebedb::{Database, DatabaseOptions};

#[test]
fn test_crash_before_metadata_commit() {
    let vfs = CrashingVfs::new(1);
    let mut database = Database::open(Box::new(vfs.clone()), DatabaseOptions::default()).unwrap();

    // metadata written count: 1

    database.put("key", "hello world").unwrap();

    // New copy-on-write pages should be written successfully,
    // the metadata should fail to be renamed
    database.flush().unwrap_err();

    // metadata written count: 2

    // Expect old pages with revision flag 0 to be read, and flag 1 to be ignored:
    vfs.set_threshold(usize::MAX);
    let mut database = Database::open(Box::new(vfs), DatabaseOptions::default()).unwrap();

    assert_eq!(database.get("key").unwrap(), None);
}

#[test]
fn test_crash_after_metadata_commit() {
    let vfs = CrashingVfs::new(2);
    let mut database = Database::open(Box::new(vfs.clone()), DatabaseOptions::default()).unwrap();

    // metadata written count: 1

    database.put("key", "hello world").unwrap();

    // New copy-on-write pages should be written successfully,
    // the metadata should to be renamed successfully,
    // but subsequent copy-on-write pages should fail to rename from revision flag 1 to 0
    database.flush().unwrap_err();

    // metadata written count: 2

    // Expect read new pages in either copy-on-write revision flag 0 or 1:
    vfs.set_threshold(usize::MAX);
    let mut database = Database::open(Box::new(vfs), DatabaseOptions::default()).unwrap();

    assert_eq!(
        database
            .get("key")
            .unwrap()
            .map(|item| String::from_utf8(item).unwrap()),
        Some("hello world".to_string())
    );
}
