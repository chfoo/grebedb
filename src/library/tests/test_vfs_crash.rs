mod common;

use std::sync::atomic::Ordering;

use common::CrashingVfs;
use grebedb::{Database, DatabaseOptions};

#[test]
fn test_crash_before_metadata_commit() {
    let vfs = CrashingVfs::new();
    let options = DatabaseOptions {
        keys_per_node: 128,
        page_cache_size: 4,
        automatic_flush: false,
        ..Default::default()
    };
    let mut database = Database::open(Box::new(vfs.clone()), options).unwrap();

    for num in 0..2000 {
        database
            .put(format!("key:{:04x}", num), "hello world")
            .unwrap();

        if num == 1000 {
            database.flush().unwrap();
        }
    }

    for num in 0..2000 {
        database.get(format!("key:{:04x}", num)).unwrap();
    }

    database.put("key:0000", "new value").unwrap(); // a key near start
    database.put("key:07A0", "new value").unwrap(); // a key near end

    // New copy-on-write pages should be written successfully,
    // the metadata should fail to be renamed
    vfs.metadata_rename_crash.store(true, Ordering::Relaxed);
    database.flush().unwrap_err();

    // Expect old pages with revision flag 0 to be read, and flag 1 to be ignored:
    let mut database = Database::open(Box::new(vfs), DatabaseOptions::default()).unwrap();

    assert_eq!(
        database
            .get("key:0000")
            .unwrap()
            .map(|item| String::from_utf8(item).unwrap()),
        Some("hello world".to_string())
    );
    assert_eq!(database.get("key:07A0").unwrap(), None);
}

#[test]
fn test_crash_after_metadata_commit() {
    let vfs = CrashingVfs::new();
    let options = DatabaseOptions {
        keys_per_node: 128,
        page_cache_size: 4,
        automatic_flush: false,
        ..Default::default()
    };
    let mut database = Database::open(Box::new(vfs.clone()), options).unwrap();

    for num in 0..2000 {
        database
            .put(format!("key:{:04x}", num), "hello world")
            .unwrap();

        if num == 1000 {
            database.flush().unwrap();
        }
    }

    for num in 0..2000 {
        database.get(format!("key:{:04x}", num)).unwrap();
    }

    database.put("key:0000", "new value").unwrap(); // a key near start
    database.put("key:07A0", "new value").unwrap(); // a key near end

    // New copy-on-write pages should be written successfully,
    // the metadata should to be renamed successfully,
    // but subsequent copy-on-write pages should fail to rename from revision flag 1 to 0
    vfs.after_metadata_rename_crash
        .store(true, Ordering::Relaxed);

    database.flush().unwrap_err();

    vfs.after_metadata_rename_crash
        .store(false, Ordering::Relaxed);

    // Expect read new pages in either copy-on-write revision flag 0 or 1:
    let mut database = Database::open(Box::new(vfs), DatabaseOptions::default()).unwrap();

    assert_eq!(
        database
            .get("key:0000")
            .unwrap()
            .map(|item| String::from_utf8(item).unwrap()),
        Some("new value".to_string())
    );
    assert_eq!(
        database
            .get("key:07A0")
            .unwrap()
            .map(|item| String::from_utf8(item).unwrap()),
        Some("new value".to_string())
    );
}
