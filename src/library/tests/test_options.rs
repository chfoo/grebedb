mod common;

use grebedb::{
    vfs::{MemoryVfs, ReadOnlyVfs},
    Database, DatabaseCompressionLevel, DatabaseOpenMode, DatabaseOptions, DatabaseSyncOption,
};

#[test]
fn test_read_only() -> anyhow::Result<()> {
    let memory_vfs = MemoryVfs::default();
    let options = DatabaseOptions {
        keys_per_node: 128,
        page_cache_size: 4,
        ..Default::default()
    };
    let mut db = Database::open(Box::new(memory_vfs.clone()), options)?;

    for num in 0..1000 {
        let key = format!("key:{:016x}", num);
        let value = format!("hello world {}", num);
        db.put(key, value)?;
    }

    db.flush()?;
    drop(db);

    let options = DatabaseOptions {
        open_mode: DatabaseOpenMode::ReadOnly,
        keys_per_node: 128,
        page_cache_size: 4,
        ..Default::default()
    };
    let mut db = Database::open(Box::new(memory_vfs.clone()), options.clone())?;

    for num in 0..1000 {
        let key = format!("key:{:016x}", num);
        db.get(key)?;
    }

    let mut db = Database::open(Box::new(ReadOnlyVfs::new(Box::new(memory_vfs))), options)?;

    for num in 0..1000 {
        let key = format!("key:{:016x}", num);
        db.get(key)?;
    }

    Ok(())
}

#[test]
fn test_no_compression() -> anyhow::Result<()> {
    let options = DatabaseOptions {
        compression_level: DatabaseCompressionLevel::None,
        ..Default::default()
    };
    let mut db = Database::open_memory(options)?;

    db.put("my key", "hello world")?;
    db.flush()?;

    Ok(())
}

#[test]
fn test_no_file_locking() -> anyhow::Result<()> {
    let dir = common::make_tempdir();
    let options = DatabaseOptions {
        file_locking: false,
        ..Default::default()
    };
    let mut db = Database::open_path(dir.path(), options)?;

    db.put("my key", "hello world")?;
    db.flush()?;

    Ok(())
}

#[test]
fn test_no_file_sync() -> anyhow::Result<()> {
    let dir = common::make_tempdir();
    let options = DatabaseOptions {
        file_sync: DatabaseSyncOption::None,
        keys_per_node: 128,
        page_cache_size: 4,
        ..Default::default()
    };
    let mut db = Database::open_path(dir.path(), options)?;

    for num in 0..1000 {
        db.put(format!("my key {}", num), "hello world")?;
    }
    db.flush()?;

    for num in 0..1000 {
        db.get(format!("my key {}", num))?;
    }

    Ok(())
}
