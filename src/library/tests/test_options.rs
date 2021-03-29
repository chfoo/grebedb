mod common;

use grebedb::{
    vfs::{MemoryVfs, ReadOnlyVfs},
    CompressionLevel, Database, OpenMode, Options, SyncOption,
};

#[test]
fn test_read_only() -> anyhow::Result<()> {
    let memory_vfs = MemoryVfs::default();
    let options = Options {
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

    let options = Options {
        open_mode: OpenMode::ReadOnly,
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
fn test_create_only() -> anyhow::Result<()> {
    let memory_vfs = MemoryVfs::default();
    let options = Options::default();
    let mut db = Database::open(Box::new(memory_vfs.clone()), options)?;

    for num in 0..1000 {
        let key = format!("key:{:016x}", num);
        let value = format!("hello world {}", num);
        db.put(key, value)?;
    }

    db.flush()?;
    drop(db);

    let options = Options {
        open_mode: OpenMode::CreateOnly,
        ..Default::default()
    };

    let result = Database::open(Box::new(memory_vfs), options);

    assert!(result.is_err());

    Ok(())
}

#[test]
fn test_load_only() {
    let options = Options {
        open_mode: OpenMode::LoadOnly,
        ..Default::default()
    };

    let result = Database::open_memory(options);

    assert!(result.is_err());
}

#[test]
fn test_no_compression() -> anyhow::Result<()> {
    let vfs = MemoryVfs::default();
    let options = Options {
        compression_level: CompressionLevel::None,
        ..Default::default()
    };
    let mut db = Database::open(Box::new(vfs.clone()), options.clone())?;

    db.put("my key", "hello world")?;
    db.flush()?;

    let mut db = Database::open(Box::new(vfs), options)?;

    assert!(db.get("my key")?.is_some());

    Ok(())
}

#[test]
fn test_no_file_locking() -> anyhow::Result<()> {
    let dir = common::make_tempdir();
    let options = Options {
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
    let options = Options {
        file_sync: SyncOption::None,
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
