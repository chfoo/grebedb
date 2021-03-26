use grebedb::{
    vfs::{MemoryVfs, ReadOnlyVfs},
    Database, DatabaseOpenMode, DatabaseOptions,
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
