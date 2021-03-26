use grebedb::{Database, DatabaseOptions};

#[test]
fn test_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Database>();
}

#[test]
fn test_sync() {
    fn assert_sync<T: Sync>() {}
    assert_sync::<Database>();
}

#[test]
fn test_send_thread() -> anyhow::Result<()> {
    let mut database = Database::open_memory(DatabaseOptions::default())?;

    database.put("k", "v")?;

    let handle = std::thread::spawn(move || {
        database.get("k").unwrap();
    });

    handle.join().unwrap();

    Ok(())
}
