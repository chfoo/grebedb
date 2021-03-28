use grebedb::{Database, Options};

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
    let mut database = Database::open_memory(Options::default())?;

    database.put("k", "v")?;

    let handle = std::thread::spawn(move || {
        database.get("k").unwrap();
    });

    handle.join().unwrap();

    Ok(())
}
