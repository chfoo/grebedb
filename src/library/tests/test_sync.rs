use grebedb::{Database, DatabaseOptions};

#[test]
fn test_can_be_send() -> anyhow::Result<()> {
    let mut database = Database::open_memory(DatabaseOptions::default())?;

    database.put("k", "v")?;

    let handle = std::thread::spawn(move || {
        database.get("k").unwrap();
    });

    handle.join().unwrap();

    Ok(())
}
