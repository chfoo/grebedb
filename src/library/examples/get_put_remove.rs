fn main() -> Result<(), grebedb::Error> {
    // A directory is used to store a GrebeDB database.
    let path = std::path::PathBuf::from("grebedb_example_data/get_put_remove/");

    std::fs::create_dir_all(&path)?;

    let options = grebedb::DatabaseOptions::default();
    let mut db = grebedb::Database::open_path(path, options)?;

    // Store some key-values
    db.put("key1", "hello world 1!")?;
    db.put("key2", "hello world 2!")?;
    db.put("key3", "hello world 3!")?;

    // Getting some values
    println!("The value of key1 is {:?}", db.get("key1")?);
    println!("The value of key2 is {:?}", db.get("key2")?);
    println!("The value of key3 is {:?}", db.get("key3")?);

    // Deleting a key-value
    db.remove("key2")?;

    println!("The value of key2 is {:?}", db.get("key2")?);

    // Data stored in internal cache is automatically written to the
    // file system when needed, but this only happens when a database
    // operation function is called or when the database is dropped.
    //
    // If you need to ensure all data is persisted at a given time,
    // you can call flush().
    db.flush()?;

    Ok(())
}
