// Demonstrates key-value operations.

use grebedb::{Database, DatabaseOptions};

fn main() -> Result<(), grebedb::Error> {
    // A directory is used to store a GrebeDB database.
    let path = std::path::PathBuf::from("grebedb_example_data/get_put_remove/");

    std::fs::create_dir_all(&path)?;

    let options = DatabaseOptions::default();
    let mut db = Database::open_path(path, options)?;

    // Store some key-values
    db.put("key:1", "hello world 1!")?;
    db.put("key:2", "hello world 2!")?;
    db.put("key:3", "hello world 3!")?;

    // Getting some values
    println!("The value of key1 is {:?}", db.get("key:1")?);
    println!("The value of key2 is {:?}", db.get("key:2")?);
    println!("The value of key3 is {:?}", db.get("key:3")?);

    // Overwrite the value
    db.put("key:2", "new value")?;

    println!("The value of key2 is {:?}", db.get("key:2")?);

    // Deleting a key-value
    db.remove("key:2")?;

    println!("The value of key2 is {:?}", db.get("key:2")?);

    // Data stored in internal cache is automatically written to the
    // file system when needed, but this only happens when a database
    // operation function is called or when the database is dropped.
    //
    // If you need to ensure all data is persisted at a given time,
    // you can call flush().
    db.flush()?;

    Ok(())
}
