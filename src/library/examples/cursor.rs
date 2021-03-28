// Demonstrates the use of a cursor.

use grebedb::{Database, Options};

fn main() -> Result<(), grebedb::Error> {
    let options = Options::default();
    let mut db = Database::open_memory(options)?;

    for number in 0..10 {
        db.put(
            format!("key:{:04x}", number),
            format!("hello world {}!", number),
        )?;
    }

    println!("Printing all the key-values...");

    for (key, value) in db.cursor() {
        println!(
            "Cursor key = {}, value = {}",
            std::str::from_utf8(&key).unwrap(),
            std::str::from_utf8(&value).unwrap()
        );
    }

    println!("Printing all the key-values starting from [key:0004, key:0008) ...");

    let cursor = db.cursor_range(Some("key:0004"), Some("key:0008"))?;

    for (key, value) in cursor {
        println!(
            "Cursor key = {}, value = {}",
            std::str::from_utf8(&key).unwrap(),
            std::str::from_utf8(&value).unwrap()
        );
    }

    Ok(())
}
