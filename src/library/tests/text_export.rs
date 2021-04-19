use std::io::BufReader;

use grebedb::{Database, Options};

#[test]
fn test_export() {
    let mut database = Database::open_memory(Options::default()).unwrap();

    database.put("key1", "value1").unwrap();
    database.put("key2", "value2").unwrap();
    database.put("key3", "value3").unwrap();

    let mut file = Vec::new();

    grebedb::export::export(&mut database, &mut file, |_| {}).unwrap();

    let mut database = Database::open_memory(Options::default()).unwrap();

    grebedb::export::import(
        &mut database,
        &mut BufReader::new(std::io::Cursor::new(file)),
        |_| {},
    )
    .unwrap();

    assert_eq!(database.get("key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(database.get("key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(database.get("key3").unwrap(), Some(b"value3".to_vec()));
}
