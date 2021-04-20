use grebedb::{Database, Options};
use indexmap::IndexSet;

#[test]
fn test_metadata() {
    let options = Options::default();
    let mut db = Database::open_memory(options).unwrap();

    let mut keys = IndexSet::new();

    for num in 0..500 {
        keys.insert(num);
        let key = format!("{:08x}", num);
        db.put(key, "hello world!").unwrap();
    }

    assert_eq!(db.metadata().key_value_count(), 500);
}
