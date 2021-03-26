use grebedb::{Database, DatabaseOptions};

#[test]
fn test_debug() {
    let mut database = Database::open_memory(DatabaseOptions::default()).unwrap();

    println!("{:?}", &database);
    println!("{:?}", database.cursor());
}
