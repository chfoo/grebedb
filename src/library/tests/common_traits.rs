use grebedb::{Database, Options};

#[test]
fn test_debug() {
    let mut database = Database::open_memory(Options::default()).unwrap();

    println!("{:?}", &database);
    println!("{:?}", database.cursor());
}
