mod common;

use grebedb::{Database, Error};

fn simple_get_put_remove(mut database: Database) -> Result<(), Error> {
    database.put("key1", "hello")?;
    database.put("key2", "world")?;

    assert!(database.contains_key("key1")?);
    assert!(database.contains_key("key2")?);
    assert!(!database.contains_key("key3")?);

    assert_eq!(database.get("key1")?, Some("hello".into()));
    assert_eq!(database.get("key2")?, Some("world".into()));
    assert_eq!(database.get("key3")?, None);

    database.remove("key1")?;
    database.remove("key2")?;
    database.remove("key3")?;

    assert!(!database.contains_key("key1")?);
    assert!(!database.contains_key("key2")?);
    assert!(!database.contains_key("key3")?);

    Ok(())
}

fn sequential_numbers(mut database: Database) -> Result<(), Error> {
    let mut buffer = Vec::new();

    for num in 0..10000 {
        let key = format!("{:08x}", num);
        let value = format!("hello world {}", num);

        assert!(!database.contains_key(&key)?);
        database.put(key.clone(), value.clone())?;
        assert!(database.contains_key(&key)?);
        database.get_buf(&key, &mut buffer)?;
        assert_eq!(&buffer, value.as_bytes());
    }

    for num in 0..10000 {
        let key = format!("{:08x}", num);

        database.remove(&key)?;
        assert!(!database.contains_key(&key)?);
    }

    Ok(())
}

multiple_vfs_test!(simple_get_put_remove);
multiple_vfs_small_options_test!(simple_get_put_remove);
multiple_vfs_test!(sequential_numbers);
multiple_vfs_small_options_test!(sequential_numbers);
