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

fn cursor_sequential(mut database: Database) -> Result<(), Error> {
    for num in 0..10000 {
        let key = format!("{:08x}", num);
        let value = format!("hello world {}", num);

        database.put(key.clone(), value.clone())?;
    }

    let cursor = database.cursor();
    let values: Vec<(Vec<u8>, Vec<u8>)> = cursor.collect();

    assert_eq!(values.len(), 10000);

    for (num, (key, value)) in values.iter().enumerate() {
        let expected_key = format!("{:08x}", num);
        let expected_value = format!("hello world {}", num);

        assert_eq!(key, expected_key.as_bytes());
        assert_eq!(value, expected_value.as_bytes());
    }

    Ok(())
}

fn cursor_range(mut database: Database) -> Result<(), Error> {
    database.put("key:100", "hello world 100")?;
    database.put("key:200", "hello world 200")?;
    database.put("key:300", "hello world 300")?;
    database.put("key:400", "hello world 400")?;
    database.put("key:500", "hello world 500")?;
    database.put("key:600", "hello world 600")?;
    database.put("key:700", "hello world 700")?;
    database.put("key:800", "hello world 800")?;

    let cursor = database.cursor_range(Some("key:250"), Some("key:650"))?;
    let keys: Vec<String> = cursor
        .map(|(key, _value)| String::from_utf8(key).unwrap())
        .collect();

    assert_eq!(
        keys,
        vec![
            "key:300".to_string(),
            "key:400".to_string(),
            "key:500".to_string(),
            "key:600".to_string()
        ]
    );

    Ok(())
}

multiple_vfs_test!(simple_get_put_remove);
multiple_vfs_small_options_test!(simple_get_put_remove);
multiple_vfs_test!(sequential_numbers);
multiple_vfs_small_options_test!(sequential_numbers);
multiple_vfs_test!(cursor_sequential);
multiple_vfs_small_options_test!(cursor_sequential);
multiple_vfs_test!(cursor_range);
multiple_vfs_small_options_test!(cursor_range);
