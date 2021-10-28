mod common;

use grebedb::{Database, Error};

fn cursor_sequential(mut database: Database) -> Result<(), Error> {
    for num in 0..10000 {
        let key = format!("{:08x}", num);
        let value = format!("hello world {}", num);

        database.put(key, value)?;
    }

    let cursor = database.cursor()?;
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

fn cursor_iter_manual(mut database: Database) -> Result<(), Error> {
    for num in 0..500 {
        let key = format!("{:08x}", num);
        let value = format!("hello world {}", num);

        database.put(key, value)?;
    }

    let mut cursor = database.cursor()?;
    let mut count = 0;

    while let Some((key, value)) = cursor.next() {
        assert!(!key.is_empty());
        assert!(!value.is_empty());
        assert!(cursor.error().is_none());
        count += 1;
    }

    assert_eq!(count, 500);

    Ok(())
}

fn cursor_next_buf(mut database: Database) -> Result<(), Error> {
    for num in 0..500 {
        let key = format!("{:08x}", num);
        let value = format!("hello world {}", num);

        database.put(key, value)?;
    }

    let mut cursor = database.cursor()?;
    let mut count = 0;
    let mut key = Vec::new();
    let mut value = Vec::new();

    while cursor.next_buf(&mut key, &mut value)? {
        let expected_key = format!("{:08x}", count);
        let expected_value = format!("hello world {}", count);

        assert_eq!(key, expected_key.as_bytes());
        assert_eq!(value, expected_value.as_bytes());

        count += 1;
    }

    assert_eq!(count, 500);

    Ok(())
}

fn cursor_removed_items(mut database: Database) -> Result<(), Error> {
    for num in 0..10000 {
        let key = format!("{:08x}", num);
        let value = format!("hello world {}", num);

        database.put(key, value)?;
    }

    for num in 0..10000 {
        let key = format!("{:08x}", num);
        database.remove(key)?;
    }

    let cursor = database.cursor()?;

    assert_eq!(cursor.count(), 0);

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

    let cursor = database.cursor_range("key:250".."key:650")?;
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

    let cursor = database.cursor_range("key:100"..="key:200")?;
    let keys: Vec<String> = cursor
        .map(|(key, _value)| String::from_utf8(key).unwrap())
        .collect();
    assert_eq!(keys, vec!["key:100".to_string(), "key:200".to_string(),]);

    let cursor = database.cursor_range(.."key:200")?;
    let keys: Vec<String> = cursor
        .map(|(key, _value)| String::from_utf8(key).unwrap())
        .collect();
    assert_eq!(keys, vec!["key:100".to_string(),]);

    let cursor = database.cursor_range("key:750"..)?;
    let keys: Vec<String> = cursor
        .map(|(key, _value)| String::from_utf8(key).unwrap())
        .collect();
    assert_eq!(keys, vec!["key:800".to_string(),]);

    Ok(())
}

matrix_test!(cursor_sequential);
matrix_test!(cursor_iter_manual);
matrix_test!(cursor_next_buf);
matrix_test!(cursor_range);
matrix_test!(cursor_removed_items);
