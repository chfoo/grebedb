// Example showing how to store data using Serde.
//
// In this example, standalone functions are used. For simple applications,
// this is usually sufficient. In more complex cases, you may choose to write
// a wrapper around Database and Cursor instead.
//
// For details about Serde, please read the documentation at https://serde.rs/

use grebedb::{Database, Options};

fn main() -> anyhow::Result<()> {
    let path = std::path::PathBuf::from("grebedb_example_data/serde/");

    std::fs::create_dir_all(&path)?;

    let options = Options::default();
    let mut db = Database::open_path(path, options)?;

    put_serde(
        &mut db,
        ("document", 1u32),
        MyDocument {
            id: 1,
            name: "hello".to_string(),
            content: "Hello world!".to_string(),
        },
    )?;

    let value = get_serde::<_, MyDocument>(&mut db, ("document", 1u32))?;

    println!("The value of document 1 is {:?}", value);

    db.flush()?;

    Ok(())
}

fn put_serde<K, V>(database: &mut Database, key: K, value: V) -> anyhow::Result<()>
where
    K: serde::Serialize,
    V: serde::Serialize,
{
    let key = strkey::to_vec(&key)?;
    let value = serde_json::to_vec(&value)?;

    database.put(key, value)?;

    Ok(())
}

fn get_serde<K, V>(database: &mut Database, key: K) -> anyhow::Result<Option<V>>
where
    K: serde::Serialize,
    V: serde::de::DeserializeOwned,
{
    let key = strkey::to_vec(&key)?;
    let value = database.get(key)?;

    if let Some(value) = value {
        Ok(Some(serde_json::from_slice(&value)?))
    } else {
        Ok(None)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MyDocument {
    id: u32,
    name: String,
    content: String,
}
