mod common;

use grebedb::Database;

fn put_one(mut db: Database) -> anyhow::Result<()> {
    db.put("my key", "hello world")?;
    db.flush()?;

    Ok(())
}

fn put_many(mut db: Database) -> anyhow::Result<()> {
    for num in 0..10000 {
        let key = format!("{:08x}", num);
        let value = format!("hello world {}", num);

        db.put(key, value)?;
    }

    db.flush()?;

    Ok(())
}

matrix_test!(put_one);
matrix_test!(put_many);
