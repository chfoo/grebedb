mod common;

use grebedb::{Database, Error};
use indexmap::IndexSet;
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

fn fill_and_random_remove(mut db: Database) -> Result<(), Error> {
    let mut keys = IndexSet::new();

    for num in 0..2000 {
        keys.insert(num);
        let key = format!("{:08x}", num);
        db.put(key, "hello world!")?;
    }

    let mut rng = XorShiftRng::seed_from_u64(1);
    let mut count = 0;

    while !keys.is_empty() {
        let index = rng.gen_range(0..keys.len());
        let key = keys.swap_remove_index(index).unwrap();
        let key = format!("{:08x}", key);

        assert!(db.contains_key(&key)?);

        db.remove(&key)?;

        if count % 100 == 0 {
            db.verify(|_, _| {})?;
        }

        assert!(!db.contains_key(&key)?);

        count += 1;
    }

    Ok(())
}

matrix_test!(fill_and_random_remove);
