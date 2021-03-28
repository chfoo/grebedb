use grebedb::{Database, Options};
use indexmap::IndexSet;
use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;

#[test]
fn test_fill_and_random_remove() {
    let options = Options::default();
    let mut db = Database::open_memory(options).unwrap();

    let mut keys = IndexSet::new();

    for num in 0..2000 {
        keys.insert(num);
        let key = format!("{:08x}", num);
        db.put(key, "hello world!").unwrap();
    }

    let mut rng = XorShiftRng::seed_from_u64(1);

    while !keys.is_empty() {
        let index = rng.gen_range(0..keys.len());
        let key = keys.swap_remove_index(index).unwrap();
        let key = format!("{:08x}", key);

        assert!(db.contains_key(&key).unwrap());

        db.remove(&key).unwrap();

        assert!(!db.contains_key(&key).unwrap());
    }
}
