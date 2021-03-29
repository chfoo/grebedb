mod common;

use std::collections::BTreeMap;

use grebedb::{Database, Error};
use indexmap::IndexSet;
use rand::{distributions::Uniform, prelude::*, SeedableRng};
use rand_xorshift::XorShiftRng;

enum OperationChoice {
    Get,
    Put,
    Remove,
    Flush,
}

enum Operation {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
    Flush,
}

struct OperationGenerator {
    rng: XorShiftRng,
    key_range: Uniform<u64>,
    existing_keys: IndexSet<u64>,
    choices: Vec<OperationChoice>,
}

impl OperationGenerator {
    fn new(max_keys: usize) -> Self {
        let mut choices = Vec::new();

        for _ in 0..200 {
            choices.push(OperationChoice::Get);
        }
        for _ in 0..700 {
            choices.push(OperationChoice::Put);
        }
        for _ in 0..100 {
            choices.push(OperationChoice::Remove);
        }

        choices.push(OperationChoice::Flush);

        Self {
            rng: XorShiftRng::seed_from_u64(1),
            key_range: Uniform::from(0..max_keys as u64),
            existing_keys: IndexSet::new(),
            choices,
        }
    }

    pub fn get(&mut self) -> Operation {
        let operation_seed: u64 = self.rng.gen();
        let mut operation_rng = XorShiftRng::seed_from_u64(operation_seed);
        let choice = self.choices.choose(&mut operation_rng).unwrap();

        match choice {
            OperationChoice::Get => {
                if !self.existing_keys.is_empty() && self.rng.gen_bool(0.6) {
                    let key = self.get_existing_key(&mut operation_rng);
                    let key_vec = Self::derive_key_bytes(key);
                    Operation::Get(key_vec)
                } else {
                    let key = self.get_nonexisting_key(&mut operation_rng);
                    let key_vec = Self::derive_key_bytes(key);
                    Operation::Get(key_vec)
                }
            }
            OperationChoice::Put => {
                if !self.existing_keys.is_empty() && self.rng.gen_bool(0.6) {
                    let key = self.get_existing_key(&mut operation_rng);
                    let key_vec = Self::derive_key_bytes(key);
                    let value_vec = Self::derive_value_bytes(key, operation_seed);
                    Operation::Put(key_vec, value_vec)
                } else {
                    let key = self.generate_new_key(&mut operation_rng);
                    let key_vec = Self::derive_key_bytes(key);
                    let value_vec = Self::derive_value_bytes(key, operation_seed);
                    Operation::Put(key_vec, value_vec)
                }
            }
            OperationChoice::Remove => {
                if !self.existing_keys.is_empty() && self.rng.gen_bool(0.8) {
                    let index = self.get_existing_index(&mut operation_rng);
                    let key = self.existing_keys.swap_remove_index(index).unwrap();
                    let key_vec = Self::derive_key_bytes(key);
                    Operation::Remove(key_vec)
                } else {
                    let key = self.get_nonexisting_key(&mut operation_rng);
                    let key_vec = Self::derive_key_bytes(key);
                    Operation::Remove(key_vec)
                }
            }
            OperationChoice::Flush => Operation::Flush,
        }
    }

    fn get_existing_index(&mut self, rng: &mut impl Rng) -> usize {
        rng.gen_range(0..self.existing_keys.len())
    }

    fn get_existing_key(&mut self, rng: &mut impl Rng) -> u64 {
        let index = self.get_existing_index(rng);
        *self.existing_keys.get_index(index).unwrap()
    }

    fn get_nonexisting_key(&mut self, rng: &mut impl Rng) -> u64 {
        loop {
            let key = self.key_range.sample(rng);

            if !self.existing_keys.contains(&key) {
                return key;
            }
        }
    }

    fn generate_new_key(&mut self, rng: &mut impl Rng) -> u64 {
        let key = self.key_range.sample(rng);
        self.existing_keys.insert(key);
        key
    }

    fn derive_key_bytes(key: u64) -> Vec<u8> {
        format!("my key {:016x}", key).into_bytes()
    }

    fn derive_value_bytes(key: u64, seed: u64) -> Vec<u8> {
        let mut value_rng = XorShiftRng::seed_from_u64(key ^ seed);

        let size = value_rng.gen_range(1..=4096);
        let mut value = vec![0; size];
        value_rng.fill_bytes(&mut value);

        value
    }
}

fn rand_operation(mut database: Database, rounds: usize) -> Result<(), Error> {
    let mut generator = OperationGenerator::new((rounds / 4).max(10));
    let mut std_map = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut value_buffer = Vec::new();

    for _num in 0..rounds {
        match generator.get() {
            Operation::Get(key) => {
                let has_key = database.get_buf(&key, &mut value_buffer)?;

                match std_map.get(&key) {
                    Some(value) => {
                        assert!(has_key);
                        assert_eq!(&value_buffer, value);
                    }
                    None => {
                        assert!(!has_key);
                    }
                }
            }
            Operation::Put(key, value) => {
                database.put(key.clone(), value.clone())?;
                assert!(database.contains_key(&key)?);
                std_map.insert(key, value);
            }
            Operation::Remove(key) => {
                database.remove(key.clone())?;
                assert!(!database.contains_key(&key)?);
                std_map.remove(&key);
            }
            Operation::Flush => database.flush()?,
        }
    }

    println!(
        "current len={}, expected len={}",
        database.cursor().count(),
        std_map.len()
    );

    let mut cursor = database.cursor();
    let mut std_iter = std_map.iter();

    loop {
        let current = cursor.next();
        let expected = std_iter.next();

        if current.is_none() && expected.is_none() {
            break;
        } else {
            let (key, value) = current.unwrap();
            let (expected_key, expected_value) = expected.unwrap();

            assert_eq!(&key, expected_key);
            assert_eq!(&value, expected_value);
        }
    }

    Ok(())
}

fn rand_operation_10000(database: Database) -> Result<(), Error> {
    rand_operation(database, 10000)
}

fn rand_operation_100000(database: Database) -> Result<(), Error> {
    rand_operation(database, 100000)
}

#[cfg(debug_assertions)]
mod a {
    use grebedb::Options;

    use super::*;

    #[test]
    fn rand_operation_10000_fast() {
        let database = Database::open_memory(Options::default()).unwrap();
        rand_operation(database, 10000).unwrap();
    }

    matrix_test_ignore!(rand_operation_10000);
    matrix_test_ignore!(rand_operation_100000);
}
#[cfg(not(debug_assertions))]
mod a {
    use super::*;

    matrix_test!(rand_operation_10000);
    matrix_test_ignore!(rand_operation_100000);
}
