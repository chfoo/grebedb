// Sample program that inserts key-values infinitely.

use std::time::{Duration, SystemTime};

use grebedb::{Database, DatabaseOptions};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;

fn main() -> Result<(), grebedb::Error> {
    let path = std::path::PathBuf::from("grebedb_example_data/insert_simulator/");

    std::fs::create_dir_all(&path)?;

    let options = DatabaseOptions::default();
    let mut db = Database::open_path(path, options)?;

    let mut counter = 0u64;

    loop {
        for _ in 0..100 {
            let duration = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            let ms = duration.as_micros() as u64;
            let key = format!("{:016x}{:016x}", ms, counter);

            let mut rng = XorShiftRng::seed_from_u64(counter);
            let mut buffer = vec![0u8; 1024];
            rng.fill_bytes(&mut buffer);

            db.put(key, buffer)?;

            counter += 1;
        }

        std::thread::sleep(Duration::from_secs_f32(0.5))
    }
}
