// Sample program that inserts UUID (random ordered) key-values infinitely.

use std::time::Duration;

use clap::{App, Arg};
use grebedb::{Database, DatabaseOptions};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use uuid::Uuid;

fn main() -> Result<(), grebedb::Error> {
    let matches = App::new("GrebeDB UUID insert simulator")
        .arg(
            Arg::with_name("delay")
                .long("delay")
                .takes_value(true)
                .default_value("0.5")
                .help("Delay between insert batches in seconds."),
        )
        .get_matches();

    let delay = matches.value_of("delay").unwrap();
    let delay = delay.parse::<f32>().unwrap();

    let path = std::path::PathBuf::from("grebedb_example_data/uuid_simulator/");

    std::fs::create_dir_all(&path)?;

    let options = DatabaseOptions::default();
    let mut db = Database::open_path(path, options)?;

    let mut counter = 0u64;

    loop {
        for _ in 0..100 {
            let id = Uuid::new_v4();
            let key = id.as_bytes();

            let mut rng = XorShiftRng::seed_from_u64(counter);
            let mut buffer = vec![0u8; 1024];
            rng.fill_bytes(&mut buffer);

            db.put(key.to_vec(), buffer)?;

            counter += 1;
        }

        if delay > 0.0 {
            std::thread::sleep(Duration::from_secs_f32(delay))
        }
    }
}
