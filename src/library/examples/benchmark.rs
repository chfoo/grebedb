// Test database operation speeds

use std::time::{Duration, Instant};

use clap::{App, Arg};
use grebedb::{CompressionLevel, Database, Error, Options, SyncOption};
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use statrs::statistics::{Data, Median};

fn main() -> Result<(), Error> {
    let args = App::new("GrebeDB benchmark")
        .arg(
            Arg::with_name("sync")
                .long("sync")
                .possible_values(&["none", "data", "all"])
                .default_value("data"),
        )
        .arg(
            Arg::with_name("store")
                .long("store")
                .possible_values(&["disk", "memory"])
                .default_value("disk"),
        )
        .arg(
            Arg::with_name("page_cache_size")
                .long("page-cache-size")
                .default_value("64"),
        )
        .arg(
            Arg::with_name("keys_per_node")
                .long("keys-per-node")
                .default_value("1024"),
        )
        .arg(
            Arg::with_name("compression_level")
                .long("compression-level")
                .possible_values(&["none", "verylow", "low", "medium", "high"])
                .default_value("low"),
        )
        .arg(
            Arg::with_name("max_batch_size")
                .long("max-batch-size")
                .default_value("2000"),
        )
        .arg(
            Arg::with_name("batches")
                .long("batches")
                .default_value("100"),
        )
        .arg(Arg::with_name("rounds").long("rounds").default_value("3"))
        .get_matches();

    let options = Options {
        automatic_flush: false,
        file_sync: match args.value_of("sync").unwrap() {
            "none" => SyncOption::None,
            "data" => SyncOption::Data,
            "all" => SyncOption::All,
            _ => unreachable!(),
        },
        page_cache_size: args.value_of("page_cache_size").unwrap().parse().unwrap(),
        keys_per_node: args.value_of("keys_per_node").unwrap().parse().unwrap(),
        compression_level: match args.value_of("compression_level").unwrap() {
            "none" => CompressionLevel::None,
            "verylow" => CompressionLevel::VeryLow,
            "low" => CompressionLevel::Low,
            "medium" => CompressionLevel::Medium,
            "high" => CompressionLevel::High,
            _ => unreachable!(),
        },
        ..Default::default()
    };

    let (mut db_sequential, mut db_random) = if args.value_of("store").unwrap() == "disk" {
        let path_sequential =
            std::path::PathBuf::from("grebedb_example_data/benchmark/db_sequential");
        let path_random = std::path::PathBuf::from("grebedb_example_data/benchmark/db_random");

        std::fs::create_dir_all(&path_sequential)?;
        std::fs::create_dir_all(&path_random)?;

        (
            Database::open_path(path_sequential, options.clone())?,
            Database::open_path(path_random, options)?,
        )
    } else {
        (
            Database::open_memory(options.clone())?,
            Database::open_memory(options)?,
        )
    };

    let max_batch_size: usize = args.value_of("max_batch_size").unwrap().parse().unwrap();
    let batches: usize = args.value_of("batches").unwrap().parse().unwrap();
    let rounds: usize = args.value_of("rounds").unwrap().parse().unwrap();
    let mut id = 0u64;
    let mut seed = 1u64;

    for batch_id in 0usize..batches {
        let batch_size = ((batch_id + 1).pow(4)).min(max_batch_size);
        let mut stats_sequential = Stats::default();

        for _ in 0..rounds {
            insert_sequential(&mut db_sequential, batch_size, id, &mut stats_sequential)?;
            id += batch_size as u64;
        }

        let mut stats_random = Stats::default();

        for _ in 0..rounds {
            insert_random(&mut db_random, batch_size, seed, &mut stats_random)?;
            seed ^= mix(seed);
        }

        println!(
            "Batch size {}, rounds {}, sequential total {}, random total {}",
            batch_size,
            rounds,
            db_sequential.metadata().key_value_count(),
            db_random.metadata().key_value_count(),
        );
        println!("  Sequential");

        let median = stats_sequential.insert_median();
        let rate = 1.0 / median * batch_size as f64;
        println!("    insert\t{:.6} s/batch\t{:.2} pairs/s", median, rate);

        let median = stats_sequential.flush_median();
        let rate = 1.0 / median * batch_size as f64;
        println!("    flush\t{:.6} s/batch\t{:.2} pairs/s", median, rate);

        let median = stats_sequential.read_median();
        let rate = 1.0 / median * batch_size as f64;
        println!("    read\t{:.6} s/batch\t{:.2} pairs/s", median, rate);

        println!("  Random");

        let median = stats_random.insert_median();
        let rate = 1.0 / median * batch_size as f64;
        println!("    insert\t{:.6} s/batch\t{:.2} pairs/s", median, rate);

        let median = stats_random.flush_median();
        let rate = 1.0 / median * batch_size as f64;
        println!("    flush\t{:.6} s/batch\t{:.2} pairs/s", median, rate);

        let median = stats_random.read_median();
        let rate = 1.0 / median * batch_size as f64;
        println!("    read\t{:.6} s/batch\t{:.2} pairs/s", median, rate);

        std::thread::sleep(Duration::from_secs_f32(0.1));
    }

    Ok(())
}

#[derive(Default)]
struct Stats {
    insert: Vec<Duration>,
    read: Vec<Duration>,
    flush: Vec<Duration>,
}

impl Stats {
    fn insert_median(&mut self) -> f64 {
        self.insert.sort_unstable();
        let times: Vec<f64> = self.insert.iter().map(|item| item.as_secs_f64()).collect();
        let times = Data::new(times);
        times.median()
    }

    fn read_median(&mut self) -> f64 {
        self.read.sort_unstable();
        let times: Vec<f64> = self.read.iter().map(|item| item.as_secs_f64()).collect();
        let times = Data::new(times);
        times.median()
    }

    fn flush_median(&mut self) -> f64 {
        self.flush.sort_unstable();
        let times: Vec<f64> = self.flush.iter().map(|item| item.as_secs_f64()).collect();
        let times = Data::new(times);
        times.median()
    }
}

fn insert_sequential(
    db: &mut Database,
    batch_size: usize,
    id_offset: u64,
    stats: &mut Stats,
) -> Result<(), Error> {
    let time_start = Instant::now();

    for id in 0..batch_size {
        let id = id as u64 + id_offset;
        let mut rng = XorShiftRng::seed_from_u64(id);

        let key = format!("{:016x}", id);
        let mut value = vec![0u8; 1024];
        rng.fill_bytes(&mut value);

        db.put(key, value)?;
    }

    stats.insert.push(time_start.elapsed());

    let time_start = Instant::now();

    db.flush()?;

    stats.flush.push(time_start.elapsed());

    let time_start = Instant::now();

    for id in 0..batch_size {
        let id = id as u64 + id_offset;

        let key = format!("{:016x}", id);

        db.get(key)?;
    }

    stats.read.push(time_start.elapsed());

    Ok(())
}

fn insert_random(
    db: &mut Database,
    batch_size: usize,
    seed: u64,
    stats: &mut Stats,
) -> Result<(), Error> {
    let time_start = Instant::now();

    for id in 0..batch_size {
        let id = id as u64;
        let mut rng = XorShiftRng::seed_from_u64(mix(id) ^ seed);

        let key = format!("{:016x}", rng.gen::<u64>());
        let mut value = vec![0u8; 1024];
        rng.fill_bytes(&mut value);

        db.put(key, value)?;
    }

    stats.insert.push(time_start.elapsed());

    let time_start = Instant::now();

    db.flush()?;

    stats.flush.push(time_start.elapsed());

    let time_start = Instant::now();

    for id in 0..batch_size {
        let id = id as u64;
        let mut rng = XorShiftRng::seed_from_u64(mix(id) ^ seed);

        let key = format!("{:016x}", rng.gen::<u64>());

        db.get(key)?;
    }

    stats.read.push(time_start.elapsed());

    Ok(())
}

fn mix(mut value: u64) -> u64 {
    value ^= 0xc001cafe;
    value <<= 2;
    value *= 0xc001cafe;
    value += 1;
    value
}
