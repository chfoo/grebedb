use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use grebedb::vfs::{MemoryVfs, Vfs};
use tempfile::TempDir;

#[allow(dead_code)]
pub fn make_tempdir() -> TempDir {
    tempfile::Builder::new()
        .prefix("grebetest_")
        .tempdir()
        .unwrap()
}

#[macro_export]
macro_rules! multiple_vfs_test {
    ($fn_name:ident) => {
        paste::paste! {
            #[test]
            fn [<test_memory_ $fn_name>]() {
                let db = grebedb::Database::open_memory(grebedb::DatabaseOptions::default()).unwrap();
                $fn_name(db).unwrap();
            }
        }

        paste::paste! {
            #[test]
            fn [<test_disk_ $fn_name>]() {
                let temp_dir = $crate::common::make_tempdir();
                let db = grebedb::Database::open_path(temp_dir.path(), grebedb::DatabaseOptions::default()).unwrap();
                $fn_name(db).unwrap();
            }
        }
    };
}

#[macro_export]
macro_rules! multiple_vfs_test_ignore {
    ($fn_name:ident) => {
        paste::paste! {
            #[test]
            #[ignore]
            fn [<test_memory_ $fn_name>]() {
                let db = grebedb::Database::open_memory(grebedb::DatabaseOptions::default()).unwrap();
                $fn_name(db).unwrap();
            }
        }

        paste::paste! {
            #[test]
            #[ignore]
            fn [<test_disk_ $fn_name>]() {
                let temp_dir = $crate::common::make_tempdir();
                let db = grebedb::Database::open_path(temp_dir.path(), grebedb::DatabaseOptions::default()).unwrap();
                $fn_name(db).unwrap();
            }
        }
    };
}

#[macro_export]
macro_rules! multiple_vfs_small_options_test {
    ($fn_name:ident) => {
        paste::paste! {
            #[test]
            fn [<test_memory_small_options_ $fn_name>]() {
                let options = grebedb::DatabaseOptions {
                    keys_per_node: 128,
                    page_cache_size: 4,
                    ..Default::default()
                };
                let db = grebedb::Database::open_memory(options).unwrap();
                $fn_name(db).unwrap();
            }
        }

        paste::paste! {
            #[test]
            fn [<test_disk_small_options_ $fn_name>]() {
                let temp_dir = $crate::common::make_tempdir();
                let options = grebedb::DatabaseOptions {
                    keys_per_node: 128,
                    page_cache_size: 4,
                    ..Default::default()
                };
                let db = grebedb::Database::open_path(temp_dir.path(), options).unwrap();
                $fn_name(db).unwrap();
            }
        }
    };
}

#[derive(Clone)]
pub struct CrashingVfs {
    inner: MemoryVfs,
    counter: Arc<AtomicUsize>,
    threshold: Arc<AtomicUsize>,
}

impl CrashingVfs {
    #[allow(dead_code)]
    pub fn new(threshold: usize) -> Self {
        Self {
            inner: MemoryVfs::default(),
            counter: Arc::new(AtomicUsize::new(0)),
            threshold: Arc::new(AtomicUsize::new(threshold)),
        }
    }

    #[allow(dead_code)]
    pub fn set_threshold(&self, value: usize) {
        self.threshold.store(value, Ordering::Relaxed);
    }

    fn maybe_crash(&self) -> Result<(), grebedb::Error> {
        if self.counter.load(Ordering::Relaxed) >= self.threshold.load(Ordering::Relaxed) {
            Err(grebedb::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "crash",
            )))
        } else {
            Ok(())
        }
    }
}

impl Vfs for CrashingVfs {
    fn lock(&mut self, path: &str) -> Result<(), grebedb::Error> {
        self.inner.lock(path)
    }

    fn unlock(&mut self, path: &str) -> Result<(), grebedb::Error> {
        self.inner.unlock(path)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>, grebedb::Error> {
        eprintln!("read {}", path);
        self.maybe_crash()?;
        self.inner.read(path)
    }

    fn write(&mut self, path: &str, data: &[u8]) -> Result<(), grebedb::Error> {
        eprintln!("write {}", path);
        self.inner.write(path, data)
    }

    fn write_and_sync_all(&mut self, path: &str, data: &[u8]) -> Result<(), grebedb::Error> {
        eprintln!("write_and_sync_all {}", path);
        self.inner.write_and_sync_all(path, data)
    }

    fn remove_file(&mut self, path: &str) -> Result<(), grebedb::Error> {
        eprintln!("remove_file {}", path);
        self.inner.remove_file(path)
    }

    fn read_dir(&self, path: &str) -> Result<Vec<String>, grebedb::Error> {
        eprintln!("read_dir {}", path);
        self.inner.read_dir(path)
    }

    fn create_dir(&mut self, path: &str) -> Result<(), grebedb::Error> {
        eprintln!("create_dir {}", path);
        self.maybe_crash()?;
        self.inner.create_dir(path)
    }

    fn remove_dir(&mut self, path: &str) -> Result<(), grebedb::Error> {
        eprintln!("remove_dir {}", path);
        self.maybe_crash()?;
        self.inner.remove_dir(path)
    }

    fn rename_file(&mut self, old_path: &str, new_path: &str) -> Result<(), grebedb::Error> {
        eprintln!("rename_file {} {}", old_path, new_path);
        self.maybe_crash()?;

        if new_path == "grebedb_meta.grebedb" {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }

        self.inner.rename_file(old_path, new_path)
    }

    fn is_dir(&self, path: &str) -> Result<bool, grebedb::Error> {
        eprintln!("is_dir {}", path);
        self.inner.is_dir(path)
    }

    fn exists(&self, path: &str) -> Result<bool, grebedb::Error> {
        eprintln!("exists {}", path);
        self.inner.exists(path)
    }
}
