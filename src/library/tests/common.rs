use tempfile::TempDir;

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
