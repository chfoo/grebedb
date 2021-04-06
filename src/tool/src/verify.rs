use std::path::Path;

use grebedb::{Database, OpenMode, Options};

pub fn verify(database_path: &Path, write: bool, verbose: bool) -> anyhow::Result<()> {
    let options = Options {
        open_mode: if write {
            OpenMode::LoadOnly
        } else {
            OpenMode::ReadOnly
        },
        ..Default::default()
    };

    let mut database = Database::open_path(database_path, options)?;

    database.verify(|current, total| {
        if verbose {
            let percent = if total > 0 {
                current as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            eprintln!("\t{:.1}%\t{}\t{}", percent, current, total);
        }
    })?;

    if verbose {
        eprintln!("OK");
    }

    Ok(())
}
