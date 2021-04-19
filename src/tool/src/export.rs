use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use grebedb::{Database, OpenMode, Options};

pub fn dump(
    database_path: &Path,
    output_path: &Path,
    compression: Option<i32>,
) -> anyhow::Result<()> {
    let options = Options {
        open_mode: OpenMode::ReadOnly,
        ..Default::default()
    };
    let mut database = Database::open_path(database_path, options)?;

    // TODO: this needs refactoring
    if output_path.as_os_str() != "-" {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_path)?;

        if let Some(compression) = compression {
            #[cfg(feature = "zstd")]
            {
                let mut file = zstd::Encoder::new(&mut file, compression)?;
                grebedb::export::export(&mut database, &mut file, |_| {})?;
                file.finish()?;
            }
            #[cfg(not(feature = "zstd"))]
            {
                let _ = compression;
                return Err(anyhow::anyhow!("Compression feature not enabled"));
            }
        } else {
            grebedb::export::export(&mut database, &mut file, |_| {})?;
        }

        file.flush()?;
        file.sync_all()?;
    } else {
        let mut file = BufWriter::new(std::io::stdout());

        if let Some(compression) = compression {
            #[cfg(feature = "zstd")]
            {
                let mut file = zstd::Encoder::new(&mut file, compression)?;
                grebedb::export::export(&mut database, &mut file, |_| {})?;
                file.finish()?;
            }
            #[cfg(not(feature = "zstd"))]
            {
                let _ = compression;
                return Err(anyhow::anyhow!("Compression feature not enabled"));
            }
        } else {
            grebedb::export::export(&mut database, &mut file, |_| {})?;
        }
        file.flush()?;
    }

    Ok(())
}

pub fn load(database_path: &Path, input_path: &Path, compression: bool) -> anyhow::Result<()> {
    let options = Options {
        open_mode: OpenMode::CreateOnly,
        ..Default::default()
    };
    let mut database = Database::open_path(database_path, options)?;

    let mut file: BufReader<Box<dyn Read>> = if input_path.as_os_str() != "-" {
        BufReader::new(Box::new(File::open(input_path)?))
    } else {
        BufReader::new(Box::new(std::io::stdin()))
    };

    if compression {
        #[cfg(feature = "zstd")]
        {
            let mut file = BufReader::new(zstd::Decoder::new(file)?);
            grebedb::export::import(&mut database, &mut file, |_| {})?
        }
        #[cfg(not(feature = "zstd"))]
        {
            return Err(anyhow::anyhow!("Compression feature not enabled"));
        }
    } else {
        grebedb::export::import(&mut database, &mut file, |_| {})?
    }

    database.flush()?;

    Ok(())
}
