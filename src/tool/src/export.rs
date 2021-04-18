use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

use grebedb::{Database, OpenMode, Options};
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

const RECORD_SEPARATOR: u8 = 0x1e;
const NEWLINE: u8 = 0x0a;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Row {
    Metadata(MetadataRow),
    KeyValue(KeyValueRow),
    Eof,
}

#[derive(Default, Serialize, Deserialize)]
struct MetadataRow {
    pub key_value_count: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct KeyValueRow {
    #[serde(serialize_with = "vec_to_hex")]
    #[serde(deserialize_with = "hex_to_vec")]
    pub key: Vec<u8>,

    #[serde(serialize_with = "vec_to_hex")]
    #[serde(deserialize_with = "hex_to_vec")]
    pub value: Vec<u8>,

    pub index: u64,

    pub key_crc32c: u32,
    pub value_crc32c: u32,
}

fn vec_to_hex<S>(vec: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&data_encoding::HEXUPPER.encode(&vec))
}

fn hex_to_vec<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = <&str>::deserialize(deserializer)?;
    match data_encoding::HEXUPPER.decode(s.as_bytes()) {
        Ok(value) => Ok(value),
        Err(error) => Err(Error::custom(format!("{:?}", error))),
    }
}

struct Dumper<W: Write> {
    database: Option<Database>,
    counter: u64,
    output_file: W,
}

impl<W: Write> Dumper<W> {
    fn open(database_path: &Path, output_file: W) -> anyhow::Result<Self> {
        let options = Options {
            open_mode: OpenMode::ReadOnly,
            ..Default::default()
        };
        let database = Database::open_path(database_path, options)?;

        Ok(Self {
            database: Some(database),
            counter: 0,
            output_file,
        })
    }

    pub fn dump(&mut self) -> anyhow::Result<()> {
        self.write_header()?;
        self.write_key_values()?;
        self.write_footer()?;

        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.output_file
    }

    fn write_row<T>(&mut self, row: T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        Self::serialize_row(&mut self.output_file, &row)?;

        Ok(())
    }

    fn serialize_row<T>(mut dest: &mut W, row: T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        dest.write_all(&[RECORD_SEPARATOR])?;
        let mut serializer = serde_json::Serializer::new(dest);
        row.serialize(&mut serializer)?;
        dest = serializer.into_inner();
        dest.write_all(&[NEWLINE])?;

        Ok(())
    }

    fn write_header(&mut self) -> anyhow::Result<()> {
        let database = self.database.take().unwrap();
        let header_row = MetadataRow {
            key_value_count: database.metadata().key_value_count(),
        };

        self.write_row(Row::Metadata(header_row))?;

        self.database = Some(database);

        Ok(())
    }
    fn write_footer(&mut self) -> anyhow::Result<()> {
        self.write_row(Row::Eof)
    }

    fn write_key_values(&mut self) -> anyhow::Result<()> {
        let mut database = self.database.take().unwrap();
        let mut cursor = database.cursor()?;

        loop {
            let mut row = KeyValueRow::default();
            let has_item = cursor.next_buf(&mut row.key, &mut row.value)?;

            if !has_item {
                break;
            }

            row.index = self.counter;
            row.key_crc32c = crc32c::crc32c(&row.key);
            row.value_crc32c = crc32c::crc32c(&row.value);
            self.counter += 1;

            self.write_row(Row::KeyValue(row))?;
        }

        Ok(())
    }
}

struct Loader<R: BufRead> {
    database: Database,
    input_file: R,
    header_found: bool,
    footer_found: bool,
}

impl<R: BufRead> Loader<R> {
    fn open(database_path: &Path, input_file: R) -> anyhow::Result<Self> {
        let options = Options {
            open_mode: OpenMode::CreateOnly,
            ..Default::default()
        };
        let database = Database::open_path(database_path, options)?;

        Ok(Self {
            database,
            input_file,
            header_found: false,
            footer_found: false,
        })
    }

    pub fn load(&mut self) -> anyhow::Result<()> {
        let mut buffer = Vec::new();

        while self.read_record_separator()? {
            buffer.clear();
            self.input_file.read_until(NEWLINE, &mut buffer)?;

            if buffer.last().cloned().unwrap_or(0) != NEWLINE {
                return Err(anyhow::anyhow!("unexpected file end"));
            }

            let row: Row = serde_json::from_slice(&buffer)?;

            match row {
                Row::Metadata(row) => {
                    self.process_metadata(&row)?;
                }
                Row::KeyValue(row) => {
                    self.process_key_value_row(row)?;
                }
                Row::Eof => {
                    self.process_eof_row()?;
                }
            }
        }

        self.database.flush()?;
        self.validate_footer()?;

        Ok(())
    }

    fn read_record_separator(&mut self) -> anyhow::Result<bool> {
        let mut record_flag = [0u8; 1];

        if let Err(error) = self.input_file.read_exact(&mut record_flag) {
            if let std::io::ErrorKind::UnexpectedEof = error.kind() {
                return Ok(false);
            } else {
                return Err(anyhow::Error::new(error));
            }
        }

        if record_flag[0] != RECORD_SEPARATOR {
            Err(anyhow::anyhow!("missing record separator"))
        } else {
            Ok(true)
        }
    }

    fn process_metadata(&mut self, _row: &MetadataRow) -> anyhow::Result<()> {
        if self.header_found {
            return Err(anyhow::anyhow!("duplicate header"));
        }

        self.header_found = true;

        Ok(())
    }

    fn process_key_value_row(&mut self, row: KeyValueRow) -> anyhow::Result<()> {
        if !self.header_found {
            return Err(anyhow::anyhow!("header not found"));
        }

        let key_crc = crc32c::crc32c(&row.key);

        if key_crc != row.key_crc32c {
            return Err(anyhow::anyhow!("bad checksum, key, row = {}", row.index));
        }

        let value_crc = crc32c::crc32c(&row.value);

        if value_crc != row.value_crc32c {
            return Err(anyhow::anyhow!("bad checksum, value, row = {}", row.index));
        }

        self.database.put(row.key, row.value)?;

        Ok(())
    }

    fn process_eof_row(&mut self) -> anyhow::Result<()> {
        if self.footer_found {
            return Err(anyhow::anyhow!("duplicate footer"));
        }

        self.footer_found = true;

        Ok(())
    }

    fn validate_footer(&self) -> anyhow::Result<()> {
        if !self.footer_found {
            Err(anyhow::anyhow!("footer not found"))
        } else {
            Ok(())
        }
    }
}

pub fn dump(
    database_path: &Path,
    output_path: &Path,
    compression: Option<i32>,
) -> anyhow::Result<()> {
    // TODO: this needs refactoring
    if output_path.as_os_str() != "-" {
        if let Some(compression) = compression {
            #[cfg(feature = "zstd")]
            {
                let file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(output_path)?;
                let file = zstd::Encoder::new(file, compression)?;
                let mut dumper = Dumper::open(database_path, file)?;
                dumper.dump()?;

                let file = dumper.into_inner();
                let mut file = file.finish()?;
                file.flush()?;
                file.sync_all()?;
            }
            #[cfg(not(feature = "zstd"))]
            {
                let _ = compression;
                return Err(anyhow::anyhow!("Compression feature not enabled"));
            }
        } else {
            let file = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(output_path)?,
            );

            let mut dumper = Dumper::open(database_path, file)?;
            dumper.dump()?;

            let mut file = dumper.into_inner();
            file.flush()?;
            let file = file.into_inner()?;
            file.sync_all()?;
        }
    } else if let Some(compression) = compression {
        #[cfg(feature = "zstd")]
        {
            let file = BufWriter::new(std::io::stdout());
            let file = zstd::Encoder::new(file, compression)?;
            let mut dumper = Dumper::open(database_path, file)?;
            dumper.dump()?;

            let file = dumper.into_inner();
            let mut file = file.finish()?;
            file.flush()?;
        }
        #[cfg(not(feature = "zstd"))]
        {
            let _ = compression;
            return Err(anyhow::anyhow!("Compression feature not enabled"));
        }
    } else {
        let file = BufWriter::new(std::io::stdout());

        let mut dumper = Dumper::open(database_path, file)?;
        dumper.dump()?;

        let mut file = dumper.into_inner();
        file.flush()?;
    }

    Ok(())
}

pub fn load(database_path: &Path, input_path: &Path, compression: bool) -> anyhow::Result<()> {
    let file: BufReader<Box<dyn Read>> = if input_path.as_os_str() != "-" {
        BufReader::new(Box::new(File::open(input_path)?))
    } else {
        BufReader::new(Box::new(std::io::stdin()))
    };

    if compression {
        #[cfg(feature = "zstd")]
        {
            let file = BufReader::new(zstd::Decoder::new(file)?);
            let mut loader = Loader::open(database_path, file)?;
            loader.load()?;
        }
        #[cfg(not(feature = "zstd"))]
        {
            return Err(anyhow::anyhow!("Compression feature not enabled"));
        }
    } else {
        let mut loader = Loader::open(database_path, file)?;
        loader.load()?;
    }

    Ok(())
}
