use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Stdout, Write},
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

struct Dumper {
    database: Option<Database>,
    counter: u64,
    output_file: Option<BufWriter<File>>,
    output_stdout: Option<BufWriter<Stdout>>,
}

impl Dumper {
    fn open(database_path: &Path, output_path: &Path) -> anyhow::Result<Self> {
        let mut output_file = None;
        let mut output_stdout = None;

        if output_path.as_os_str() != "-" {
            output_file = Some(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(output_path)?,
            ));
        } else {
            output_stdout = Some(BufWriter::new(std::io::stdout()));
        };

        let options = Options {
            open_mode: OpenMode::ReadOnly,
            ..Default::default()
        };
        let database = Database::open_path(database_path, options)?;

        Ok(Self {
            database: Some(database),
            counter: 0,
            output_file,
            output_stdout,
        })
    }

    pub fn dump(&mut self) -> anyhow::Result<()> {
        self.write_header()?;
        self.write_key_values()?;
        self.write_footer()?;

        self.finish_file()?;

        Ok(())
    }

    fn write_row<T>(&mut self, row: T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        if let Some(dest) = &mut self.output_file {
            Self::serialize_row(dest, &row)?;
        }
        if let Some(dest) = &mut self.output_stdout {
            Self::serialize_row(dest, &row)?;
        }

        Ok(())
    }

    fn serialize_row<W, T>(mut dest: W, row: T) -> anyhow::Result<()>
    where
        W: Write,
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

    fn finish_file(&mut self) -> anyhow::Result<()> {
        if let Some(mut dest) = self.output_file.take() {
            dest.flush()?;
            let dest = dest.into_inner()?;
            dest.sync_all()?;
        }
        if let Some(mut dest) = self.output_stdout.take() {
            dest.flush()?;
        }

        Ok(())
    }
}

struct Loader {
    database: Database,
    input_file: BufReader<Box<dyn Read>>,
    header_found: bool,
    footer_found: bool,
}

impl Loader {
    fn open(database_path: &Path, input_path: &Path) -> anyhow::Result<Self> {
        let options = Options {
            open_mode: OpenMode::CreateOnly,
            ..Default::default()
        };
        let database = Database::open_path(database_path, options)?;

        let file: Box<dyn Read> = if input_path.as_os_str() != "-" {
            Box::new(File::open(input_path)?)
        } else {
            Box::new(std::io::stdin())
        };
        let input_file = BufReader::new(file);

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

pub fn dump(database_path: &Path, output_path: &Path) -> anyhow::Result<()> {
    let mut dumper = Dumper::open(database_path, output_path)?;
    dumper.dump()?;

    Ok(())
}

pub fn load(database_path: &Path, input_path: &Path) -> anyhow::Result<()> {
    let mut loader = Loader::open(database_path, input_path)?;
    loader.load()?;

    Ok(())
}
