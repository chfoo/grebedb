//! Export and import database key-value pairs.
//!
//! The functions allow saving database contents into another file
//! which can be used for migrating data or for backup purposes.
//!
//! The export file format is a JSON text sequence (RFC 7464).

const RECORD_SEPARATOR: u8 = 0x1e;
const NEWLINE: u8 = 0x0a;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Row {
    Metadata(MetadataRow),
    KeyValue(KeyValueRow),
    Eof,
}

use std::io::{BufRead, Write};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Database, Error};

/// Import and export errors.
#[derive(thiserror::Error, Debug)]
pub enum ExportError {
    /// Missing record separator.
    ///
    /// File is not JSON text sequence formatted.
    #[error("missing record separator")]
    MissingRecordSeparator,

    /// Duplicate header.
    ///
    /// File unexpectedly contains another file.
    #[error("duplicate header")]
    DuplicateHeader,

    /// Header not found
    ///
    /// Beginning of the file is missing.
    #[error("header not found")]
    HeaderNotFound,

    /// Bad checksum.
    ///
    /// Data is corrupted.
    #[error("bad checksum, {column}, row = {row}")]
    BadChecksum {
        /// Located at key or value
        column: &'static str,
        /// Row index (0 based)
        row: u64,
    },

    /// Duplicate footer.
    ///
    /// File unexpectedly contains another file.
    #[error("duplicate footer")]
    DuplicateFooter,

    /// Footer not found.
    ///
    /// The file is incomplete.
    #[error("footer not found")]
    FooterNotFound,

    /// Unexpected end of file.
    ///
    /// The file is incomplete.
    #[error("unexpected end of file")]
    UnexpectedEof,
}

impl From<ExportError> for Error {
    fn from(error: ExportError) -> Self {
        Self::Other(Box::new(error))
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Other(Box::new(error))
    }
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
        Err(error) => Err(serde::de::Error::custom(format!("{:?}", error))),
    }
}

struct ImportReader<'a, R: BufRead> {
    database: &'a mut Database,
    input_file: &'a mut R,
    header_found: bool,
    footer_found: bool,
}

impl<'a, R: BufRead> ImportReader<'a, R> {
    fn new(input_file: &'a mut R, database: &'a mut Database) -> Self {
        Self {
            database,
            input_file,
            header_found: false,
            footer_found: false,
        }
    }

    fn import<C>(&mut self, mut progress: C) -> Result<(), Error>
    where
        C: FnMut(u64),
    {
        let mut buffer = Vec::new();
        let mut counter = 0u64;

        while self.read_record_separator()? {
            buffer.clear();
            self.input_file.read_until(NEWLINE, &mut buffer)?;

            if buffer.last().cloned().unwrap_or(0) != NEWLINE {
                return Err(ExportError::UnexpectedEof.into());
            }

            let row: Row = serde_json::from_slice(&buffer)?;

            match row {
                Row::Metadata(row) => {
                    self.process_metadata(&row)?;
                }
                Row::KeyValue(row) => {
                    self.process_key_value_row(row)?;
                    counter += 1;
                    progress(counter);
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

    fn read_record_separator(&mut self) -> Result<bool, Error> {
        let mut record_flag = [0u8; 1];

        if let Err(error) = self.input_file.read_exact(&mut record_flag) {
            if let std::io::ErrorKind::UnexpectedEof = error.kind() {
                return Ok(false);
            } else {
                return Err(error.into());
            }
        }

        if record_flag[0] != RECORD_SEPARATOR {
            Err(ExportError::MissingRecordSeparator.into())
        } else {
            Ok(true)
        }
    }

    fn process_metadata(&mut self, _row: &MetadataRow) -> Result<(), Error> {
        if self.header_found {
            return Err(ExportError::DuplicateHeader.into());
        }

        self.header_found = true;

        Ok(())
    }

    fn process_key_value_row(&mut self, row: KeyValueRow) -> Result<(), Error> {
        if !self.header_found {
            return Err(ExportError::HeaderNotFound.into());
        }

        let key_crc = crc32c::crc32c(&row.key);

        if key_crc != row.key_crc32c {
            return Err(ExportError::BadChecksum {
                column: "key",
                row: row.index,
            }
            .into());
        }

        let value_crc = crc32c::crc32c(&row.value);

        if value_crc != row.value_crc32c {
            return Err(ExportError::BadChecksum {
                column: "value",
                row: row.index,
            }
            .into());
        }

        self.database.put(row.key, row.value)?;

        Ok(())
    }

    fn process_eof_row(&mut self) -> Result<(), Error> {
        if self.footer_found {
            return Err(ExportError::DuplicateFooter.into());
        }

        self.footer_found = true;

        Ok(())
    }

    fn validate_footer(&self) -> Result<(), Error> {
        if !self.footer_found {
            Err(ExportError::FooterNotFound.into())
        } else {
            Ok(())
        }
    }
}

struct ExportWriter<'a, W: Write> {
    database: Option<&'a mut Database>,
    counter: u64,
    output_file: &'a mut W,
}

impl<'a, W: Write> ExportWriter<'a, W> {
    fn new(output_file: &'a mut W, database: &'a mut Database) -> Self {
        Self {
            database: Some(database),
            counter: 0,
            output_file,
        }
    }

    fn export<C>(&mut self, mut progress: C) -> Result<(), Error>
    where
        C: FnMut(u64),
    {
        self.write_header()?;
        self.write_key_values(&mut progress)?;
        self.write_footer()?;

        Ok(())
    }

    fn write_row<T>(&mut self, row: T) -> Result<(), Error>
    where
        T: Serialize,
    {
        self.output_file.write_all(&[RECORD_SEPARATOR])?;

        let mut serializer = serde_json::Serializer::new(&mut self.output_file);
        row.serialize(&mut serializer)?;

        self.output_file.write_all(&[NEWLINE])?;

        Ok(())
    }

    fn write_header(&mut self) -> Result<(), Error> {
        let database = self.database.take().unwrap();

        let header_row = MetadataRow {
            key_value_count: database.metadata().key_value_count(),
        };

        self.write_row(Row::Metadata(header_row))?;

        self.database = Some(database);

        Ok(())
    }

    fn write_footer(&mut self) -> Result<(), Error> {
        self.write_row(Row::Eof)
    }

    fn write_key_values(&mut self, progress: &mut dyn FnMut(u64)) -> Result<(), Error> {
        let database = self.database.take().unwrap();
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

            progress(self.counter);
        }

        self.database = Some(database);

        Ok(())
    }
}

/// Import key-value pairs from the given source file into the database.
///
/// The provided progress callback will be called with the number of pairs
/// processed.
///
/// It is the caller's responsibility to call [`Database::flush()`] after
/// the function completes.
pub fn import<R, C>(database: &mut Database, input_file: &mut R, progress: C) -> Result<(), Error>
where
    C: FnMut(u64),
    R: BufRead,
{
    let mut reader = ImportReader::new(input_file, database);
    reader.import(progress)?;

    Ok(())
}

/// Export key-value pairs from the database to the destination file.
///
/// The provided progress callback will be called with the number of pairs
/// processed.
///
/// It is the caller's responsibility to ensure data has been persisted using
/// functions such as `flush()` or `sync_data()`.
pub fn export<W, C>(database: &mut Database, output_file: &mut W, progress: C) -> Result<(), Error>
where
    W: Write,
    C: FnMut(u64),
{
    let mut writer = ExportWriter::new(output_file, database);
    writer.export(progress)?;

    Ok(())
}
