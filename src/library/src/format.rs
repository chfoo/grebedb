use std::io::{Cursor, Read, Write};

use relative_path::RelativePath;
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    lru::LruVec,
    vfs::{Vfs, VfsSyncOption},
};

const MAGIC_BYTES: [u8; 8] = [0xFE, b'G', b'r', b'e', b'b', b'e', 0x00, 0x00];

pub struct Format {
    file_buffer: Vec<u8>,
    page_buffer: Vec<u8>,
    payload_buffer: Vec<u8>,
    compression_level: Option<i32>,
    dir_create_cache: LruVec<String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            file_buffer: Vec::new(),
            page_buffer: Vec::new(),
            payload_buffer: Vec::new(),
            compression_level: if cfg!(feature = "zstd") {
                Some(0)
            } else {
                None
            },
            dir_create_cache: LruVec::new(8),
        }
    }
}

impl Format {
    pub fn set_compression_level(&mut self, value: Option<i32>) {
        self.compression_level = value;
    }

    pub fn read_file<'de, T>(&mut self, vfs: &mut dyn Vfs, path: &str) -> Result<T, Error>
    where
        T: Deserialize<'de>,
    {
        let mut file = Cursor::new(vfs.read(path)?);

        let mut magic_bytes: [u8; 8] = [0u8; 8];
        file.read_exact(&mut magic_bytes)?;

        if MAGIC_BYTES != magic_bytes {
            return Err(Error::InvalidFileFormat {
                path: path.to_string(),
                message: "not a database",
            });
        }

        let mut compression_flag: [u8; 1] = [0u8; 1];
        file.read_exact(&mut compression_flag)?;

        if compression_flag[0] == 0x01 {
            self.decompress_to_page_buffer(&mut file)?;
        } else {
            self.page_buffer.clear();
            file.read_to_end(&mut self.page_buffer)?;
        }

        self.deserialize_page(path)
    }

    pub fn write_file<T>(
        &mut self,
        vfs: &mut dyn Vfs,
        path: &str,
        payload: T,
        sync_option: VfsSyncOption,
    ) -> Result<(), Error>
    where
        T: Serialize,
    {
        self.file_buffer.clear();
        self.page_buffer.clear();
        self.payload_buffer.clear();

        self.file_buffer.write_all(&MAGIC_BYTES)?;

        if self.compression_level.is_some() {
            self.file_buffer.write_all(&[0x01])?;
            self.serialize_page(payload)?;
            self.write_compressed_page_to_file_buffer()?;
        } else {
            self.file_buffer.write_all(&[0x00])?;
            self.serialize_page(payload)?;
            self.file_buffer.write_all(&self.page_buffer)?;
        }

        let rel_path = RelativePath::new(path);
        let dir_path = rel_path.parent().unwrap();

        if !self.is_in_dir_cache(dir_path) {
            vfs.create_dir_all(dir_path.as_str())?;
        }

        vfs.write(path, &self.file_buffer, sync_option)?;

        Ok(())
    }

    fn serialize_page<T>(&mut self, object: T) -> Result<(), Error>
    where
        T: Serialize,
    {
        serialize_payload(object, &mut self.payload_buffer)?;

        let size_bytes = self.payload_buffer.len().to_be_bytes();

        self.page_buffer.write_all(&size_bytes)?;
        self.page_buffer.write_all(&self.payload_buffer)?;

        let crc = crc32c::crc32c(&self.payload_buffer);
        let crc_bytes = crc.to_be_bytes();

        self.page_buffer.write_all(&crc_bytes)?;

        Ok(())
    }

    fn write_compressed_page_to_file_buffer(&mut self) -> Result<(), Error> {
        #[cfg(feature = "zstd")]
        {
            let mut temp_buffer = Vec::with_capacity(0);
            std::mem::swap(&mut self.file_buffer, &mut temp_buffer);

            let compression_level = self.compression_level.unwrap();
            let mut compressor = zstd::Encoder::new(temp_buffer, compression_level)?;
            compressor.write_all(&self.page_buffer)?;
            let mut old_writer = compressor.finish()?;

            std::mem::swap(&mut self.file_buffer, &mut old_writer);

            Ok(())
        }
        #[cfg(not(feature = "zstd"))]
        {
            Err(Error::CompressionUnavailable)
        }
    }

    fn is_in_dir_cache(&mut self, dir_path: &RelativePath) -> bool {
        let dir_path = dir_path.to_string();

        if !self.dir_create_cache.touch(&dir_path) {
            self.dir_create_cache.insert(dir_path);
            false
        } else {
            true
        }
    }

    fn decompress_to_page_buffer(&mut self, source: &mut dyn Read) -> Result<(), Error> {
        self.page_buffer.clear();

        #[cfg(feature = "zstd")]
        {
            let mut decompressor = zstd::Decoder::new(source)?;
            decompressor.read_to_end(&mut self.page_buffer)?;
            Ok(())
        }
        #[cfg(not(feature = "zstd"))]
        {
            Err(Error::CompressionUnavailable)
        }
    }

    fn deserialize_page<'de, T>(&mut self, path: &str) -> Result<T, Error>
    where
        T: Deserialize<'de>,
    {
        let mut size_bytes: [u8; 8] = [0u8; 8];
        let mut data = Cursor::new(&mut self.page_buffer);

        data.read_exact(&mut size_bytes)?;
        let size = u64::from_be_bytes(size_bytes) as usize;

        let payload = deserialize_payload(&mut data)?;

        let mut crc_bytes: [u8; 4] = [0; 4];
        data.read_exact(&mut crc_bytes)?;
        let crc = u32::from_be_bytes(crc_bytes);

        let test_crc = crc32c::crc32c(&self.page_buffer[8..8 + size]);

        if crc != test_crc {
            Err(Error::BadChecksum {
                path: path.to_string(),
            })
        } else {
            Ok(payload)
        }
    }
}

fn serialize_payload<T, W>(object: T, destination: W) -> Result<(), Error>
where
    T: Serialize,
    W: Write,
{
    let mut serializer = Serializer::new(destination)
        .with_binary()
        .with_string_variants()
        .with_struct_map();

    match object.serialize(&mut serializer) {
        Ok(_) => Ok(()),
        Err(error) => Err(Error::Other(Box::new(error))),
    }
}

fn deserialize_payload<'de, T, R>(source: R) -> Result<T, Error>
where
    T: Deserialize<'de>,
    R: Read,
{
    let mut deserializer = Deserializer::new(source).with_binary();

    match Deserialize::deserialize(&mut deserializer) {
        Ok(value) => Ok(value),
        Err(error) => Err(Error::Other(Box::new(error))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::MemoryVfs;

    #[test]
    fn test_format() -> Result<(), Error> {
        let mut format = Format::default();
        let mut vfs = MemoryVfs::new();

        format.write_file(&mut vfs, "my_file", "hello world", VfsSyncOption::None)?;

        let payload: String = format.read_file(&mut vfs, "my_file")?;

        assert_eq!(&payload, "hello world");

        Ok(())
    }
}
