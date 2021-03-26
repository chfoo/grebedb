//! Virtual file system interface for database storage.

use std::{collections::HashMap, fmt::Debug, io::Write, path::PathBuf};

use relative_path::{RelativePath, RelativePathBuf};
use vfs::{MemoryFS, VfsFileType, VfsPath};

use crate::error::Error;

/// Represents a virtual file system.
///
/// File paths are characters within pattern `[a-z0-9._]` in Unix style
/// where directory separators as slashes (`/`). Paths are specified in
/// relative notation such as `example/my_file.ext`.
///
/// Implementations are not expected to support directory traversal notations
/// or handling redundant slashes. Implementations can return an error in
/// those cases.
pub trait Vfs {
    /// Lock the file preventing other processes from accessing it.
    ///
    /// If the file is already locked, an error is returned.
    fn lock(&mut self, path: &str) -> Result<(), Error>;

    /// Unlock the file.
    ///
    /// If the file is not locked, an error is returned.
    fn unlock(&mut self, path: &str) -> Result<(), Error>;

    /// Read the contents of a file to a vector.
    fn read(&self, path: &str) -> Result<Vec<u8>, Error>;

    /// Write the contents to a file.
    ///
    /// The file will be created if it does not exist and existing data is
    /// overwritten.
    fn write(&mut self, path: &str, data: &[u8]) -> Result<(), Error>;

    /// Write the contents to a file and ensure data is written to storage.
    ///
    /// Like `write` but all data is flushed from buffers to persistent
    /// storage before returning.
    fn write_and_sync_all(&mut self, path: &str, data: &[u8]) -> Result<(), Error>;

    /// Delete a file.
    ///
    /// If the file does not exist, an error is returned.
    fn remove_file(&mut self, path: &str) -> Result<(), Error>;

    /// Return a vector of filenames in a directory.
    fn read_dir(&self, path: &str) -> Result<Vec<String>, Error>;

    /// Create a directory at the given path.
    ///
    /// The parent directory must exist.
    fn create_dir(&mut self, path: &str) -> Result<(), Error>;

    /// Create directories for all components of the path if they do not exist.
    fn create_dir_all(&mut self, path: &str) -> Result<(), Error> {
        let mut current_path = RelativePathBuf::default();
        for part in RelativePath::new(path).components() {
            current_path.push(part.as_str());

            if !self.exists(current_path.as_str())? {
                self.create_dir(current_path.as_str())?;
            }
        }

        Ok(())
    }

    /// Remove an empty directory.
    ///
    /// If the path is not an empty directory, an error is returned.
    fn remove_dir(&mut self, path: &str) -> Result<(), Error>;

    /// Remove empty directories in the path if they exist.
    fn remove_empty_dir_all(&mut self, path: &str) -> Result<(), Error> {
        let mut current_path = RelativePathBuf::from(path);

        loop {
            if current_path.as_str() != "" && self.read_dir(current_path.as_str())?.is_empty() {
                self.remove_dir(current_path.as_str())?;
            } else {
                break;
            }

            if let Some(parent) = current_path.parent() {
                current_path = parent.to_owned();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Rename a file.
    ///
    /// If the destination file path already exists, the file is overwritten.
    fn rename_file(&mut self, old_path: &str, new_path: &str) -> Result<(), Error>;

    /// Return whether the path is a directory.
    ///
    /// Returns an error if the path does not exist.
    fn is_dir(&self, path: &str) -> Result<bool, Error>;

    /// Return whether the path exists.
    fn exists(&self, path: &str) -> Result<bool, Error>;
}

/// A file system that is stored temporarily to memory.
#[derive(Clone)]
pub struct MemoryVfs {
    vfs: VfsPath,
}

impl MemoryVfs {
    /// Create a in-memory file system.
    pub fn new() -> Self {
        Self {
            vfs: VfsPath::new(MemoryFS::default()),
        }
    }
}

impl Default for MemoryVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for MemoryVfs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryVfs")
    }
}

impl Vfs for MemoryVfs {
    fn lock(&mut self, _path: &str) -> Result<(), Error> {
        Ok(())
    }

    fn unlock(&mut self, _path: &str) -> Result<(), Error> {
        Ok(())
    }

    fn read(&self, path: &str) -> Result<Vec<u8>, Error> {
        let mut file = self.vfs.join(path)?.open_file()?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    fn write(&mut self, path: &str, data: &[u8]) -> Result<(), Error> {
        let mut file = self.vfs.join(path)?.create_file()?;
        file.write_all(data)?;
        Ok(())
    }

    fn write_and_sync_all(&mut self, path: &str, data: &[u8]) -> Result<(), Error> {
        self.write(path, data)
    }

    fn remove_file(&mut self, path: &str) -> Result<(), Error> {
        self.vfs.join(path)?.remove_file()?;
        Ok(())
    }

    fn read_dir(&self, path: &str) -> Result<Vec<String>, Error> {
        let mut filenames = Vec::new();

        for sub_path in self.vfs.join(path)?.read_dir()? {
            filenames.push(sub_path.filename());
        }

        Ok(filenames)
    }

    fn create_dir(&mut self, path: &str) -> Result<(), Error> {
        self.vfs.join(path)?.create_dir()?;
        Ok(())
    }

    fn remove_dir(&mut self, path: &str) -> Result<(), Error> {
        self.vfs.join(path)?.remove_dir()?;
        Ok(())
    }

    fn rename_file(&mut self, old_path: &str, new_path: &str) -> Result<(), Error> {
        if self.exists(new_path)? {
            self.remove_file(new_path)?;
        }

        self.vfs
            .join(old_path)?
            .move_file(&self.vfs.join(new_path)?)?;

        Ok(())
    }

    fn is_dir(&self, path: &str) -> Result<bool, Error> {
        let metadata = self.vfs.join(path)?.metadata()?;
        Ok(matches!(metadata.file_type, VfsFileType::Directory))
    }

    fn exists(&self, path: &str) -> Result<bool, Error> {
        Ok(self.vfs.join(path)?.exists()?)
    }
}

#[cfg(feature = "fslock")]
type LockFileType = fslock::LockFile;

/// Interface to a real file system on disk.
pub struct OsVfs {
    root: PathBuf,

    #[cfg(feature = "fslock")]
    locks: HashMap<PathBuf, LockFileType>,
}

impl OsVfs {
    /// Create a file system interface to the given path.
    ///
    /// The given path is treated as the root for subsequent relative path
    /// operations.
    pub fn new<P>(root: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            root: root.into(),
            #[cfg(feature = "fslock")]
            locks: HashMap::new(),
        }
    }
}

impl Debug for OsVfs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OsVfs {{ path: {:?} }}", &self.root)
    }
}

impl Vfs for OsVfs {
    #[cfg(feature = "fslock")]
    fn lock(&mut self, path: &str) -> Result<(), Error> {
        let mut lock = fslock::LockFile::open(self.root.join(path).as_path())?;
        if !lock.try_lock()? {
            return Err(Error::Locked);
        }
        self.locks.insert(self.root.join(path), lock);

        Ok(())
    }
    #[cfg(not(feature = "fslock"))]
    fn lock(&mut self, _path: &str) -> Result<(), Error> {
        Err(Error::FileLockingUnavailable)
    }

    #[cfg(feature = "fslock")]
    fn unlock(&mut self, path: &str) -> Result<(), Error> {
        if let Some(mut lock) = self.locks.remove(&self.root.join(path)) {
            lock.unlock()?;
        } else {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "file not locked",
            )));
        }

        Ok(())
    }

    #[cfg(not(feature = "fslock"))]
    fn unlock(&mut self, path: &str) -> Result<(), Error> {
        Err(Error::FileLockingUnavailable)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>, Error> {
        Ok(std::fs::read(self.root.join(path))?)
    }

    fn write(&mut self, path: &str, data: &[u8]) -> Result<(), Error> {
        Ok(std::fs::write(self.root.join(path), data)?)
    }

    fn write_and_sync_all(&mut self, path: &str, data: &[u8]) -> Result<(), Error> {
        let mut file = std::fs::File::create(self.root.join(path))?;
        file.write_all(&data)?;
        file.sync_all()?;
        Ok(())
    }

    fn remove_file(&mut self, path: &str) -> Result<(), Error> {
        Ok(std::fs::remove_file(self.root.join(path))?)
    }

    fn read_dir(&self, path: &str) -> Result<Vec<String>, Error> {
        let dir = std::fs::read_dir(self.root.join(path))?;
        let mut filenames = Vec::new();

        for entry in dir {
            let entry = entry?;

            if let Ok(filename) = entry.file_name().into_string() {
                filenames.push(filename);
            }
        }

        Ok(filenames)
    }

    fn create_dir(&mut self, path: &str) -> Result<(), Error> {
        std::fs::create_dir(self.root.join(path))?;
        Ok(())
    }

    fn remove_dir(&mut self, path: &str) -> Result<(), Error> {
        std::fs::remove_dir(self.root.join(path))?;
        Ok(())
    }

    fn rename_file(&mut self, old_path: &str, new_path: &str) -> Result<(), Error> {
        std::fs::rename(self.root.join(old_path), self.root.join(new_path))?;
        Ok(())
    }

    fn is_dir(&self, path: &str) -> Result<bool, Error> {
        let metadata = std::fs::metadata(self.root.join(path))?;

        Ok(metadata.is_dir())
    }

    fn exists(&self, path: &str) -> Result<bool, Error> {
        Ok(self.root.join(path).exists())
    }
}

/// Wrapper that allows only read operations.
pub struct ReadOnlyVfs {
    inner: Box<dyn Vfs + Sync + Send>,
}

impl ReadOnlyVfs {
    /// Wrap a VFS.
    pub fn new(inner: Box<dyn Vfs + Sync + Send>) -> Self {
        Self { inner }
    }

    /// Return the wrapped VFS.
    pub fn into_inner(self) -> Box<dyn Vfs + Sync + Send> {
        self.inner
    }
}

impl Vfs for ReadOnlyVfs {
    fn lock(&mut self, path: &str) -> Result<(), Error> {
        self.inner.lock(path)
    }

    fn unlock(&mut self, path: &str) -> Result<(), Error> {
        self.inner.unlock(path)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>, Error> {
        self.inner.read(path)
    }

    fn write(&mut self, _path: &str, _data: &[u8]) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn write_and_sync_all(&mut self, _path: &str, _data: &[u8]) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn remove_file(&mut self, _path: &str) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn read_dir(&self, path: &str) -> Result<Vec<String>, Error> {
        self.inner.read_dir(path)
    }

    fn create_dir(&mut self, _path: &str) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn remove_dir(&mut self, _path: &str) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn rename_file(&mut self, _old_path: &str, _new_path: &str) -> Result<(), Error> {
        Err(Error::ReadOnly)
    }

    fn is_dir(&self, path: &str) -> Result<bool, Error> {
        self.inner.is_dir(path)
    }

    fn exists(&self, path: &str) -> Result<bool, Error> {
        self.inner.exists(path)
    }
}

impl Debug for ReadOnlyVfs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadOnlyVfs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recursive_helpers() {
        let mut vfs = MemoryVfs::new();

        vfs.create_dir_all("a/b/c").unwrap();
        vfs.write("a/b/c/my_file", "hello world!".as_bytes())
            .unwrap();
        vfs.remove_empty_dir_all("a/b/c").unwrap();
        assert!(vfs.exists("a/b/c").unwrap());
        vfs.remove_file("a/b/c/my_file").unwrap();
        vfs.remove_empty_dir_all("a/b/c").unwrap();
        assert!(!vfs.exists("a/b/c").unwrap());
    }
}
