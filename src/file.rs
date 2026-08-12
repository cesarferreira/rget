//! Random-access destination file (PRD §9).
//!
//! Workers write straight into the final file at the right offset. There are no
//! per-chunk temp files and no merge pass, so a 500 GB download needs 500 GB of
//! disk, not 1 TB, and finishing costs nothing.
//!
//! Positioned writes (`pwrite`) are used rather than seek+write so that
//! concurrent writers share one file descriptor without a lock and without
//! racing on the file cursor.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// Identifies the *file*, not the path. Used on resume to detect that the file
/// we recorded progress against has been replaced by a different one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileIdentity {
    pub dev: u64,
    pub ino: u64,
}

pub struct DestFile {
    file: File,
    path: PathBuf,
}

impl DestFile {
    /// Open (or create) the destination for random-access writing without
    /// truncating: an existing partial file is exactly what we want to resume
    /// into.
    pub fn open(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Size the file up front. This turns "out of disk" into an error now
    /// rather than at 90%, and gives the filesystem a chance to lay the file
    /// out contiguously.
    ///
    /// `set_len` creates a sparse file on every filesystem we target; it
    /// reserves the *size*, not necessarily the *blocks*. We accept that: the
    /// alternative (writing zeroes over the whole range) would double the I/O
    /// for a 500 GB download.
    pub fn preallocate(&self, size: u64) -> io::Result<()> {
        if self.file.metadata()?.len() != size {
            self.file.set_len(size)?;
        }
        Ok(())
    }

    /// Write every byte of `buf` at `offset`. Short writes are retried, so a
    /// successful return means the whole buffer reached the kernel.
    pub fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(buf, offset)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut written = 0;
            while written < buf.len() {
                let n = self
                    .file
                    .seek_write(&buf[written..], offset + written as u64)?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                written += n;
            }
            Ok(())
        }
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_at(buf, offset)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.file.seek_read(buf, offset)
        }
    }

    /// The durability barrier. See `docs/CRASH_CONSISTENCY.md`: this must
    /// return before any of the bytes it covers may be recorded as complete.
    ///
    /// `sync_data` rather than `sync_all` — we need the data and the block map
    /// durable, not the mtime.
    pub fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Named `size` rather than `len` because a file has no meaningful
    /// `is_empty` counterpart and the pair would only mislead.
    pub fn size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn truncate(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    pub fn identity(&self) -> io::Result<FileIdentity> {
        identity_of(&self.file)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reopen a fresh handle for hashing, so verification reads do not disturb
    /// the write handle.
    pub fn open_for_read(&self) -> io::Result<File> {
        File::open(&self.path)
    }
}

fn identity_of(file: &File) -> io::Result<FileIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let m = file.metadata()?;
        Ok(FileIdentity {
            dev: m.dev(),
            ino: m.ino(),
        })
    }
    #[cfg(windows)]
    {
        // The equivalents on `Metadata` are still unstable
        // (`windows_by_handle`), so ask the OS directly. Volume serial plus
        // file index is Windows' answer to dev+ino.
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };

        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        // Safe: the handle is owned by `file` and outlives the call, and `info`
        // is a valid, correctly sized out-parameter.
        let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as HANDLE, &mut info) };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(FileIdentity {
            dev: u64::from(info.dwVolumeSerialNumber),
            ino: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rget-file-test-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn writes_at_offsets_out_of_order() {
        let dir = tmpdir("offsets");
        let path = dir.join("out.bin");
        let f = DestFile::open(&path).unwrap();
        f.preallocate(10).unwrap();
        f.write_at(b"world", 5).unwrap();
        f.write_at(b"hello", 0).unwrap();
        f.sync_data().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"helloworld");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn opening_does_not_truncate() {
        let dir = tmpdir("notrunc");
        let path = dir.join("keep.bin");
        std::fs::write(&path, b"existing").unwrap();
        let f = DestFile::open(&path).unwrap();
        assert_eq!(f.size().unwrap(), 8);
        drop(f);
        assert_eq!(std::fs::read(&path).unwrap(), b"existing");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn preallocate_sets_exact_size() {
        let dir = tmpdir("prealloc");
        let path = dir.join("big.bin");
        let f = DestFile::open(&path).unwrap();
        f.preallocate(1024 * 1024).unwrap();
        assert_eq!(f.size().unwrap(), 1024 * 1024);
        // Idempotent.
        f.preallocate(1024 * 1024).unwrap();
        assert_eq!(f.size().unwrap(), 1024 * 1024);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn identity_distinguishes_files() {
        let dir = tmpdir("identity");
        let a = DestFile::open(&dir.join("a.bin")).unwrap();
        let b = DestFile::open(&dir.join("b.bin")).unwrap();

        // Two different files never share an identity...
        assert_ne!(a.identity().unwrap(), b.identity().unwrap());
        // ...and reopening the same file yields the same one.
        let a_again = DestFile::open(&dir.join("a.bin")).unwrap();
        assert_eq!(a.identity().unwrap(), a_again.identity().unwrap());

        // Note: delete-then-recreate can legitimately reuse an inode on Linux,
        // which is why resume also checks the recorded size, not identity alone.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn creates_missing_parents() {
        let dir = tmpdir("parents");
        let path = dir.join("a/b/c/deep.bin");
        let f = DestFile::open(&path).unwrap();
        f.write_at(b"x", 0).unwrap();
        assert!(path.exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
