use crate::mvcc_nodes::PageID;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

// Pager for file I/O
pub struct Pager {
    pub file: Mutex<File>,
    pub page_size: usize,
    pub is_file_new: bool,
}

// Implementation for Pager
impl Pager {
    pub fn new(path: &Path, page_size: usize) -> io::Result<Self> {
        let is_file_new = !path.exists();

        let file = if is_file_new {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?
        } else {
            OpenOptions::new().read(true).write(true).open(path)?
        };

        Ok(Self {
            file: Mutex::new(file),
            page_size,
            is_file_new,
        })
    }

    pub fn write_page(&mut self, page_id: PageID, page: &[u8]) -> io::Result<()> {
        let mut file: MutexGuard<File> = self.file.lock().unwrap();
        if page.len() > self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Page overflow: page_id={:?} size={} > PAGE_SIZE={}",
                    page_id,
                    page.len(),
                    self.page_size
                ),
            ));
        }

        // Seek to the correct position
        file.seek(SeekFrom::Start(
            (page_id.0 as u64) * (self.page_size as u64),
        ))?;

        // Write the page data
        file.write_all(page)?;

        // Pad with zeros if needed
        let padding_size = self.page_size - page.len();
        if padding_size > 0 {
            let padding = vec![0u8; padding_size];
            file.write_all(&padding)?;
        }

        Ok(())
    }

    pub fn read_page(&self, page_id: PageID) -> io::Result<Vec<u8>> {
        let mut file: MutexGuard<File> = self.file.lock().unwrap();
        let offset = (page_id.0 as u64) * (self.page_size as u64);
        file.seek(SeekFrom::Start(offset))?;
        let mut page = vec![0u8; self.page_size];
        let bytes_read = file.read(&mut page)?;
        if bytes_read < self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }
        Ok(page)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let mut file: MutexGuard<File> = self.file.lock().unwrap();
        file.flush()?;
        // fsync equivalent in Rust
        #[cfg(unix)]
        unsafe {
            let result = libc::fsync(file.as_raw_fd());
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}
