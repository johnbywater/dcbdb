use crate::common::PageID;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use memmap2::MmapOptions;

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

    pub fn write_page(&self, page_id: PageID, page: &[u8]) -> io::Result<()> {
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
        file.seek(SeekFrom::Start(page_id.0 * (self.page_size as u64)))?;

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

    pub fn write_pages(&self, pages: &[(PageID, Vec<u8>)]) -> io::Result<()> {
        let mut file: MutexGuard<File> = self.file.lock().unwrap();
        for (page_id, page) in pages.iter() {
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
            // Seek to the correct position for this page
            file.seek(SeekFrom::Start(page_id.0 * (self.page_size as u64)))?;
            // Write the page data
            file.write_all(page)?;
            // Pad with zeros if needed
            let padding_size = self.page_size - page.len();
            if padding_size > 0 {
                let padding = vec![0u8; padding_size];
                file.write_all(&padding)?;
            }
        }
        Ok(())
    }

    pub fn read_page(&self, page_id: PageID) -> io::Result<Vec<u8>> {
        let mut file: MutexGuard<File> = self.file.lock().unwrap();
        let offset = page_id.0 * (self.page_size as u64);
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

    pub fn read_page_mmap(&self, page_id: PageID) -> io::Result<Vec<u8>> {
        // Lock the file for consistent metadata and mapping lifetime
        let file: MutexGuard<File> = self.file.lock().unwrap();
        let file_len = file.metadata()?.len();
        let offset = page_id.0 * (self.page_size as u64);
        let end = offset + (self.page_size as u64);
        if end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }
        // Map the entire file to avoid OS page-alignment issues with offsets.
        // For large files, consider mapping a window aligned to the OS page size.
        let mmap = unsafe { MmapOptions::new().map(&*file)? };
        let start = offset as usize;
        let stop = start + self.page_size;
        let mut out = Vec::with_capacity(self.page_size);
        out.extend_from_slice(&mmap[start..stop]);
        Ok(out)
    }

    pub fn flush(&self) -> io::Result<()> {
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


#[cfg(test)]
mod tests {
    use super::Pager;
    use crate::common::PageID;
    use tempfile::tempdir;
    use std::path::PathBuf;

    fn temp_file_path(name: &str) -> PathBuf {
        let dir = tempdir().expect("tempdir");
        dir.into_path().join(name)
    }

    #[test]
    fn mmap_read_matches_normal_read() {
        let page_size = 1024usize;
        let path = temp_file_path("pager_mmap_test.db");
        let pager = Pager::new(&path, page_size).expect("pager new");

        let data1 = vec![1u8; 100];
        let data2 = (0..page_size).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        pager.write_page(PageID(0), &data1).expect("write page 0");
        pager.write_page(PageID(1), &data2).expect("write page 1");
        pager.flush().expect("flush");

        let r0 = pager.read_page(PageID(0)).expect("read0");
        let r0m = pager.read_page_mmap(PageID(0)).expect("read0m");
        assert_eq!(r0, r0m, "mmap read should match std read for page 0");
        // padding zeros expected after data1 length
        assert_eq!(&r0[..100], &data1[..]);
        assert!(r0[100..].iter().all(|&b| b == 0));

        let r1 = pager.read_page(PageID(1)).expect("read1");
        let r1m = pager.read_page_mmap(PageID(1)).expect("read1m");
        assert_eq!(r1, r1m, "mmap read should match std read for page 1");
        assert_eq!(&r1[..], &data2[..]);
    }

    #[test]
    fn mmap_read_out_of_bounds() {
        let page_size = 512usize;
        let path = temp_file_path("pager_mmap_oob.db");
        let pager = Pager::new(&path, page_size).expect("pager new");

        pager.write_page(PageID(0), &[42u8; 10]).expect("write p0");
        pager.flush().expect("flush");

        let err = pager.read_page_mmap(PageID(1)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
