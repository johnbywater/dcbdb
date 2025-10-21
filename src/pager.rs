use crate::common::PageID;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use memmap2::{Mmap, MmapOptions};
// use memmap2::{Advice, Mmap, MmapOptions};

// Pager for file I/O
pub struct Pager {
    pub file: Mutex<File>,
    pub page_size: usize,
    pub is_file_new: bool,
    // Number of logical database pages contained in a single mmap window.
    mmap_pages_per_map: usize,
    // Cache of memory maps, keyed by map identifier (floor(page_id / mmap_pages_per_map)).
    mmaps: Mutex<HashMap<u64, Arc<Mmap>>>,
}

// A zero-copy view over a page backed by a memory map. Holds an Arc to keep the mapping alive.
pub struct MappedPage {
    mmap: Arc<Mmap>,
    start: usize,
    len: usize,
}

impl MappedPage {
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[self.start..self.start + self.len]
    }
}

// Implementation for Pager
impl Pager {
    fn gcd(mut a: usize, mut b: usize) -> usize {
        while b != 0 {
            let t = b;
            b = a % t;
            a = t;
        }
        a
    }

    #[cfg(unix)]
    fn os_page_size() -> usize {
        // Safe: sysconf is thread-safe and returns a constant for page size
        unsafe {
            let sz = libc::sysconf(libc::_SC_PAGESIZE);
            if sz <= 0 {
                4096usize // sensible default
            } else {
                sz as usize
            }
        }
    }

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

        // Compute pages per mmap so that:
        // - Each mmap offset is aligned to OS page size (and implicitly DB page size), and
        // - Each mapping window is at least 10 MiB.
        let os_ps = Self::os_page_size();
        let g = Self::gcd(os_ps, page_size);
        // Alignment factor: number of DB pages per window must be a multiple of this to keep
        // mmap offsets aligned to OS page boundaries.
        let align_pages = usize::max(1, os_ps / g);
        // Minimum window size in bytes: 10 MiB
        let min_window_bytes: usize = 10 * 1024 * 1024;
        // Minimum number of DB pages to reach at least the min window size
        let min_pages = (min_window_bytes + page_size - 1) / page_size; // ceil
        // Choose smallest multiple of align_pages that is >= min_pages
        let k = if min_pages == 0 { 1 } else { (min_pages + align_pages - 1) / align_pages }; // ceil
        let mmap_pages_per_map = align_pages * usize::max(1, k);

        Ok(Self {
            file: Mutex::new(file),
            page_size,
            is_file_new,
            mmap_pages_per_map,
            mmaps: Mutex::new(HashMap::new()),
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
        // Precompute addressing values
        let page_size_u64 = self.page_size as u64;
        let offset = page_id.0 * page_size_u64;
        let pages_per_map = self.mmap_pages_per_map as u64;
        let map_id = page_id.0 / pages_per_map;
        let map_offset = map_id * pages_per_map * page_size_u64;
        let within = (offset - map_offset) as usize;

        // Fast path: if mapping exists, do not obtain file lock; take Arc clone and release map lock
        if let Some(mmap_arc) = {
            let maps = self.mmaps.lock().unwrap();
            maps.get(&map_id).cloned()
        } {
            let start = within;
            let stop = start + self.page_size;
            if stop > mmap_arc.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Page {page_id:?} not found"),
                ));
            }
            return Ok(mmap_arc[start..stop].to_vec());
        }

        // Slow path: need to create the mapping with double-checked locking
        let file: MutexGuard<File> = self.file.lock().unwrap();
        let file_len = file.metadata()?.len();

        // Re-check if mapping appeared while we acquired the file lock
        if let Some(mmap_arc) = {
            let maps = self.mmaps.lock().unwrap();
            maps.get(&map_id).cloned()
        } {
            let start = within;
            let stop = start + self.page_size;
            if stop > mmap_arc.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Page {page_id:?} not found"),
                ));
            }
            return Ok(mmap_arc[start..stop].to_vec());
        }

        // Calculate standard mapping window length (does not depend on current file length)
        let max_len = pages_per_map * page_size_u64;

        // Before mapping, ensure the requested page is within the current file length.
        let page_end = offset + page_size_u64;
        if page_end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }

        // Ensure the underlying file is large enough to permit a full standard-length mapping.
        let required_len = map_offset + max_len;
        if file_len < required_len {
            file.set_len(required_len)?;
        }

        // Create the mmap and insert it, but guard with a double-check
        let mmap_new = unsafe {
            MmapOptions::new()
                .offset(map_offset)
                .len(max_len as usize)
                .map(&*file)?
        };
        let mmap_arc = {
            let mut maps = self.mmaps.lock().unwrap();
            // Another thread could have inserted meanwhile
            if let Some(existing) = maps.get(&map_id) {
                existing.clone()
            } else {
                let arc = Arc::new(mmap_new);
                maps.insert(map_id, arc.clone());
                // println!(
                //     "Created new mmap: map_id={} offset={} len={}",
                //     map_id, map_offset, max_len
                // );
                arc
            }
        };

        // Now copy the requested page
        let start = within;
        let stop = start + self.page_size;
        if stop > mmap_arc.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }
        Ok(mmap_arc[start..stop].to_vec())
    }

    pub fn read_page_mmap_slice(&self, page_id: PageID) -> io::Result<MappedPage> {
        // Precompute addressing values
        let page_size_u64 = self.page_size as u64;
        let offset = page_id.0 * page_size_u64;
        let pages_per_map = self.mmap_pages_per_map as u64;
        let map_id = page_id.0 / pages_per_map;
        let map_offset = map_id * pages_per_map * page_size_u64;
        let within = (offset - map_offset) as usize;

        // Fast path: if mapping exists, do not obtain file lock; take Arc clone and release map lock
        if let Some(mmap_arc) = {
            let maps = self.mmaps.lock().unwrap();
            maps.get(&map_id).cloned()
        } {
            let start = within;
            let stop = start + self.page_size;
            if stop > mmap_arc.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Page {page_id:?} not found"),
                ));
            }
            return Ok(MappedPage { mmap: mmap_arc, start, len: self.page_size });
        }

        // Slow path: need to create the mapping with double-checked locking
        let file: MutexGuard<File> = self.file.lock().unwrap();
        let file_len = file.metadata()?.len();

        // Re-check if mapping appeared while we acquired the file lock
        if let Some(mmap_arc) = {
            let maps = self.mmaps.lock().unwrap();
            maps.get(&map_id).cloned()
        } {
            let start = within;
            let stop = start + self.page_size;
            if stop > mmap_arc.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Page {page_id:?} not found"),
                ));
            }
            return Ok(MappedPage { mmap: mmap_arc, start, len: self.page_size });
        }

        // Calculate standard mapping window length (does not depend on current file length)
        let max_len = pages_per_map * page_size_u64;

        // Before mapping, ensure the requested page is within the current file length.
        let page_end = offset + page_size_u64;
        if page_end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }

        // Ensure the underlying file is large enough to permit a full standard-length mapping.
        let required_len = map_offset + max_len;
        if file_len < required_len {
            file.set_len(required_len)?;
        }

        // Create the mmap and insert it, but guard with a double-check
        let mmap_new = unsafe {
            MmapOptions::new()
                .offset(map_offset)
                .len(max_len as usize)
                .map(&*file)?
        };
        // mmap_new.advise(Advice::Random)?;
        // mmap_new.advise(Advice::WillNeed)?;
        let mmap_arc = {
            let mut maps = self.mmaps.lock().unwrap();
            // Another thread could have inserted meanwhile
            if let Some(existing) = maps.get(&map_id) {
                existing.clone()
            } else {
                let arc = Arc::new(mmap_new);
                maps.insert(map_id, arc.clone());
                // println!(
                //     "Created new mmap: map_id={} offset={} len={}",
                //     map_id, map_offset, max_len
                // );
                arc
            }
        };

        // Now form the mapped page view
        let start = within;
        let stop = start + self.page_size;
        if stop > mmap_arc.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Page {page_id:?} not found"),
            ));
        }
        Ok(MappedPage { mmap: mmap_arc, start, len: self.page_size })
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

    #[cfg(test)]
    pub fn debug_mmap_count(&self) -> usize {
        self.mmaps.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn debug_pages_per_mmap(&self) -> usize {
        self.mmap_pages_per_map
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
        dir.keep().join(name)
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

    #[test]
    fn mmap_reuse_within_same_map() {
        // Try a few page sizes to ensure we get at least 2 pages per mmap window
        let candidates = [512usize, 1024, 2048, 4096, 8192];
        let mut pager_opt = None;
        for ps in candidates {
            let path = temp_file_path("pager_mmap_reuse.db");
            let pager = Pager::new(&path, ps).expect("pager new");
            if pager.debug_pages_per_mmap() >= 2 {
                pager_opt = Some((pager, ps));
                break;
            }
        }
        let (pager, page_size) = pager_opt.expect("could not find suitable page size for test");

        // Write two pages
        pager
            .write_page(PageID(0), &vec![1u8; page_size / 2])
            .expect("write p0");
        pager
            .write_page(PageID(1), &vec![2u8; page_size])
            .expect("write p1");
        pager.flush().expect("flush");

        // Read both pages via mmap
        let _ = pager.read_page_mmap(PageID(0)).expect("read0m");
        assert_eq!(pager.debug_mmap_count(), 1, "first mmap created");
        let _ = pager.read_page_mmap(PageID(1)).expect("read1m");
        assert_eq!(pager.debug_mmap_count(), 1, "should reuse same mmap for pages in same window");
    }

    #[test]
    fn mmap_creates_new_on_boundary() {
        // Ensure pages_per_mmap is available and >= 2 by selecting suitable page size
        let candidates = [512usize, 1024, 2048, 4096, 8192];
        let mut pager_opt = None;
        for ps in candidates {
            let path = temp_file_path("pager_mmap_boundary.db");
            let pager = Pager::new(&path, ps).expect("pager new");
            if pager.debug_pages_per_mmap() >= 1 {
                pager_opt = Some((pager, ps));
                break;
            }
        }
        let (pager, page_size) = pager_opt.expect("failed to create pager");
        let ppm = pager.debug_pages_per_mmap();

        // Write enough pages to cover two windows
        for p in 0..(ppm as u64 + 1) {
            let fill = if (p % 2) == 0 { 0xAA } else { 0x55 };
            pager
                .write_page(PageID(p), &vec![fill; page_size])
                .expect("write page");
        }
        pager.flush().expect("flush");

        // First read creates first mmap
        let _ = pager.read_page_mmap(PageID(0)).expect("read first window");
        assert_eq!(pager.debug_mmap_count(), 1);
        // Reading the first page in the next window should create a second mmap
        let _ = pager
            .read_page_mmap(PageID(ppm as u64))
            .expect("read second window");
        assert_eq!(pager.debug_mmap_count(), 2);
    }
}
