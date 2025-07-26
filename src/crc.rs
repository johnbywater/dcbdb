use crc32fast::Hasher;

/// Calculate CRC32 checksum for data
pub fn calc_crc(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}