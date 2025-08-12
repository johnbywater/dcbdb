use std::{fmt, io};

// Error types
#[derive(Debug)]
pub enum LmdbError {
    Io(io::Error),
    PageNotFound(PageID),
    DirtyPageNotFound(PageID),
    RootIDMismatch(PageID, PageID),
    DatabaseCorrupted(String),
    SerializationError(String),
    DeserializationError(String),
    PageAlreadyFreed(PageID),
    PageAlreadyDirty(PageID),
}

impl From<io::Error> for LmdbError {
    fn from(err: io::Error) -> Self {
        LmdbError::Io(err)
    }
}

impl fmt::Display for LmdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LmdbError::Io(err) => write!(f, "IO error: {err}"),
            LmdbError::PageNotFound(page_id) => write!(f, "Page not found: {page_id:?}"),
            LmdbError::DirtyPageNotFound(page_id) => {
                write!(f, "Dirty page not found: {page_id:?}")
            }
            LmdbError::RootIDMismatch(old_id, new_id) => {
                write!(f, "Root ID mismatched: old {old_id:?} new {new_id:?}")
            }
            LmdbError::DatabaseCorrupted(msg) => write!(f, "Database corrupted: {msg}"),
            LmdbError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            LmdbError::DeserializationError(msg) => write!(f, "Deserialization error: {msg}"),
            LmdbError::PageAlreadyFreed(page_id) => {
                write!(f, "Page already freed: {page_id:?}")
            }
            LmdbError::PageAlreadyDirty(page_id) => {
                write!(f, "Page already dirty: {page_id:?}")
            }
        }
    }
}

impl std::error::Error for LmdbError {}

// Result type alias
pub type Result<T> = std::result::Result<T, LmdbError>;

// NewType definitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageID(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tsn(pub u32);
