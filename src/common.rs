use std::{fmt, io};

// Error types
#[derive(Debug)]
pub enum DbError {
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

impl From<io::Error> for DbError {
    fn from(err: io::Error) -> Self {
        DbError::Io(err)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(err) => write!(f, "IO error: {err}"),
            DbError::PageNotFound(page_id) => write!(f, "Page not found: {page_id:?}"),
            DbError::DirtyPageNotFound(page_id) => {
                write!(f, "Dirty page not found: {page_id:?}")
            }
            DbError::RootIDMismatch(old_id, new_id) => {
                write!(f, "Root ID mismatched: old {old_id:?} new {new_id:?}")
            }
            DbError::DatabaseCorrupted(msg) => write!(f, "Database corrupted: {msg}"),
            DbError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            DbError::DeserializationError(msg) => write!(f, "Deserialization error: {msg}"),
            DbError::PageAlreadyFreed(page_id) => {
                write!(f, "Page already freed: {page_id:?}")
            }
            DbError::PageAlreadyDirty(page_id) => {
                write!(f, "Page already dirty: {page_id:?}")
            }
        }
    }
}

impl std::error::Error for DbError {}

// Result type alias
pub type DbResult<T> = Result<T, DbError>;

// NewType definitions
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageID(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tsn(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);
