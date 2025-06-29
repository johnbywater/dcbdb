//! DCBSD - Dynamic Consistency Boundaries Event Store
//! 
//! This library provides an event store implementation with support for
//! dynamic consistency boundaries.

// Export the API module
pub mod api;

// Export the WAL module
pub mod wal;

// Export the Segments module
pub mod segments;

// Export the Positions module
pub mod positions;
pub mod positions_old;

// Export the Paged Index File module
pub mod pagedfile;

// Export the Index Pages module
pub mod indexpages;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert!(true);
    }
}
