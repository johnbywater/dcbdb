//! DCBSD - A fresh start
//! 
//! This library is a clean slate for the project.

/// Placeholder function
pub fn hello() -> &'static str {
    "Hello, World!"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(hello(), "Hello, World!");
    }
}