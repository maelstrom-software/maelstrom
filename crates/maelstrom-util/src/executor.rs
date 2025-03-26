pub fn foo() -> i64 {
    42
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_foo() {
        assert_eq!(foo(), 42);
    }
}
