use std::{cmp::Ordering, time::Duration};

pub fn cmp(lhs: &Option<Duration>, rhs: &Option<Duration>) -> Ordering {
    match (lhs, rhs) {
        (None, None) => Ordering::Equal,
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(lhs), Some(rhs)) => lhs.cmp(rhs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cmp1() {
        let d0 = None;
        let d1 = Some(Duration::from_secs(1));
        let d2 = Some(Duration::from_secs(2));

        assert_eq!(cmp(&d0, &d0), Ordering::Equal);
        assert_eq!(cmp(&d0, &d1), Ordering::Greater);
        assert_eq!(cmp(&d0, &d2), Ordering::Greater);
        assert_eq!(cmp(&d1, &d0), Ordering::Less);
        assert_eq!(cmp(&d1, &d1), Ordering::Equal);
        assert_eq!(cmp(&d1, &d2), Ordering::Less);
        assert_eq!(cmp(&d2, &d0), Ordering::Less);
        assert_eq!(cmp(&d2, &d1), Ordering::Greater);
        assert_eq!(cmp(&d2, &d2), Ordering::Equal);
    }
}
