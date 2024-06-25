use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as usize
}

pub fn now_as_nano() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as usize
}

#[cfg(test)]
mod tests {
    use crate::utils::time_utils::now_as_nano;

    use super::now;

    #[test]
    fn now_should_return_current_time() {
        let now = now();
        assert!(now > 0);
        let now_str = now.to_string();
        println!("now_str: {}", now_str);
        assert!(now_str.len() == 16);
    }

    #[test]
    fn now_as_nano_should_return_current_time() {
        let now = now_as_nano();
        assert!(now > 0);
        let now_str = now.to_string();
        println!("now_str: {}", now_str);
        assert!(now_str.len() == 19);
    }
}
