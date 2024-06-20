#[derive(Debug, PartialEq, Clone)]
pub struct Offset {
    pub(crate) offset: usize,
    pub(crate) len: usize,
}

impl Offset {
    pub fn from(offset: usize) -> Self {
        Self { offset, len: 0 }
    }

    pub fn with_fd_lens(offset: usize, fd_lens: Vec<u32>) -> Self {
        let sum: u64 = fd_lens.iter().map(|&num| num as u64).sum();
        Self {
            offset,
            len: sum as usize,
        }
    }

    pub fn update(&mut self, len: usize) {
        self.len = len;
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
