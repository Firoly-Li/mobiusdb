mod memtable;
mod sstable;
mod utils;
mod wal;

#[derive(Debug, PartialEq, Clone)]
pub struct Index {
    pub(crate)offset: usize,
    pub(crate)len: usize,
    indexs: Vec<u32>,
}

impl Index {
    fn from(offset: usize) -> Self {
        Self {
            offset,
            len: 0,
            indexs: Vec::new(),
        }
    }

    fn with_indexs(offset: usize, indexs: Vec<u32>) -> Self {
        let sum: u64 = indexs.iter().map(|&num| num as u64).sum();
        Self {
            offset,
            len: sum as usize,
            indexs,
        }
    }

    fn add_index(&mut self, index: u32) {
        self.indexs.push(index);
    }

    fn update(&mut self, len: usize) {
        self.len = len;
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
