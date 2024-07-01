use arrow::array::RecordBatch;
use dashmap::DashMap;



pub fn batch_size(batch: &RecordBatch) -> usize {
    
    let mut size = 0;
    for column in batch.columns() {
        size += column.get_array_memory_size();
    }
    size
}





#[derive(Debug,Default,Clone)]
pub struct TableSize{
    table: DashMap<String,usize>
}

impl AsRef<DashMap<String,usize>> for TableSize {
    fn as_ref(&self) -> &DashMap<String,usize> {
        &self.table
    }
}


impl TableSize {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self,key:impl AsRef<str>,value:usize) -> usize{
        let mut size = self.get(key.as_ref()).unwrap_or(0);
        size += value;
        self.table.insert(key.as_ref().to_string(),size);
        size
    }

    pub fn get(&self,key:impl AsRef<str>) -> Option<usize>{
        let size = self.table.get(key.as_ref());
        match size {
            Some(size) => Some(size.clone()),
            None => None
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_insert() {
        let mut table_size = TableSize::new();
        table_size.insert("key1", 10);
        let size = &table_size.table.get("key1").unwrap().clone();
        assert_eq!(size, &10);
        table_size.insert("key1", 10);
        table_size.insert("key1", 10);
        let size = &table_size.table.get("key1").unwrap().clone();
        assert_eq!(size, &30);
    }
}