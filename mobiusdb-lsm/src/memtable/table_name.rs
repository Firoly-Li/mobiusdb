

// table_name:  perfix + u64 

use crate::utils::time_utils::now;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName(String);

impl TableName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(format!("{}-{}",name.into(),now().to_string()))
    }
}