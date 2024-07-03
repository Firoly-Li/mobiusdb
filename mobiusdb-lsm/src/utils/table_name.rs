use crate::utils::{file_utils::SSTABLE_FILE_SUFFIX, time_utils::now};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TableName {
    pub(crate) prefix: String,
    pub(crate) time: Option<u64>,
    pub(crate) suffix: Option<String>,
}

impl TableName {
    pub fn new(prefix: impl AsRef<str>) -> Self {
        Self {
            prefix: prefix.as_ref().to_string(),
            time: None,
            suffix: None,
        }
    }

    pub fn new_with_mem_name(mem_table: impl AsRef<str>) -> Self {
        let time = mem_table.as_ref().split("-").collect::<Vec<&str>>();
        Self {
            prefix: time[0].to_string(),
            time: Some(time[1].parse::<u64>().unwrap()),
            suffix: None,
        }
    }
    pub fn new_mem_name(prefix: impl AsRef<str>) -> Self {
        let time = now() as u64;
        Self::new_with_time(prefix, time)
    }

    pub fn new_ss_name(prefix: impl AsRef<str>) -> Self {
        let time = now() as u64;
        Self::new_with_opts(prefix, time, SSTABLE_FILE_SUFFIX)
    }

    pub fn new_with_time(prefix: impl AsRef<str>, create_time: u64) -> Self {
        Self {
            prefix: prefix.as_ref().to_string(),
            time: Some(create_time),
            suffix: None,
        }
    }
    pub fn new_with_opts(
        prefix: impl AsRef<str>,
        create_time: u64,
        suffix: impl AsRef<str>,
    ) -> Self {
        Self {
            prefix: prefix.as_ref().to_string(),
            time: Some(create_time),
            suffix: Some(suffix.as_ref().to_string()),
        }
    }
}

impl TableName {
    pub fn get_memtable_name(&self) -> String {
        match self.time {
            Some(t) => format!("{}-{}", self.prefix, t),
            None => format!("{}", self.prefix),
        }
    }

    pub fn get_sstable_name(&self) -> String {
        match (self.time, self.suffix.clone()) {
            (Some(t), Some(s)) => format!("{}-{}{}", self.prefix, t, s),
            (None, Some(s)) => format!("{}{}", self.prefix, s),
            (Some(t), None) => format!("{}-{}", self.prefix, t),
            (None, None) => self.prefix.clone(),
        }
    }

    pub fn get_prefix_name(&self) -> String {
        self.prefix.clone()
    }

    pub fn get_prefix_name_with_name(mem_table: &str) -> String {
        mem_table.split("-").next().unwrap().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::TableName;

    #[test]
    fn test_table_name() {
        let ss_table = TableName::new_mem_name("test");
        println!("ss_table:{}", ss_table.get_sstable_name());
    }
}
