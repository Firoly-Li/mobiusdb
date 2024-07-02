use anyhow::Result;
use tokio::fs;

use super::time_utils::now;

pub const SSTABLE_PATH: &str = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
pub const SSTABLE_FILE_SUFFIX: &'static str = ".sst";

pub const WAL_PATH: &str = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/tmp/wal";

#[derive(Debug, Clone)]
pub enum Level {
    L0,
    L1,
    L2,
    L3,
    L4,
    L5,
}
impl From<Level> for String {
    fn from(value: Level) -> Self {
        match value {
            Level::L0 => String::from("l0"),
            Level::L1 => String::from("l1"),
            Level::L2 => String::from("l2"),
            Level::L3 => String::from("l3"),
            Level::L4 => String::from("l4"),
            Level::L5 => String::from("l5"),
        }
    }
}

/**
 *
 */
pub fn get_sstable_file_name(table_path: &str) -> Option<String> {
    if let Some(v) = table_path.split("/").collect::<Vec<&str>>().last() {
        Some(v.to_string())
    } else {
        None
    }
}

/**
 * 根据文件名称获取sstable文件的路径
 */
pub fn get_sstable_path(table_name: &str, level: Level) -> String {
    match SSTABLE_PATH.ends_with("/") {
        true => format!(
            "{}{}/{}{}",
            SSTABLE_PATH,
            String::from(level),
            table_name,
            SSTABLE_FILE_SUFFIX
        ),
        false => format!(
            "{}/{}/{}{}",
            SSTABLE_PATH,
            String::from(level),
            table_name,
            SSTABLE_FILE_SUFFIX
        ),
    }
}

/**
 *  通过table_prefix + time 生成新的table_name
 */
pub fn create_sstable_path(table_prefix: &str, level: Level) -> String {
    let time = now().to_string();
    let new_table_name = format!("{}-{}", table_prefix, time);
    match SSTABLE_PATH.ends_with("/") {
        true => format!(
            "{}{}/{}{}",
            SSTABLE_PATH,
            String::from(level),
            new_table_name,
            SSTABLE_FILE_SUFFIX
        ),
        false => format!(
            "{}/{}/{}{}",
            SSTABLE_PATH,
            String::from(level),
            new_table_name,
            SSTABLE_FILE_SUFFIX
        ),
    }
}

/**
 * 异步打开文件
 */
pub async fn async_open_flie(path: &str) -> Result<tokio::fs::File> {
    if let false = async_is_file(path).await {
        tokio::fs::File::create(path).await?;
    }
    Ok(tokio::fs::OpenOptions::new()
        .append(true)
        .read(true)
        .open(path)
        .await?)
}

/**
 * 此时的path没有文件名称，只到文件所在的文件夹
 * 例如：/home/mobiusdb/data/test.txt
 *  path = /home/mobiusdb/data/
 */
pub async fn has_file_in_path(path: &str) -> Result<bool> {
    let mut entries = fs::read_dir(path).await?; // 异步读取目录条目                                            // 使用Stream的any方法，只要有一个条目就返回true，否则返回false
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            return Ok(true); // 发现文件，立即返回true
        }
    }
    Ok(false)
}

/**
 * 获取指定路径下的文件名称
 */
pub async fn get_files_name(path: &str) -> Result<Vec<String>> {
    let mut entries = fs::read_dir(path).await?;
    let mut resp = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_file() {
            let file_name = String::from(entry.file_name().to_str().unwrap());
            resp.push(file_name);
        }
    }
    Ok(resp)
}

/**
 * 异步打开文件，此时文件只读
 */
pub async fn async_open_only_read_flie(path: &str) -> tokio::fs::File {
    if let false = async_is_file(path).await {
        tokio::fs::File::create(path).await.unwrap();
    }
    let opts = tokio::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .unwrap();
    opts
}

pub async fn async_is_file(path: &str) -> bool {
    let r = match tokio::fs::metadata(path).await {
        Ok(metadata) => {
            if metadata.is_file() {
                println!("文件存在");
                true
            } else {
                println!("路径存在，但不是一个文件");
                false
            }
        }
        Err(_error) => {
            println!("文件不存在");
            false
        }
    };
    r
}

fn is_file(path: &str) -> bool {
    let r = match std::fs::metadata(path) {
        Ok(metadata) => {
            if metadata.is_file() {
                println!("文件存在");
                true
            } else {
                println!("路径存在，但不是一个文件");
                false
            }
        }
        Err(_error) => {
            println!("文件不存在");
            false
        }
    };
    r
}

pub fn open_file(path: &str) -> std::fs::File {
    if let false = is_file(path) {
        std::fs::File::create(path).unwrap();
    }
    let opts = std::fs::OpenOptions::new()
        .append(true)
        .read(true)
        .open(path)
        .unwrap();
    opts
}

#[cfg(test)]
mod tests {
    use crate::utils::file_utils::{get_files_name, get_sstable_file_name, has_file_in_path};

    use super::open_file;

    #[test]
    fn open_file_test() {
        let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/test.wal";
        let path = open_file(path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn async_open_file() {
        let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/";
        let path1 = "";
        let resp = has_file_in_path(path).await;
        assert!(resp.unwrap());
        let resp = has_file_in_path(path1).await;
        assert!(resp.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_files_name_test() {
        let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/";
        let resp = get_files_name(path).await;
        println!("resp: {:?}", resp);
    }

    #[test]
    fn get_sstable_file_name_test() {
        let path = "/Users/firoly/Documents/code/rust/mobiusdb/mobiusdb-lsm/tmp/l0/file0.sst";
        let resp = get_sstable_file_name(path);
        println!("resp: {:?}", resp);
    }
}
