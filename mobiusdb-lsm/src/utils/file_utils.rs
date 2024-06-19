pub async fn async_open_flie(path: &str) -> tokio::fs::File {
    if let false = async_is_file(path).await {
        tokio::fs::File::create(path).await.unwrap();
    }
    let opts = tokio::fs::OpenOptions::new()
        .append(true)
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
