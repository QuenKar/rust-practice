use csv::{ReaderBuilder, WriterBuilder};
use regex::Regex;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::Path;
use std::time::Instant;

/// 从数据集中提取包含"minutex"的SQL语句，并进行去重
pub fn extract_minutex_sql() -> Result<(), Box<dyn Error>> {
    println!("开始提取包含'minutex'模式的SQL语句...");

    // 定义输入目录和输出文件
    let input_dir = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/test/dataset";
    let output_file =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/minutex_sql_dedup.csv";

    // 显示路径信息
    println!("数据集目录: {}", input_dir);
    println!("输出文件: {}", output_file);

    // 编译正则表达式用于匹配 "minuteX"，其中 X 是数字
    let regex = Regex::new(r"minute\d+")?;

    // 用于存储找到的SQL语句（去重）
    let mut sql_statements = HashSet::new();

    // 开始计时
    let start = Instant::now();

    // 获取目录下的所有CSV文件
    let entries = std::fs::read_dir(input_dir)?;
    let mut file_count = 0;
    let mut processed_count = 0;

    for entry_result in entries {
        let entry = entry_result?;
        let path = entry.path();

        // 只处理CSV文件
        if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("csv") {
            file_count += 1;
            println!(
                "处理文件 ({}/{}): {}",
                file_count,
                file_count,
                path.display()
            );

            // 打开文件
            let file = File::open(&path)?;
            let reader = BufReader::new(file);

            // 创建CSV读取器
            let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

            // 检查字段名是否包含"sql"
            let headers = csv_reader.headers()?;
            if let Some(sql_index) = headers.iter().position(|h| h == "sql") {
                // 读取每一行
                for record_result in csv_reader.records() {
                    let record = record_result?;
                    if record.len() > sql_index {
                        let sql = &record[sql_index];

                        // 检查SQL是否包含"minuteX"模式
                        if regex.is_match(sql) {
                            sql_statements.insert(sql.to_string());
                        }
                    }

                    processed_count += 1;
                    if processed_count % 100_000 == 0 {
                        println!(
                            "已处理 {} 条SQL记录，找到 {} 条包含minuteX的SQL",
                            processed_count,
                            sql_statements.len()
                        );
                    }
                }
            } else {
                println!("警告: 文件 {} 没有'sql'列", path.display());
                continue;
            }
        }
    }

    println!(
        "处理完成! 共处理了 {} 个文件, {} 条SQL记录",
        file_count, processed_count
    );
    println!(
        "找到 {} 条包含'minuteX'模式的唯一SQL语句",
        sql_statements.len()
    );

    // 将结果写入CSV文件
    let output = File::create(output_file)?;
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(output);

    // 写入标题行
    writer.write_record(&["sql"])?;

    // 写入所有SQL语句
    for sql in &sql_statements {
        writer.write_record(&[sql])?;
    }

    // 确保所有数据写入磁盘
    writer.flush()?;

    println!("结果已保存到: {}", output_file);
    println!("总耗时: {:.2}秒", start.elapsed().as_secs_f64());

    Ok(())
}

/// 提供命令行入口点
pub fn run_minutex_extraction() -> Result<(), Box<dyn Error>> {
    // 检查输出目录是否存在，如果不存在则创建
    let output_dir = Path::new("/Users/zww/workspace/codes/github/rust-practice/tspider-parser");
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)?;
    }

    // 执行提取
    extract_minutex_sql()
}

// 模块测试
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minutex_regex() {
        let regex = Regex::new(r"minute\d+").unwrap();

        assert!(regex.is_match("SELECT * FROM table WHERE time > minute7"));
        assert!(regex.is_match("minute60"));
        assert!(regex.is_match("this is minute123 test"));
        assert!(!regex.is_match("minutes"));
        assert!(!regex.is_match("minute"));
    }

    #[test]
    fn test_extract_minutex_sql() {
        extract_minutex_sql().unwrap();
    }
}
