use csv::{ReaderBuilder, WriterBuilder};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub(crate) mod remove_dup;

struct ProcessStats {
    total_count: usize,
    success_count: usize,
    error_count: usize,
}

impl ProcessStats {
    fn new() -> Self {
        Self {
            total_count: 0,
            success_count: 0,
            error_count: 0,
        }
    }

    fn print_progress(&self, elapsed: std::time::Duration) {
        let seconds = elapsed.as_secs_f64();
        let rate = if seconds > 0.0 {
            self.total_count as f64 / seconds
        } else {
            0.0
        };

        println!(
            "处理进度: 总计 {} 条 SQL, 成功: {} ({}%), 失败: {} ({}%), 速率: {:.2} 条/秒",
            self.total_count,
            self.success_count,
            if self.total_count > 0 {
                self.success_count * 100 / self.total_count
            } else {
                0
            },
            self.error_count,
            if self.total_count > 0 {
                self.error_count * 100 / self.total_count
            } else {
                0
            },
            rate
        );
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // 显示当前工作目录
    let current_dir = std::env::current_dir()?;
    println!("当前工作目录: {}", current_dir.display());

    // 获取 tspider-parser/0227/dataset 目录下所有的 *.csv 文件
    let dataset_dir = "tspider-parser/0226/dataset";
    let error_path = "tspider-parser/0226/output/error.csv";

    // 确保输出目录存在
    std::fs::create_dir_all("tspider-parser/0226/output")?;

    // 显示数据集目录
    println!("扫描数据集目录: {}", dataset_dir);

    // 获取所有CSV文件
    let mut csv_files = Vec::new();
    for entry in std::fs::read_dir(dataset_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("csv") {
            csv_files.push(path);
        }
    }

    println!("找到 {} 个CSV文件", csv_files.len());
    if csv_files.is_empty() {
        return Err("未找到CSV文件".into());
    }

    // 按文件名排序
    csv_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let global_start = Instant::now();

    // 初始化CSV写入器
    let mut stats = ProcessStats::new();

    for (index, input_path) in csv_files.iter().enumerate() {
        // 确认输入文件存在
        if !Path::new(input_path).exists() {
            return Err(format!("输入文件不存在: {:?}", input_path).into());
        }

        println!(
            "[{}/{}] 处理文件: {}",
            index + 1,
            csv_files.len(),
            input_path.file_name().unwrap_or_default().to_string_lossy()
        );

        // 初始化CSV写入器 (使用同一个错误输出文件)
        let error_writer = create_output_writer(&error_path)?;

        // 处理文件
        process_sql_file(input_path, error_writer, &mut stats)?;
    }

    // 打印总体统计信息
    stats.print_progress(global_start.elapsed());

    println!("总计时：{} second", global_start.elapsed().as_secs());
    println!("Done!");

    Ok(())
}

fn create_output_writer(path: &str) -> Result<csv::Writer<File>, Box<dyn Error>> {
    // use append mode
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(path)?;
    let writer = WriterBuilder::new().has_headers(true).from_writer(file);
    Ok(writer)
}

fn process_sql_file(
    input_path: &PathBuf,
    mut error_writer: csv::Writer<File>,
    stats: &mut ProcessStats,
) -> Result<(), Box<dyn Error>> {
    // 打开输入文件
    let file = File::open(input_path)?;
    let _file_size = file.metadata()?.len();
    let reader = BufReader::with_capacity(100 * 1024 * 1024, file); // 10MB 缓冲区

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 写入头部
    if stats.total_count == 0 {
        error_writer.write_record(&["sql", "error"])?;
    }

    // 初始化统计信息
    let dialect = MySqlDialect {}; // SQL 方言
    let start = Instant::now();
    let mut last_progress_time = Instant::now();
    let _file_name = input_path.file_name().unwrap_or_default().to_string_lossy();

    // 读取和处理每一条SQL记录
    for (index, result) in csv_reader.records().enumerate() {
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                eprintln!("CSV 读取错误 (行 {}): {}", index + 1, e);
                continue;
            }
        };

        if record.len() == 0 {
            continue;
        }

        let sql = &record[0];
        stats.total_count += 1;

        // 尝试解析SQL
        match Parser::parse_sql(&dialect, sql) {
            Ok(_) => {
                // SQL 解析成功
                // success_writer.write_record(&[sql])?;
                stats.success_count += 1;
            }
            Err(e) => {
                // SQL 解析失败
                error_writer.write_record(&[sql, &e.to_string()])?;
                stats.error_count += 1;
            }
        }

        // 定期刷新输出文件
        if stats.total_count % 10000 == 0 {
            error_writer.flush()?;
        }

        // 每5秒或每100,000条记录打印进度
        if stats.total_count % 100000 == 0 || last_progress_time.elapsed().as_secs() >= 5 {
            stats.print_progress(start.elapsed());
            last_progress_time = Instant::now();
        }
    }

    // 确保所有数据都写入磁盘
    error_writer.flush()?;

    Ok(())
}

#[test]
fn test_sql_parser() {
    let sql = r#"WITH login AS (
    SELECT 
        vopenid,
        dtEventTime,
        SUBSTR(dtEventTime, 1, 10) AS login_day
    FROM 
        100842_PlayerLogin_bklog
    WHERE 
        dtEventTime >= '2024-12-08 00:00:00' 
        AND dtEventTime <= '2024-12-08 14:01:00'
        AND dtEventTime >= '2024-12-09 10:00:00'
        AND ClientVersion <> '99.99.999.99'
), 
-- 登出记录
logout AS (
    SELECT 
        vopenid,
        dtEventTime,
        SUBSTR(dtEventTime, 1, 10) AS logout_day
    FROM 
        100842_PlayerLogout_bklog2
    WHERE 
        dtEventTime >= '2024-12-08 00:00:00' 
        AND dtEventTime <= '2024-12-08 14:01:00'
        AND dtEventTime >= '2024-12-09 10:00:00'
        AND ClientVersion <> '99.99.999.99'
), 
-- 合并登录和登出记录
active_users AS (
    SELECT 
        vopenid,
        active_day
    FROM (
        SELECT 
            vopenid, 
            login_day AS active_day 
        FROM 
            login
        UNION ALL
        SELECT 
            vopenid, 
            logout_day AS active_day 
        FROM 
            logout
    ) AS combined
    GROUP BY 
        vopenid, 
        active_day
)
SELECT 
    COUNT(DISTINCT vopenid) AS p_count 
FROM 
    active_users;"#;
    let dialect = MySqlDialect {};
    let result = Parser::parse_sql(&dialect, sql);
    println!("{:?}", result);
    assert_eq!(result.is_ok(), true);
}

#[allow(unused)]
fn v3_parse_error_remove_dup() {
    // read csv file queryset_v3_parse_error.csv
    // remove duplicate sql
    // write to new file queryset_v3_parse_error_no_dup.csv
    let input_path = "/Users/zww/workspace/codes/github/rust-practice/queryset_v3_parse_error.csv";
    let output_path =
        "/Users/zww/workspace/codes/github/rust-practice/queryset_v3_parse_error_no_dup.csv";

    // 打开输入文件
    let file = File::open(input_path).unwrap();
    let reader = BufReader::with_capacity(10 * 1024 * 1024, file); // 1MB 缓冲区

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 创建 CSV 写入器
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(output_path)
        .unwrap();
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

    // 初始化统计信息
    let mut stats = ProcessStats::new();
    let start = Instant::now();
    let mut last_progress_time = Instant::now();

    // 读取和处理每一条SQL记录
    let mut sql_set = std::collections::HashSet::new();

    for (index, result) in csv_reader.records().enumerate() {
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                eprintln!("CSV 读取错误 (行 {}): {}", index + 1, e);
                continue;
            }
        };

        if record.len() == 0 {
            continue;
        }

        let sql = record[2].to_string();
        stats.total_count += 1;

        if sql_set.contains(&sql) {
            continue;
        }

        sql_set.insert(sql.clone());
        writer.write_record(&[&sql]).unwrap();

        // 定期刷新输出文件
        if stats.total_count % 10000 == 0 {
            writer.flush().unwrap();
        }

        // 每5秒或每100,000条记录打印进度
        if stats.total_count % 100000 == 0 || last_progress_time.elapsed().as_secs() >= 5 {
            stats.print_progress(start.elapsed());
            last_progress_time = Instant::now();
        }
    }

    // 确保所有数据都写入磁盘
    writer.flush().unwrap();

    println!("Done!");
}

#[test]
fn test_v3_parse_error_remove_dup() {
    v3_parse_error_remove_dup();
}
