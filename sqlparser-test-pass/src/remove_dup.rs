use csv::{ReaderBuilder, WriterBuilder};
use std::error::Error;
use std::fs::OpenOptions;
use std::time::Instant;
use std::{fs::File, io::BufReader};

#[allow(unused)]
fn deduplicate_error_csv(input_path: &str, output_path: &str) -> Result<(), Box<dyn Error>> {
    println!("开始去重处理: {} -> {}", input_path, output_path);

    // 打开输入文件
    let file = File::open(input_path)?;
    let reader = BufReader::with_capacity(10 * 1024 * 1024, file); // 10MB 缓冲区

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 创建 CSV 写入器
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true) // 覆盖而非追加
        .open(output_path)?;
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

    // 用于去重的集合
    let mut unique_sqls = std::collections::HashSet::new();

    // 统计信息
    let mut total_count = 0;
    let mut unique_count = 0;
    let mut duplicate_count = 0;

    let start = Instant::now();

    // 写入头部
    writer.write_record(["sql"])?;

    // 读取和处理每一条SQL记录
    for (index, result) in csv_reader.records().enumerate() {
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                eprintln!("CSV 读取错误 (行 {}): {}", index + 1, e);
                continue;
            }
        };

        if record.len() < 2 {
            eprintln!("CSV 记录格式错误 (行 {}): 列数不足", index + 1);
            continue;
        }

        let sql = record[0].to_string();
        let error = record[1].to_string();

        total_count += 1;

        // 如果这个SQL已经存在，跳过
        if unique_sqls.contains(&sql) {
            duplicate_count += 1;
            continue;
        }

        // 添加到去重集合并写入输出文件
        unique_sqls.insert(sql.clone());
        writer.write_record([&sql])?;
        unique_count += 1;

        // 每10,000条记录刷新一次输出文件并显示进度
        if total_count % 10000 == 0 {
            writer.flush()?;
            println!(
                "处理进度: 总计 {} 条记录, 唯一 {} 条, 重复 {} 条, 处理率: {:.2}%",
                total_count,
                unique_count,
                duplicate_count,
                (total_count as f64 / unique_sqls.len() as f64) * 100.0
            );
        }
    }

    // 确保所有数据都写入磁盘
    writer.flush()?;

    let elapsed = start.elapsed();
    println!(
        "去重完成: 总计 {} 条记录, 唯一 {} 条, 重复 {} 条, 处理率: {:.2}%",
        total_count,
        unique_count,
        duplicate_count,
        (unique_count as f64 / total_count as f64) * 100.0
    );
    println!("处理用时: {:.2} 秒", elapsed.as_secs_f64());

    Ok(())
}

#[allow(unused)]
fn deduplicate_error_csv_7day(input_path: &str, output_path: &str) -> Result<(), Box<dyn Error>> {
    println!("开始去重处理: {} -> {}", input_path, output_path);

    // 打开输入文件
    let file = File::open(input_path)?;
    let reader = BufReader::with_capacity(10 * 1024 * 1024, file); // 10MB 缓冲区

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 创建 CSV 写入器
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true) // 覆盖而非追加
        .open(output_path)?;
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

    // 用于去重的集合
    let mut unique_sqls = std::collections::HashSet::new();

    // 统计信息
    let mut total_count = 0;
    let mut unique_count = 0;
    let mut duplicate_count = 0;

    let start = Instant::now();

    // 写入头部
    writer.write_record(["sql"])?;

    // 读取和处理每一条SQL记录
    for (index, result) in csv_reader.records().enumerate() {
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                eprintln!("CSV 读取错误 (行 {}): {}", index + 1, e);
                continue;
            }
        };

        let sql = record[0].to_string();

        total_count += 1;

        // 如果这个SQL已经存在，跳过
        if unique_sqls.contains(&sql) {
            duplicate_count += 1;
            continue;
        }

        // 添加到去重集合并写入输出文件
        unique_sqls.insert(sql.clone());
        writer.write_record([&sql])?;
        unique_count += 1;

        // 每10,000条记录刷新一次输出文件并显示进度
        if total_count % 10000 == 0 {
            writer.flush()?;
            println!(
                "处理进度: 总计 {} 条记录, 唯一 {} 条, 重复 {} 条, 处理率: {:.2}%",
                total_count,
                unique_count,
                duplicate_count,
                (total_count as f64 / unique_sqls.len() as f64) * 100.0
            );
        }
    }

    // 确保所有数据都写入磁盘
    writer.flush()?;

    let elapsed = start.elapsed();
    println!(
        "去重完成: 总计 {} 条记录, 唯一 {} 条, 重复 {} 条, 处理率: {:.2}%",
        total_count,
        unique_count,
        duplicate_count,
        (unique_count as f64 / total_count as f64) * 100.0
    );
    println!("处理用时: {:.2} 秒", elapsed.as_secs_f64());

    Ok(())
}

#[allow(unused)]
fn dedup_time_diff_sql(input_path: &str, output_path: &str) {
    // 过滤掉仅查询时间不同，而其他字段都一样的SQL
    
}

#[test]
fn test_deduplicate_error_csv_0227() -> Result<(), Box<dyn Error>> {
    let input_path =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0227/output/error.csv";
    let output_path = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0227/output/error-dedup.csv";
    deduplicate_error_csv(input_path, output_path)?;
    Ok(())
}

#[test]
fn test_deduplicate_error_csv_0226() -> Result<(), Box<dyn Error>> {
    let input_path =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0226/output/error.csv";
    let output_path = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0226/output/error-dedup.csv";
    deduplicate_error_csv(input_path, output_path)?;
    Ok(())
}

#[test]
fn test_deduplicate_error_csv_0225() -> Result<(), Box<dyn Error>> {
    let input_path =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0225/output/error.csv";
    let output_path = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0225/output/error-dedup.csv";
    deduplicate_error_csv(input_path, output_path)?;
    Ok(())
}

#[test]
fn test_deduplicate_error_csv_0224() -> Result<(), Box<dyn Error>> {
    let input_path =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0224/output/error.csv";
    let output_path = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0224/output/error-dedup.csv";
    deduplicate_error_csv(input_path, output_path)?;
    Ok(())
}

#[test]
fn test_deduplicate_error_csv_0223() -> Result<(), Box<dyn Error>> {
    let input_path =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0223/output/error.csv";
    let output_path = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0223/output/error-dedup.csv";
    deduplicate_error_csv(input_path, output_path)?;
    Ok(())
}

#[test]
fn test_deduplicate_error_csv_0222() -> Result<(), Box<dyn Error>> {
    let input_path =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0222/output/error.csv";
    let output_path = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/0222/output/error-dedup.csv";
    deduplicate_error_csv(input_path, output_path)?;
    Ok(())
}

#[test]
fn test_deduplicate_error_csv_7_days() -> Result<(), Box<dyn Error>> {
    let input_path = "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/error-sql.csv";
    let output_path =
        "/Users/zww/workspace/codes/github/rust-practice/tspider-parser/error-sql-dedup.csv";
    deduplicate_error_csv_7day(input_path, output_path)?;
    Ok(())
}
