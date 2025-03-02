use csv::{ReaderBuilder, WriterBuilder};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::Instant;

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
    let start = Instant::now();
    let input_path = "sql_tspider.csv";
    let success_path = "tspider_parser_success.csv";
    let error_path = "tspider_parser_error.csv";

    // 显示当前工作目录
    let current_dir = std::env::current_dir()?;
    println!("当前工作目录: {}", current_dir.display());

    println!("开始处理大型 SQL CSV 文件: {}", input_path);

    // 确认输入文件存在
    if !Path::new(input_path).exists() {
        return Err(format!("输入文件不存在: {}", input_path).into());
    }

    // 初始化CSV写入器
    let success_writer = create_output_writer(success_path)?;
    let error_writer = create_output_writer(error_path)?;

    // 处理文件
    let stats = process_sql_file(input_path, success_writer, error_writer)?;

    let elapsed = start.elapsed();

    // 打印最终统计
    println!("\n处理完成!");
    println!("总计处理 SQL 语句: {}", stats.total_count);
    println!(
        "成功解析语句数: {} ({}%)",
        stats.success_count,
        if stats.total_count > 0 {
            stats.success_count * 100 / stats.total_count
        } else {
            0
        }
    );
    println!(
        "解析失败语句数: {} ({}%)",
        stats.error_count,
        if stats.total_count > 0 {
            stats.error_count * 100 / stats.total_count
        } else {
            0
        }
    );
    println!("处理用时: {:.2} 秒", elapsed.as_secs_f64());
    println!(
        "平均处理速率: {:.2} 条/秒",
        if elapsed.as_secs_f64() > 0.0 {
            stats.total_count as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        }
    );

    Ok(())
}

fn create_output_writer(path: &str) -> Result<csv::Writer<File>, Box<dyn Error>> {
    let writer = WriterBuilder::new().has_headers(true).from_path(path)?;
    Ok(writer)
}

fn process_sql_file(
    input_path: &str,
    mut success_writer: csv::Writer<File>,
    mut error_writer: csv::Writer<File>,
) -> Result<ProcessStats, Box<dyn Error>> {
    // 打开输入文件
    let file = File::open(input_path)?;
    let _file_size = file.metadata()?.len();
    let reader = BufReader::with_capacity(1024 * 1024, file); // 1MB 缓冲区

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 写入头部
    success_writer.write_record(&["sql"])?;
    error_writer.write_record(&["sql"])?;

    // 初始化统计信息
    let mut stats = ProcessStats::new();
    let dialect = GenericDialect {}; // SQL 方言
    let start = Instant::now();
    let mut last_progress_time = Instant::now();

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
        match Parser::parse_sql(&dialect, sql.to_owned()) {
            Ok(_) => {
                // SQL 解析成功
                success_writer.write_record(&[sql])?;
                stats.success_count += 1;
            }
            Err(_) => {
                // SQL 解析失败
                error_writer.write_record(&[sql])?;
                stats.error_count += 1;
            }
        }

        // 定期刷新输出文件
        if stats.total_count % 10000 == 0 {
            success_writer.flush()?;
            error_writer.flush()?;
        }

        // 每5秒或每100,000条记录打印进度
        if stats.total_count % 100000 == 0 || last_progress_time.elapsed().as_secs() >= 5 {
            stats.print_progress(start.elapsed());
            last_progress_time = Instant::now();
        }
    }

    // 确保所有数据都写入磁盘
    success_writer.flush()?;
    error_writer.flush()?;

    Ok(stats)
}
