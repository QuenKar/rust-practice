use csv::{ReaderBuilder, WriterBuilder};
use futures::{stream, StreamExt};
use sqlparser::dialect::{HiveDialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) mod minutex_sql_dedup;
pub(crate) mod remove_dup;

#[allow(unused)]
pub mod doris;

#[allow(unused)]
pub mod starrocks;

// 记录解析失败的SQL及错误信息
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SqlError {
    sql: String,
    error: String,
}

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

fn main() {}

// fn main() {
//     minutex_sql_dedup::extract_minutex_sql().unwrap();
// }

// fn main() -> Result<(), Box<dyn Error>> {
//     // 显示当前工作目录
//     let current_dir = std::env::current_dir()?;
//     println!("当前工作目录: {}", current_dir.display());

//     // 获取指定目录下所有的 *.csv 文件
//     let dataset_dir = "doris-parser/0312";
//     let error_path = "doris-parser/0312/error_without_159_100968_5016750_5000448_rt.csv";

//     // 确保输出目录存在
//     std::fs::create_dir_all("doris-parser/0312")?;

//     // 显示数据集目录
//     println!("扫描数据集目录: {}", dataset_dir);

//     // 获取所有CSV文件
//     let mut csv_files = Vec::new();
//     for entry in std::fs::read_dir(dataset_dir)? {
//         let entry = entry?;
//         let path = entry.path();
//         if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("csv") {
//             csv_files.push(path);
//         }
//     }

//     println!("找到 {} 个CSV文件", csv_files.len());
//     if csv_files.is_empty() {
//         return Err("未找到CSV文件".into());
//     }

//     // 按文件名排序
//     csv_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

//     let global_start = Instant::now();

//     // 使用 tokio 运行时进行异步处理
//     let runtime = tokio::runtime::Builder::new_multi_thread()
//         .worker_threads(num_cpus::get())
//         .enable_all()
//         .build()?;

//     // 在运行时中执行异步任务
//     let (total_stats, sql_error_list) = runtime.block_on(async {
//         // 共享的统计数据和错误列表
//         let stats = Arc::new(Mutex::new(ProcessStats::new()));
//         let sql_error_list = Arc::new(Mutex::new(HashSet::<SqlError>::new()));
//         let csv_files_arc = Arc::new(csv_files.clone());

//         // 并发限制
//         let concurrency_limit = 6; // 调整为合适的并发数

//         // 处理任务
//         let tasks = stream::iter(csv_files.clone().into_iter().enumerate())
//             .map(|(index, input_path)| {
//                 let stats = Arc::clone(&stats);
//                 let errors = Arc::clone(&sql_error_list);
//                 let input_path = input_path.clone();
//                 let csv_files = Arc::clone(&csv_files_arc);
//                 async move {
//                     if !Path::new(&input_path).exists() {
//                         eprintln!("输入文件不存在: {:?}", input_path);
//                         return;
//                     }

//                     let binding = input_path.clone();
//                     let file_name = binding.file_name().unwrap_or_default().to_string_lossy();

//                     println!(
//                         "[{}/{}] 开始处理文件: {}",
//                         index + 1,
//                         csv_files.len(),
//                         file_name
//                     );

//                     // 使用 spawn_blocking 处理 CPU 密集型任务
//                     let result = tokio::task::spawn_blocking(move || {
//                         process_sql_file_async(input_path.clone(), stats, errors)
//                     })
//                     .await;

//                     match result {
//                         Ok(Ok(_)) => println!(
//                             "[{}/{}] 文件处理完成: {}",
//                             index + 1,
//                             csv_files.len(),
//                             file_name
//                         ),
//                         Ok(Err(e)) => eprintln!("处理文件 {} 时出错: {}", file_name, e),
//                         Err(e) => eprintln!("处理文件 {} 时任务失败: {}", file_name, e),
//                     }
//                 }
//             })
//             .buffer_unordered(concurrency_limit)
//             .collect::<Vec<_>>();

//         // 等待所有任务完成
//         tasks.await;

//         // 返回最终的统计数据和错误列表
//         let final_stats = stats.lock().unwrap().clone();
//         let errors = sql_error_list.lock().unwrap().clone();
//         (final_stats, errors)
//     });

//     // 打印总体统计信息
//     total_stats.print_progress(global_start.elapsed());

//     // 写入错误到CSV文件
//     let error_vec: Vec<SqlError> = sql_error_list.into_iter().collect();
//     write_errors_to_csv(error_path, &error_vec)?;

//     println!("SQL 解析错误已写入: {}", error_path);
//     println!("总计时：{} 秒", global_start.elapsed().as_secs());
//     println!("Done!");

//     Ok(())
// }

// 处理单个SQL文件（用于异步执行）
fn process_sql_file_async(
    input_path: PathBuf,
    stats: Arc<Mutex<ProcessStats>>,
    sql_error_list: Arc<Mutex<HashSet<SqlError>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 打开输入文件
    let file = File::open(input_path.clone())?;
    let file_size = file.metadata()?.len();
    let reader = BufReader::with_capacity(10 * 1024 * 1024, file);

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 初始化统计信息
    let dialect = PostgreSqlDialect {};
    let start = Instant::now();
    let mut last_progress_time = Instant::now();
    let file_name = input_path.file_name().unwrap_or_default().to_string_lossy();

    // 当前文件的统计信息
    let mut file_stats = ProcessStats::new();

    println!(
        "开始处理文件: {} (大小: {:.2} MB)",
        file_name,
        file_size as f64 / 1024.0 / 1024.0
    );

    // 读取和处理每一条SQL记录
    for (index, result) in csv_reader.records().enumerate() {
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                eprintln!(
                    "CSV 读取错误 (文件: {}, 行 {}): {}",
                    file_name,
                    index + 1,
                    e
                );
                continue;
            }
        };

        if record.is_empty() {
            continue;
        }

        let sql = record[0].to_string();

        file_stats.total_count += 1;

        // 尝试解析SQL
        match Parser::parse_sql(&dialect, &sql) {
            Ok(_) => {
                // SQL 解析成功
                file_stats.success_count += 1;
            }
            Err(e) => {
                // SQL 解析失败 - 记录到内存
                let error = e.to_string();
                sql_error_list
                    .lock()
                    .unwrap()
                    .insert(SqlError { sql, error });
                file_stats.error_count += 1;
            }
        }

        // 每5秒或每100,000条记录打印进度
        if file_stats.total_count % 100000 == 0 || last_progress_time.elapsed().as_secs() >= 5 {
            println!("文件 {}: ", file_name);
            file_stats.print_progress(start.elapsed());
            last_progress_time = Instant::now();
        }
    }

    // 更新全局统计信息
    let mut stats_guard = stats.lock().unwrap();
    stats_guard.total_count += file_stats.total_count;
    stats_guard.success_count += file_stats.success_count;
    stats_guard.error_count += file_stats.error_count;

    println!(
        "完成处理文件: {}, 用时: {:.2}秒",
        file_name,
        start.elapsed().as_secs_f64()
    );
    file_stats.print_progress(start.elapsed());

    Ok(())
}

// 将收集到的错误写入CSV文件
fn write_errors_to_csv(path: &str, errors: &[SqlError]) -> Result<(), Box<dyn Error>> {
    println!("开始写入 {} 条解析错误到 {}", errors.len(), path);

    // 创建CSV写入器
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true) // 覆盖而非追加
        .open(path)?;
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

    // 写入头部
    writer.write_record(["sql", "error"])?;

    // 写入所有错误
    for error in errors {
        writer.write_record([&error.sql, &error.error])?;
    }

    // 刷新数据
    writer.flush()?;

    println!("成功写入 {} 条错误记录", errors.len());
    Ok(())
}

// 为了 ProcessStats 添加 Clone trait 实现
impl Clone for ProcessStats {
    fn clone(&self) -> Self {
        Self {
            total_count: self.total_count,
            success_count: self.success_count,
            error_count: self.error_count,
        }
    }
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

        if record.is_empty() {
            continue;
        }

        let sql = record[2].to_string();
        stats.total_count += 1;

        if sql_set.contains(&sql) {
            continue;
        }

        sql_set.insert(sql.clone());
        writer.write_record([&sql]).unwrap();

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

#[test]
fn test_tspider_sql_parser() {
    let sql = r#""#;
    let dialect = MySqlDialect {};
    let result = Parser::parse_sql(&dialect, sql);
    println!("{:?}", result);
    assert_eq!(result.is_ok(), true);
}

#[test]
fn test_doris_sql_parser() {
    let sql = r#""#;
    let dialect = HiveDialect {};
    let result = Parser::parse_sql(&dialect, sql);
    println!("{:?}", result);
    assert_eq!(result.is_ok(), true);
}
