use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::BufReader,
    time::Instant,
};

use csv::{ReaderBuilder, WriterBuilder};
use sqlparser::parser::Parser;

use crate::{ProcessStats, SqlError};

// error_without_159_100968_5016750_5000448_rt
#[test]
pub fn test_doris_parse() {
    // 用于记录解析成功的SQL数量
    let mut success_count = 0;
    let mut error_count = 0;
    let mut sql_error_list = HashSet::new();

    // 创建一个sqlparser的解析器，使用PostgresqlDialect
    let dialect = sqlparser::dialect::PostgreSqlDialect {};

    // 读取sql.csv文件
    let path = "/Users/zww/workspace/codes/github/rust-practice/doris-parser/0311/sql.csv";
    // 将解析失败的SQL写入到文件中
    let mut wtr = csv::Writer::from_path(
        "/Users/zww/workspace/codes/github/rust-practice/doris-parser/0311/sql_error.csv",
    )
    .unwrap();
    let mut rdr = csv::Reader::from_path(path).unwrap();

    // 读取csv文件的每一行
    for result in rdr.records() {
        let record = result.unwrap();
        let sql = record.get(0).unwrap();

        // 解析sql
        // 尝试解析SQL
        match Parser::parse_sql(&dialect, &sql) {
            Ok(_) => {
                // SQL 解析成功
                success_count += 1;
            }
            Err(e) => {
                // SQL 解析失败 - 记录到内存
                let error = e.to_string();
                sql_error_list.insert(SqlError {
                    sql: sql.to_owned(),
                    error,
                });
                error_count += 1;
            }
        }
    }

    // 打印解析结果
    println!("success_count: {}", success_count);
    println!("error_count: {}", error_count);

    for error in sql_error_list {
        wtr.write_record(&[error.sql, error.error]).unwrap();
    }
}

#[test]
fn split_doris_query() {
    let mut sql_count = 0;
    let mut json_count = 0;
    // 读取csv文件
    let input_path =
        "/Users/zww/workspace/codes/github/rust-practice/doris-parser/0311/doris_queryset_0311.csv";
    let file = File::open(input_path).unwrap();
    let reader = BufReader::with_capacity(10 * 1024 * 1024, file); // 1MB 缓冲区

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 创建 CSV 写入器，将sql和json分开写入两个文件
    let output_path = "/Users/zww/workspace/codes/github/rust-practice/doris-parser/0311/sql.csv";
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(output_path)
        .unwrap();
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);

    let output_path_json =
        "/Users/zww/workspace/codes/github/rust-practice/doris-parser/0311/json.csv";
    let file_json = OpenOptions::new()
        .append(true)
        .create(true)
        .open(output_path_json)
        .unwrap();
    let mut writer_json = WriterBuilder::new()
        .has_headers(true)
        .from_writer(file_json);

    // 初始化统计信息
    let mut stats = ProcessStats::new();
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

        if record.is_empty() {
            continue;
        }

        let sql = record[0].to_string();
        stats.total_count += 1;

        if sql.starts_with("{") {
            json_count += 1;
            writer_json.write_record([&sql]).unwrap();
        } else {
            sql_count += 1;
            writer.write_record([&sql]).unwrap();
        }

        // 定期刷新输出文件
        if stats.total_count % 10000 == 0 {
            writer.flush().unwrap();
            writer_json.flush().unwrap();
        }
    }
    stats.print_progress(start.elapsed());
    last_progress_time = Instant::now();

    // 确保所有数据都写入磁盘
    writer.flush().unwrap();
    writer_json.flush().unwrap();

    // 打印统计信息
    println!("SQL Count: {}", sql_count);
    println!("JSON Count: {}", json_count);
}

#[test]
fn parse_single_sql() {
    let sql = r#"SELECT      
    COUNT(*) AS total_count FROM 
     _bklog_bkaudit_plugin_20221027_265f906a32.doris t1 
     WHERE       
         t1.system_id = 'bk-audit'       
         AND  t1.dtEventTime >= '2025-03-11 13:55:11' AND t1.dtEventTime <= '2025-03-12 13:55:11'       
         AND  1=1       
         AND t1.result_code = 0      
         AND t1.snapshot_action_info['name'] IS NOT NULL     
         AND NOT EXISTS (         
         SELECT 1          
         FROM _IAM_doris_test_2.doris t2         
         WHERE t1.username = t2.subject_user_id           
         AND t1.action_id = t2.policy_action_id     
    );
"#;
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("{:?}", ast);
}

#[test]
fn parse_single_sql_2() {
    let sql = r#"SELECT `sql`
        FROM 591_queryengine_log.hdfs
        WHERE thedate='20250228' and `storage` = "tspider"
        LIMIT 1000000 OFFSET 0"#;
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("{:?}", ast);
}
