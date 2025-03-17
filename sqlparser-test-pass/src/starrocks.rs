use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::Path,
    time::Instant,
};

use csv::{ReaderBuilder, WriterBuilder};
use regex::Regex;
use sqlparser::parser::Parser;

use crate::{ProcessStats, SqlError};

// error_without_159_100968_5016750_5000448_rt

#[test]
pub fn test_doris_split() {
    for date in ["0306", "0307", "0308", "0309", "0310", "0311", "0312"] {
        test_doris_parse(date);
    }
}

pub fn test_doris_parse(date: &str) {
    // 用于记录解析成功的SQL数量
    let mut success_count = 0;
    let mut error_count = 0;
    let mut sql_error_list = HashSet::new();

    // 创建一个sqlparser的解析器，使用PostgresqlDialect
    let dialect = sqlparser::dialect::MySqlDialect {};

    // 读取sql.csv文件
    let path = &format!(
        "/Users/zww/workspace/codes/github/rust-practice/starrocks-parser/{}/starrocks_queryset_{}.csv",
        date,date
    );
    // 将解析失败的SQL写入到文件中
    let mut wtr = csv::Writer::from_path(&format!(
        "/Users/zww/workspace/codes/github/rust-practice/starrocks-parser/{}/sql_error.csv",
        date
    ))
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
