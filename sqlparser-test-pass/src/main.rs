use csv::{ReaderBuilder, WriterBuilder};
use futures::{stream, StreamExt};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) mod remove_dup;

// 记录解析失败的SQL及错误信息
#[derive(Debug, Clone)]
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

fn main() -> Result<(), Box<dyn Error>> {
    // 显示当前工作目录
    let current_dir = std::env::current_dir()?;
    println!("当前工作目录: {}", current_dir.display());

    // 获取 tspider-parser 目录下所有的 *.csv 文件
    let dataset_dir = "tspider-parser/0222/dataset";
    let error_path = "tspider-parser/0222/output/error.csv";

    // 确保输出目录存在
    std::fs::create_dir_all("tspider-parser/0222/output")?;

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

    // 使用 tokio 运行时进行异步处理
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .enable_all()
        .build()?;

    // 在运行时中执行异步任务
    let (total_stats, sql_error_list) = runtime.block_on(async {
        // 共享的统计数据和错误列表
        let stats = Arc::new(Mutex::new(ProcessStats::new()));
        let sql_error_list = Arc::new(Mutex::new(Vec::<SqlError>::new()));
        let csv_files_arc = Arc::new(csv_files.clone());

        // 并发限制
        let concurrency_limit = 6; // 调整为合适的并发数

        // 处理任务
        let tasks = stream::iter(csv_files.clone().into_iter().enumerate())
            .map(|(index, input_path)| {
                let stats = Arc::clone(&stats);
                let errors = Arc::clone(&sql_error_list);
                let input_path = input_path.clone();
                let csv_files = Arc::clone(&csv_files_arc);
                async move {
                    if !Path::new(&input_path).exists() {
                        eprintln!("输入文件不存在: {:?}", input_path);
                        return;
                    }

                    let binding = input_path.clone();
                    let file_name = binding.file_name().unwrap_or_default().to_string_lossy();

                    println!(
                        "[{}/{}] 开始处理文件: {}",
                        index + 1,
                        csv_files.len(),
                        file_name
                    );

                    // 使用 spawn_blocking 处理 CPU 密集型任务
                    let result = tokio::task::spawn_blocking(move || {
                        process_sql_file_async(input_path.clone(), stats, errors)
                    })
                    .await;

                    match result {
                        Ok(Ok(_)) => println!(
                            "[{}/{}] 文件处理完成: {}",
                            index + 1,
                            csv_files.len(),
                            file_name
                        ),
                        Ok(Err(e)) => eprintln!("处理文件 {} 时出错: {}", file_name, e),
                        Err(e) => eprintln!("处理文件 {} 时任务失败: {}", file_name, e),
                    }
                }
            })
            .buffer_unordered(concurrency_limit)
            .collect::<Vec<_>>();

        // 等待所有任务完成
        tasks.await;

        // 返回最终的统计数据和错误列表
        let final_stats = stats.lock().unwrap().clone();
        let errors = sql_error_list.lock().unwrap().clone();
        (final_stats, errors)
    });

    // 打印总体统计信息
    total_stats.print_progress(global_start.elapsed());

    // 写入错误到CSV文件
    write_errors_to_csv(error_path, &sql_error_list)?;

    println!("SQL 解析错误已写入: {}", error_path);
    println!("总计时：{} 秒", global_start.elapsed().as_secs());
    println!("Done!");

    Ok(())
}

// 处理单个SQL文件（用于异步执行）
fn process_sql_file_async(
    input_path: PathBuf,
    stats: Arc<Mutex<ProcessStats>>,
    sql_error_list: Arc<Mutex<Vec<SqlError>>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 打开输入文件
    let file = File::open(input_path.clone())?;
    let file_size = file.metadata()?.len();
    let reader = BufReader::with_capacity(10 * 1024 * 1024, file);

    // 创建 CSV 读取器
    let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    // 初始化统计信息
    let dialect = MySqlDialect {};
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
                sql_error_list.lock().unwrap().push(SqlError { sql, error });
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
fn test_sql_parser() {
    let sql = r#"WITH newuser AS (
    SELECT
        reg_day,
        vroleid,
        vopenid,
        dtEventTime AS createTime,
        vrolename
    FROM (
        SELECT
            SUBSTRING(dtEventTime, 1, 10) AS reg_day,
            *,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid
                ORDER BY dtEventTime DESC -- 最后一条注册记录
            ) AS t
        FROM
            100842_PlayerRegister_balance
        WHERE
            dteventtime >= '2025-02-28 10:00:00'
            -- AND dtEventTime >= '2025-02-28 00:00:00' AND dtEventTime <= '2025-02-28 23:59:59'
    )
    WHERE t = 1
),
LastLogin AS (
    SELECT
        *
    FROM (
        SELECT
            vroleid,
            vopenid,
            SUBSTRING(dtEventTime, 1, 10) AS ldate,
            dtEventTime AS last_time,
            'login' AS status,
            ilevel,
            vrolename,
            iviplevel,
            irolece,
            itotalrecharge,
            iseasonid,
            iseasonlevel,
            playerfriendsnum AS friend_info,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid, SUBSTRING(dtEventTime, 1, 10)
                ORDER BY dtEventTime DESC
            ) AS t
        FROM
            100842_PlayerLogin_balance
    )
    WHERE t = 1
),
LastLogout AS (
    SELECT
        *
    FROM (
        SELECT
            vroleid,
            vopenid,
            SUBSTRING(dtEventTime, 1, 10) AS ldate,
            dtEventTime AS last_time,
            'logout' AS status,
            ilevel,
            vrolename,
            iviplevel,
            irolece,
            itotalrecharge,
            iseasonid,
            iseasonlevel,
            playerfriendsnum AS friend_info,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid, SUBSTRING(dtEventTime, 1, 10)
                ORDER BY dtEventTime DESC
            ) AS t
        FROM
            100842_PlayerLogout_balance
    )
    WHERE t = 1
),
Combined AS (
    SELECT
        *
    FROM LastLogin
    UNION ALL
    SELECT
        *
    FROM LastLogout
),
FinalStatus AS (
    SELECT
        vroleid,
        vopenid,
        ldate,
        vrolename,
        last_time,
        status,
        ilevel,
        iviplevel,
        irolece,
        itotalrecharge,
        iseasonid,
        iseasonlevel,
        friend_info,
        ROW_NUMBER() OVER (
            PARTITION BY vroleid, ldate
            ORDER BY last_time DESC
        ) AS rn
    FROM Combined
),
role_base_data AS (
    SELECT
        vroleid,
        vopenid,
        vrolename,
        ldate AS rbd_date,
        last_time,
        status,
        ilevel,
        iviplevel,
        irolece,
        itotalrecharge,
        iseasonid,
        iseasonlevel,
        friend_info
    FROM FinalStatus
    WHERE rn = 1
    ORDER BY vroleid, ldate
),
story AS (
    SELECT * FROM (
        -- 玩家每日推图主线保存点
        SELECT
            SUBSTRING(dtEventTime, 1, 10) AS st_day,
            dtEventTime AS event_time,
            vroleid,
            dungeonid,
            savepointname,
            savepointvalue,
            currenthp,
            currentmp,
            currenthppercent,
            currentmppercent,
            isrevivefromsavepoint AS tisrevivefromsavepoint,
            CASE
                WHEN DungeonID < 50000 THEN DungeonID * 100 + SavePointValue
                WHEN DungeonID = 2020866 THEN FLOOR(DungeonID / 10) * 10 + SavePointValue
                ELSE DungeonID
            END AS stepname,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid, SUBSTRING(dtEventTime, 1, 10)
                ORDER BY dtEventTime DESC
            ) AS t
        FROM
            100842_StoryStatusSavePointFlow_balance
        WHERE dteventtime >= '2025-02-28 10:00:00'
    ) a
    WHERE t = 1
),
story_all_time AS (
    -- 玩家终身推图主线
    SELECT * FROM (
        SELECT
            SUBSTRING(dtEventTime, 1, 10) AS salldate,
            dteventtime,
            vroleid,
            dungeonid,
            SavePointValue,
            SavePointName,
            IsReviveFromSavePoint AS aisrevivefromsavepoint,
            CASE
                WHEN DungeonID < 50000 THEN DungeonID * 100 + SavePointValue
                WHEN DungeonID = 2020866 THEN FLOOR(DungeonID / 10) * 10 + SavePointValue
                ELSE DungeonID
            END AS stepname,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid
                ORDER BY dteventtime DESC
            ) AS t
        FROM
            100842_StoryStatusSavePointFlow_balance
        WHERE dteventtime >= '2025-02-28 10:00:00'
    )
    WHERE t = 1
),
OnlineStats AS (
    SELECT
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        COALESCE(SUM(onlinetime), 0) AS daily_online_time,
        COALESCE(SUM(SUM(onlinetime)) OVER (PARTITION BY vroleid), 0) AS lifetime_online_time
    FROM
        100842_PlayerLogout_balance
    WHERE
        vHeaderID <> 'DEFAULT'
        AND dteventtime >= '2025-02-28 10:00:00'
    GROUP BY
        vroleid, SUBSTR(dteventtime, 1, 10)
),
ClassifiedStats AS (
    SELECT
        vroleid,
        event_date,
        daily_online_time,
        lifetime_online_time,
        ROUND(COALESCE(daily_online_time, 0) / 60, 2) AS daily_online_time_minutes,
        ROUND(COALESCE(lifetime_online_time, 0) / 60, 2) AS lifetime_online_time_minutes
        -- CASE
        --     WHEN COALESCE(daily_online_time, 0) < 60 THEN CONCAT(CAST(FLOOR(COALESCE(daily_online_time, 0) / 60) AS VARCHAR), '~', CAST(FLOOR(COALESCE(daily_online_time, 0) / 60) + 1 AS VARCHAR), '分钟')
        --     WHEN COALESCE(daily_online_time, 0) < 1800 THEN CONCAT(CAST(FLOOR(COALESCE(daily_online_time, 0) / 60) AS VARCHAR), '分钟')
        --     ELSE CONCAT(CAST(FLOOR(COALESCE(daily_online_time, 0) / 900) * 15 AS VARCHAR), '~', CAST(FLOOR(COALESCE(daily_online_time, 0) / 900) * 15 + 15 AS VARCHAR), '分钟')
        -- END AS daily_online_time_category,
        -- CASE
        --     WHEN COALESCE(lifetime_online_time, 0) < 60 THEN CONCAT(CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 60) AS VARCHAR), '~', CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 60) + 1 AS VARCHAR), '分钟')
        --     WHEN COALESCE(lifetime_online_time, 0) < 1800 THEN CONCAT(CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 60) AS VARCHAR), '分钟')
        --     ELSE CONCAT(CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 900) * 15 AS VARCHAR), '~', CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 900) * 15 + 15 AS VARCHAR), '分钟')
        -- END AS lifetime_online_time_category
    FROM OnlineStats
),
pvp_info AS (
    SELECT
        COUNT(*) AS pvp_times,
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        SUM(COUNT(*)) OVER (PARTITION BY vroleid) AS alltime_pvp_times
    FROM
        100842_ArenaBegin_balance
    WHERE fighttype = 51
    GROUP BY vroleid, SUBSTR(dteventtime, 1, 10)
),
pv3_info AS (
    SELECT
        COUNT(*) AS pv3_times,
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        SUM(COUNT(*)) OVER (PARTITION BY vroleid) AS alltime_pv3_times
    FROM
        100842_TeamBattleBegin_balance
    GROUP BY vroleid, SUBSTR(dteventtime, 1, 10)
),
pve_info AS (
    SELECT
        COUNT(*) AS pve_times,
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        SUM(COUNT(*)) OVER (PARTITION BY vroleid) AS alltime_pve_times
    FROM
        100842_PVEBossEnd_balance
    GROUP BY vroleid, SUBSTR(dteventtime, 1, 10)
),
arena_begin AS (
    SELECT
        vopenid,
        vroleid,
        vrolename,
        dteventtime,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        fighttype,
        gametraceid,
        matchtime,
        phase,
        subphase,
        rivalisai
    FROM
        100842_ArenaBegin_balance
    WHERE
        fighttype = 51 -- 仅筛选 fighttype = 51 的记录
        AND dteventtime >= '2025-02-28 10:00:00'
        -- AND rivalisai <> 1
),
arena_end AS (
    SELECT
        dteventtime,
        vopenid,
        vroleid,
        vrolename,
        fighttype,
        gametraceid,
        fightresulttype,
        resultdetail,
        phase,
        subphase,
        score,
        changescore,
        hidderscore,
        changehidderscore
    FROM
        100842_ArenaEnd_balance
    WHERE
        fighttype = 51 -- 仅筛选 fighttype = 51 的记录
        AND dteventtime >= '2025-02-28 10:00:00'
),
game_result AS (
    -- 详细数据
    SELECT
        ab.vopenid,
        ab.vroleid,
        ab.vrolename AS `昵称`,
        ab.event_date AS `日期`,
        ab.fighttype AS `战斗类型`,
        CAST(ab.gametraceid AS VARCHAR) AS gametraceid,
        -- CAST(ab.gametraceid AS VARCHAR) AS cast_gametraceid,
        CAST(ae.gametraceid AS VARCHAR) AS end_gameid,
        ab.matchtime AS `匹配时长`,
        ab.phase AS `大段（匹配前）`,
        ab.subphase AS `小段（匹配前）`,
        ae.fightresulttype AS `结果`,
        CASE
            WHEN ae.fightresulttype = 0 THEN 'win' -- 玩家胜利
            WHEN ae.fightresulttype = 1 THEN 'draw' -- 平局
            WHEN ae.fightresulttype = 2 THEN 'lose' -- 无效结果
            WHEN ae.fightresulttype IS NULL THEN 'in game'
            ELSE 'wrong'
        END AS `战斗结果`,
        ae.resultdetail AS `对局详情`,
        ae.phase AS `大段（匹配后）`,
        ae.subphase AS `小段（匹配后）`,
        ab.dteventtime AS `开始战斗时间`,
        ae.dteventtime,
        ae.score AS `结算分数`,
        ae.changescore AS `分数变化`,
        ae.hidderscore AS `隐藏分`,
        ae.changehidderscore AS `变化的隐藏分`,
        rivalisai AS `是否机器人`
    FROM
        arena_begin ab
    LEFT JOIN arena_end ae ON ae.vopenid = ab.vopenid AND ae.gametraceid = ab.gametraceid
),
total_games AS (
    SELECT
        vopenid,
        COUNT(DISTINCT gametraceid) AS total_games,
        COUNT(IF(`战斗结果` = 'win', gametraceid, NULL)) AS total_win,
        COUNT(IF(`战斗结果` = 'lose', gametraceid, NULL)) AS total_lose,
        COUNT(IF(`战斗结果` = 'draw', gametraceid, NULL)) AS total_draw,
        COUNT(IF(`战斗结果` = 'wrong', gametraceid, NULL)) AS total_wrong
    FROM game_result
    GROUP BY vopenid
),
total_real_games AS (
    SELECT
        vopenid,
        COUNT(DISTINCT gametraceid) AS real_games
    FROM game_result
    WHERE `是否机器人` <> 1
    GROUP BY vopenid
),
final_score AS (
    SELECT * FROM (
        SELECT
            vopenid,
            `结算分数`,
            ROW_NUMBER() OVER (PARTITION BY vopenid ORDER BY dtEventTime DESC) AS t
        FROM game_result
    ) a
    WHERE t = 1
),
logout AS (
    SELECT * FROM (
        SELECT
            vopenid,
            vrolename,
            ROW_NUMBER() OVER (PARTITION BY vopenid ORDER BY dtEventTime DESC) AS t
        FROM 100842_PlayerLogout_balance
    ) a
    WHERE t = 1
),
pvp_data AS (
    -- 玩家最终段位，胜率等信息
    SELECT * FROM (
        SELECT
            gs.vopenid,
            gs.vroleid,
            lg.vrolename AS `昵称`,
            gs.`结算分数`,
            gs.`日期`,
            gs.`战斗类型`,
            gs.gametraceid,
            ROUND(gs.`匹配时长` / 1000, 2) AS `匹配时长（秒）`,
            gs.`大段（匹配前）`,
            gs.`小段（匹配前）`,
            gs.`大段（匹配后）`,
            gs.`小段（匹配后）`,
            total_games AS `总场次`,
            total_win AS `总胜场`,
            total_lose AS `总败场`,
            total_draw AS `总平局`,
            total_wrong AS `总错误对局`,
            COALESCE(real_games, 0) AS `真人对局数`,
            FORMAT('%s%%', ROUND(total_win / total_games * 100, 2)) AS `玩家胜率`,
            ROW_NUMBER() OVER (PARTITION BY gs.vopenid ORDER BY dtEventTime DESC) AS row_r
        FROM
            game_result gs
        LEFT JOIN total_games tg ON tg.vopenid = gs.vopenid
        LEFT JOIN total_real_games trg ON trg.vopenid = gs.vopenid
        LEFT JOIN final_score fs ON fs.vopenid = gs.vopenid
        LEFT JOIN logout lg ON lg.vopenid = gs.vopenid
    ) a
    WHERE row_r = 1
),
login_device AS (
    -- 登陆过的平台
    SELECT
        vopenid,
        CASE
            WHEN COUNT(DISTINCT PlatID) = 1 AND MAX(PlatID) IN (0, 1) THEN '仅手机' -- 仅在手机平台（iOS或安卓）登录
            WHEN COUNT(DISTINCT PlatID) = 1 AND MAX(PlatID) = 2 THEN '仅PC' -- 仅在PC平台登录
            WHEN COUNT(DISTINCT PlatID) > 1 THEN '双栖' -- 在手机和PC平台都登录过
            ELSE '未知' -- 其他情况
        END AS LoginType
    FROM
        100842_PlayerLogin_balance
    GROUP BY vopenid
),
Rank_data AS (
    SELECT
        vopenid,
        SUM(thescore) AS ScoreAfter
    FROM (
        SELECT
            vopenid,
            ThemeID,
            MAX(ThemeScore) AS thescore
        FROM
            100842_PVEBossEnd_balance.tspider
        WHERE dteventtime >= '2024-12-09 10:00:00'
        GROUP BY ThemeID, vopenid
    ) a
    GROUP BY vopenid
)
SELECT * FROM (
    SELECT
        rbd.*,
        newuser.reg_day,
        createTime,
        DATE_PARSE(rbd_date, '%Y-%m-%d') - DATE_PARSE(newuser.reg_day, '%Y-%m-%d') AS `游戏天数`,
        story.st_day AS `存档日期(本日)`,
        story.dungeonid AS `存档点id(本日)`,
        story.savepointname AS `存档点名(本日)`,
        story.savepointvalue AS `存档值(本日)`,
        story.stepname AS `剧情step(本日)`,
        tisrevivefromsavepoint AS `是否从存档复活`,
        sat.salldate AS `日期(终身)`,
        sat.dteventtime AS `最后主线时间(终身)`,
        -- sat.vroleid AS `玩家ID(终身)`,
        sat.dungeonid AS `副本id(终身)`,
        sat.stepname AS `剧情step(终身)`,
        cs.daily_online_time AS `每日在线(秒)`,
        cs.lifetime_online_time AS `终身在线时长(秒)`,
        cs.daily_online_time_minutes AS `每日在线(分钟)`,
        cs.lifetime_online_time_minutes AS `终身在线(分钟)`,
        -- cs.daily_online_time_category AS `每日在线区间`,
        -- cs.lifetime_online_time_category AS `终身在线区间`,
        COALESCE(pvp_info.pvp_times, 0) AS `本日天下会武次数`,
        COALESCE(pv3_info.pv3_times, 0) AS `本日鼎足轮战次数`,
        COALESCE(pve_info.pve_times, 0) AS `本日道冲之渊次数`,
        COALESCE(pvp_info.alltime_pvp_times, 0) AS `终身天下会武次数`,
        COALESCE(pv3_info.alltime_pv3_times, 0) ASWITH newuser AS (
    SELECT
        reg_day,
        vroleid,
        vopenid,
        dtEventTime AS createTime,
        vrolename
    FROM (
        SELECT
            SUBSTRING(dtEventTime, 1, 10) AS reg_day,
            *,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid
                ORDER BY dtEventTime DESC -- 最后一条注册记录
            ) AS t
        FROM
            100842_PlayerRegister_balance
        WHERE
            dteventtime >= '2025-02-28 10:00:00'
            -- AND dtEventTime >= '2025-02-28 00:00:00' AND dtEventTime <= '2025-02-28 23:59:59'
    )
    WHERE t = 1
),
LastLogin AS (
    SELECT
        *
    FROM (
        SELECT
            vroleid,
            vopenid,
            SUBSTRING(dtEventTime, 1, 10) AS ldate,
            dtEventTime AS last_time,
            'login' AS status,
            ilevel,
            vrolename,
            iviplevel,
            irolece,
            itotalrecharge,
            iseasonid,
            iseasonlevel,
            playerfriendsnum AS friend_info,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid, SUBSTRING(dtEventTime, 1, 10)
                ORDER BY dtEventTime DESC
            ) AS t
        FROM
            100842_PlayerLogin_balance
    )
    WHERE t = 1
),
LastLogout AS (
    SELECT
        *
    FROM (
        SELECT
            vroleid,
            vopenid,
            SUBSTRING(dtEventTime, 1, 10) AS ldate,
            dtEventTime AS last_time,
            'logout' AS status,
            ilevel,
            vrolename,
            iviplevel,
            irolece,
            itotalrecharge,
            iseasonid,
            iseasonlevel,
            playerfriendsnum AS friend_info,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid, SUBSTRING(dtEventTime, 1, 10)
                ORDER BY dtEventTime DESC
            ) AS t
        FROM
            100842_PlayerLogout_balance
    )
    WHERE t = 1
),
Combined AS (
    SELECT
        *
    FROM LastLogin
    UNION ALL
    SELECT
        *
    FROM LastLogout
),
FinalStatus AS (
    SELECT
        vroleid,
        vopenid,
        ldate,
        vrolename,
        last_time,
        status,
        ilevel,
        iviplevel,
        irolece,
        itotalrecharge,
        iseasonid,
        iseasonlevel,
        friend_info,
        ROW_NUMBER() OVER (
            PARTITION BY vroleid, ldate
            ORDER BY last_time DESC
        ) AS rn
    FROM Combined
),
role_base_data AS (
    SELECT
        vroleid,
        vopenid,
        vrolename,
        ldate AS rbd_date,
        last_time,
        status,
        ilevel,
        iviplevel,
        irolece,
        itotalrecharge,
        iseasonid,
        iseasonlevel,
        friend_info
    FROM FinalStatus
    WHERE rn = 1
    ORDER BY vroleid, ldate
),
story AS (
    SELECT * FROM (
        -- 玩家每日推图主线保存点
        SELECT
            SUBSTRING(dtEventTime, 1, 10) AS st_day,
            dtEventTime AS event_time,
            vroleid,
            dungeonid,
            savepointname,
            savepointvalue,
            currenthp,
            currentmp,
            currenthppercent,
            currentmppercent,
            isrevivefromsavepoint AS tisrevivefromsavepoint,
            CASE
                WHEN DungeonID < 50000 THEN DungeonID * 100 + SavePointValue
                WHEN DungeonID = 2020866 THEN FLOOR(DungeonID / 10) * 10 + SavePointValue
                ELSE DungeonID
            END AS stepname,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid, SUBSTRING(dtEventTime, 1, 10)
                ORDER BY dtEventTime DESC
            ) AS t
        FROM
            100842_StoryStatusSavePointFlow_balance
        WHERE dteventtime >= '2025-02-28 10:00:00'
    ) a
    WHERE t = 1
),
story_all_time AS (
    -- 玩家终身推图主线
    SELECT * FROM (
        SELECT
            SUBSTRING(dtEventTime, 1, 10) AS salldate,
            dteventtime,
            vroleid,
            dungeonid,
            SavePointValue,
            SavePointName,
            IsReviveFromSavePoint AS aisrevivefromsavepoint,
            CASE
                WHEN DungeonID < 50000 THEN DungeonID * 100 + SavePointValue
                WHEN DungeonID = 2020866 THEN FLOOR(DungeonID / 10) * 10 + SavePointValue
                ELSE DungeonID
            END AS stepname,
            ROW_NUMBER() OVER (
                PARTITION BY vroleid
                ORDER BY dteventtime DESC
            ) AS t
        FROM
            100842_StoryStatusSavePointFlow_balance
        WHERE dteventtime >= '2025-02-28 10:00:00'
    )
    WHERE t = 1
),
OnlineStats AS (
    SELECT
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        COALESCE(SUM(onlinetime), 0) AS daily_online_time,
        COALESCE(SUM(SUM(onlinetime)) OVER (PARTITION BY vroleid), 0) AS lifetime_online_time
    FROM
        100842_PlayerLogout_balance
    WHERE
        vHeaderID <> 'DEFAULT'
        AND dteventtime >= '2025-02-28 10:00:00'
    GROUP BY
        vroleid, SUBSTR(dteventtime, 1, 10)
),
ClassifiedStats AS (
    SELECT
        vroleid,
        event_date,
        daily_online_time,
        lifetime_online_time,
        ROUND(COALESCE(daily_online_time, 0) / 60, 2) AS daily_online_time_minutes,
        ROUND(COALESCE(lifetime_online_time, 0) / 60, 2) AS lifetime_online_time_minutes
        -- CASE
        --     WHEN COALESCE(daily_online_time, 0) < 60 THEN CONCAT(CAST(FLOOR(COALESCE(daily_online_time, 0) / 60) AS VARCHAR), '~', CAST(FLOOR(COALESCE(daily_online_time, 0) / 60) + 1 AS VARCHAR), '分钟')
        --     WHEN COALESCE(daily_online_time, 0) < 1800 THEN CONCAT(CAST(FLOOR(COALESCE(daily_online_time, 0) / 60) AS VARCHAR), '分钟')
        --     ELSE CONCAT(CAST(FLOOR(COALESCE(daily_online_time, 0) / 900) * 15 AS VARCHAR), '~', CAST(FLOOR(COALESCE(daily_online_time, 0) / 900) * 15 + 15 AS VARCHAR), '分钟')
        -- END AS daily_online_time_category,
        -- CASE
        --     WHEN COALESCE(lifetime_online_time, 0) < 60 THEN CONCAT(CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 60) AS VARCHAR), '~', CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 60) + 1 AS VARCHAR), '分钟')
        --     WHEN COALESCE(lifetime_online_time, 0) < 1800 THEN CONCAT(CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 60) AS VARCHAR), '分钟')
        --     ELSE CONCAT(CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 900) * 15 AS VARCHAR), '~', CAST(FLOOR(COALESCE(lifetime_online_time, 0) / 900) * 15 + 15 AS VARCHAR), '分钟')
        -- END AS lifetime_online_time_category
    FROM OnlineStats
),
pvp_info AS (
    SELECT
        COUNT(*) AS pvp_times,
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        SUM(COUNT(*)) OVER (PARTITION BY vroleid) AS alltime_pvp_times
    FROM
        100842_ArenaBegin_balance
    WHERE fighttype = 51
    GROUP BY vroleid, SUBSTR(dteventtime, 1, 10)
),
pv3_info AS (
    SELECT
        COUNT(*) AS pv3_times,
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        SUM(COUNT(*)) OVER (PARTITION BY vroleid) AS alltime_pv3_times
    FROM
        100842_TeamBattleBegin_balance
    GROUP BY vroleid, SUBSTR(dteventtime, 1, 10)
),
pve_info AS (
    SELECT
        COUNT(*) AS pve_times,
        vroleid,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        SUM(COUNT(*)) OVER (PARTITION BY vroleid) AS alltime_pve_times
    FROM
        100842_PVEBossEnd_balance
    GROUP BY vroleid, SUBSTR(dteventtime, 1, 10)
),
arena_begin AS (
    SELECT
        vopenid,
        vroleid,
        vrolename,
        dteventtime,
        SUBSTR(dteventtime, 1, 10) AS event_date,
        fighttype,
        gametraceid,
        matchtime,
        phase,
        subphase,
        rivalisai
    FROM
        100842_ArenaBegin_balance
    WHERE
        fighttype = 51 -- 仅筛选 fighttype = 51 的记录
        AND dteventtime >= '2025-02-28 10:00:00'
        -- AND rivalisai <> 1
),
arena_end AS (
    SELECT
        dteventtime,
        vopenid,
        vroleid,
        vrolename,
        fighttype,
        gametraceid,
        fightresulttype,
        resultdetail,
        phase,
        subphase,
        score,
        changescore,
        hidderscore,
        changehidderscore
    FROM
        100842_ArenaEnd_balance
    WHERE
        fighttype = 51 -- 仅筛选 fighttype = 51 的记录
        AND dteventtime >= '2025-02-28 10:00:00'
),
game_result AS (
    -- 详细数据
    SELECT
        ab.vopenid,
        ab.vroleid,
        ab.vrolename AS `昵称`,
        ab.event_date AS `日期`,
        ab.fighttype AS `战斗类型`,
        CAST(ab.gametraceid AS VARCHAR) AS gametraceid,
        -- CAST(ab.gametraceid AS VARCHAR) AS cast_gametraceid,
        CAST(ae.gametraceid AS VARCHAR) AS end_gameid,
        ab.matchtime AS `匹配时长`,
        ab.phase AS `大段（匹配前）`,
        ab.subphase AS `小段（匹配前）`,
        ae.fightresulttype AS `结果`,
        CASE
            WHEN ae.fightresulttype = 0 THEN 'win' -- 玩家胜利
            WHEN ae.fightresulttype = 1 THEN 'draw' -- 平局
            WHEN ae.fightresulttype = 2 THEN 'lose' -- 无效结果
            WHEN ae.fightresulttype IS NULL THEN 'in game'
            ELSE 'wrong'
        END AS `战斗结果`,
        ae.resultdetail AS `对局详情`,
        ae.phase AS `大段（匹配后）`,
        ae.subphase AS `小段（匹配后）`,
        ab.dteventtime AS `开始战斗时间`,
        ae.dteventtime,
        ae.score AS `结算分数`,
        ae.changescore AS `分数变化`,
        ae.hidderscore AS `隐藏分`,
        ae.changehidderscore AS `变化的隐藏分`,
        rivalisai AS `是否机器人`
    FROM
        arena_begin ab
    LEFT JOIN arena_end ae ON ae.vopenid = ab.vopenid AND ae.gametraceid = ab.gametraceid
),
total_games AS (
    SELECT
        vopenid,
        COUNT(DISTINCT gametraceid) AS total_games,
        COUNT(IF(`战斗结果` = 'win', gametraceid, NULL)) AS total_win,
        COUNT(IF(`战斗结果` = 'lose', gametraceid, NULL)) AS total_lose,
        COUNT(IF(`战斗结果` = 'draw', gametraceid, NULL)) AS total_draw,
        COUNT(IF(`战斗结果` = 'wrong', gametraceid, NULL)) AS total_wrong
    FROM game_result
    GROUP BY vopenid
),
total_real_games AS (
    SELECT
        vopenid,
        COUNT(DISTINCT gametraceid) AS real_games
    FROM game_result
    WHERE `是否机器人` <> 1
    GROUP BY vopenid
),
final_score AS (
    SELECT * FROM (
        SELECT
            vopenid,
            `结算分数`,
            ROW_NUMBER() OVER (PARTITION BY vopenid ORDER BY dtEventTime DESC) AS t
        FROM game_result
    ) a
    WHERE t = 1
),
logout AS (
    SELECT * FROM (
        SELECT
            vopenid,
            vrolename,
            ROW_NUMBER() OVER (PARTITION BY vopenid ORDER BY dtEventTime DESC) AS t
        FROM 100842_PlayerLogout_balance
    ) a
    WHERE t = 1
),
pvp_data AS (
    -- 玩家最终段位，胜率等信息
    SELECT * FROM (
        SELECT
            gs.vopenid,
            gs.vroleid,
            lg.vrolename AS `昵称`,
            gs.`结算分数`,
            gs.`日期`,
            gs.`战斗类型`,
            gs.gametraceid,
            ROUND(gs.`匹配时长` / 1000, 2) AS `匹配时长（秒）`,
            gs.`大段（匹配前）`,
            gs.`小段（匹配前）`,
            gs.`大段（匹配后）`,
            gs.`小段（匹配后）`,
            total_games AS `总场次`,
            total_win AS `总胜场`,
            total_lose AS `总败场`,
            total_draw AS `总平局`,
            total_wrong AS `总错误对局`,
            COALESCE(real_games, 0) AS `真人对局数`,
            FORMAT('%s%%', ROUND(total_win / total_games * 100, 2)) AS `玩家胜率`,
            ROW_NUMBER() OVER (PARTITION BY gs.vopenid ORDER BY dtEventTime DESC) AS row_r
        FROM
            game_result gs
        LEFT JOIN total_games tg ON tg.vopenid = gs.vopenid
        LEFT JOIN total_real_games trg ON trg.vopenid = gs.vopenid
        LEFT JOIN final_score fs ON fs.vopenid = gs.vopenid
        LEFT JOIN logout lg ON lg.vopenid = gs.vopenid
    ) a
    WHERE row_r = 1
),
login_device AS (
    -- 登陆过的平台
    SELECT
        vopenid,
        CASE
            WHEN COUNT(DISTINCT PlatID) = 1 AND MAX(PlatID) IN (0, 1) THEN '仅手机' -- 仅在手机平台（iOS或安卓）登录
            WHEN COUNT(DISTINCT PlatID) = 1 AND MAX(PlatID) = 2 THEN '仅PC' -- 仅在PC平台登录
            WHEN COUNT(DISTINCT PlatID) > 1 THEN '双栖' -- 在手机和PC平台都登录过
            ELSE '未知' -- 其他情况
        END AS LoginType
    FROM
        100842_PlayerLogin_balance
    GROUP BY vopenid
),
Rank_data AS (
    SELECT
        vopenid,
        SUM(thescore) AS ScoreAfter
    FROM (
        SELECT
            vopenid,
            ThemeID,
            MAX(ThemeScore) AS thescore
        FROM
            100842_PVEBossEnd_balance.tspider
        WHERE dteventtime >= '2024-12-09 10:00:00'
        GROUP BY ThemeID, vopenid
    ) a
    GROUP BY vopenid
)
SELECT * FROM (
    SELECT
        rbd.*,
        newuser.reg_day,
        createTime,
        DATE_PARSE(rbd_date, '%Y-%m-%d') - DATE_PARSE(newuser.reg_day, '%Y-%m-%d') AS `游戏天数`,
        story.st_day AS `存档日期(本日)`,
        story.dungeonid AS `存档点id(本日)`,
        story.savepointname AS `存档点名(本日)`,
        story.savepointvalue AS `存档值(本日)`,
        story.stepname AS `剧情step(本日)`,
        tisrevivefromsavepoint AS `是否从存档复活`,
        sat.salldate AS `日期(终身)`,
        sat.dteventtime AS `最后主线时间(终身)`,
        -- sat.vroleid AS `玩家ID(终身)`,
        sat.dungeonid AS `副本id(终身)`,
        sat.stepname AS `剧情step(终身)`,
        cs.daily_online_time AS `每日在线(秒)`,
        cs.lifetime_online_time AS `终身在线时长(秒)`,
        cs.daily_online_time_minutes AS `每日在线(分钟)`,
        cs.lifetime_online_time_minutes AS `终身在线(分钟)`,
        -- cs.daily_online_time_category AS `每日在线区间`,
        -- cs.lifetime_online_time_category AS `终身在线区间`,
        COALESCE(pvp_info.pvp_times, 0) AS `本日天下会武次数`,
        COALESCE(pv3_info.pv3_times, 0) AS `本日鼎足轮战次数`,
        COALESCE(pve_info.pve_times, 0) AS `本日道冲之渊次数`,
        COALESCE(pvp_info.alltime_pvp_times, 0) AS `终身天下会武次数`,
        COALESCE(pv3_info.alltime_pv3_times, 0) AS `终身鼎足轮战次数`,
        COALESCE(pve_info.alltime_pve_times,0) as `终身道冲之渊次数`,
        pvp_data.`大段（匹配后）`,
        pvp_data.`小段（匹配后）`,
        pvp_data.`总场次`,
        pvp_data.`总胜场`,
        pvp_data.`总败场`,
        pvp_data.`总平局`, 
        pvp_data.`总错误对局`,
        pvp_data.`真人对局数`,
        pvp_data.`玩家胜率`,
        login_device.LoginType as `是否双栖`,
        r.ScoreAfter as `道冲排行榜积分`
    from
    role_base_data rbd      
    left join newuser on newuser.vroleid = rbd.vroleid      
    left join story on story.vroleid = rbd.vroleid   and story.st_day = rbd.rbd_date      
    left join story_all_time sat on sat.vroleid = rbd.vroleid  and sat.salldate = rbd.rbd_date      
    left join ClassifiedStats cs on cs.vroleid = rbd.vroleid      and cs.event_date = rbd.rbd_date      
    left join pvp_info on pvp_info.vroleid = rbd.vroleid      and pvp_info.event_date = rbd.rbd_date      
    left join pv3_info on pv3_info.vroleid = rbd.vroleid      and pv3_info.event_date = rbd.rbd_date      
    left join pve_info on pve_info.vroleid = rbd.vroleid      and pve_info.event_date = rbd.rbd_date      
    left join pvp_data on pvp_data.vroleid = rbd.vroleid      
    left join login_device on login_device.vopenid = rbd.vopenid      
    left join Rank_data r on r.vopenid = rbd.vopenid      ) a  
    where 1=1  and 1=1  and 1=1   
    order by  vroleid, rbd_date 
    DESC  limit 1000;"#;
    let dialect = MySqlDialect {};
    let result = Parser::parse_sql(&dialect, sql);
    println!("{:?}", result);
    assert_eq!(result.is_ok(), true);
}
