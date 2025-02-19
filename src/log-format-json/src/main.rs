use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::sync::Once;
use tracing::{debug, info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_log::log::Level;
use tracing_log::LogTracer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry};

const DEFAULT_LOG_LEVEL: &str = "info";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoggingOptions {
    /// The directory to store log files, if not set, log to stdout.
    pub dir: String,

    /// Log file name prefix.
    pub log_file_name_prefix: String,

    /// The max number of log files.
    pub max_log_files: usize,

    /// output logs to stdout, default is true.
    pub output_stdout: bool,
}

impl PartialEq for LoggingOptions {
    fn eq(&self, other: &Self) -> bool {
        self.dir == other.dir
            && self.log_file_name_prefix == other.log_file_name_prefix
            && self.max_log_files == other.max_log_files
            && self.output_stdout == other.output_stdout
    }
}

impl Eq for LoggingOptions {}

impl Default for LoggingOptions {
    fn default() -> Self {
        Self {
            dir: "/tmp/bkbase/logs".to_string(),
            log_file_name_prefix: "queryengine".to_string(),
            // Rotation hourly, keep 3 days.
            max_log_files: 72,
            output_stdout: true,
        }
    }
}

pub fn init_global_logging(opts: &LoggingOptions) -> Vec<WorkerGuard> {
    static INIT: Once = Once::new();
    let mut guards = vec![];

    INIT.call_once(|| {
        LogTracer::init().expect("Failed to set logger");

        // config stdout log layer
        let stdout_log_layer = if opts.output_stdout {
            let (writer, g) = tracing_appender::non_blocking(std::io::stdout());
            guards.push(g);

            Some(
                Layer::new()
                    .with_writer(writer)
                    .with_ansi(atty::is(atty::Stream::Stdout))
                    .boxed(),
            )
        } else {
            None
        };

        // TODO(zww): use Sized based rolling policy instead in the future.
        // config file log layer with time rolling policy now.
        let file_log_layer = if !opts.dir.is_empty() {
            let rolling_file_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .filename_prefix(&opts.log_file_name_prefix)
                .max_log_files(opts.max_log_files)
                .build(&opts.dir)
                .unwrap_or_else(|err| {
                    panic!("Init log file appender at {} failed {}", &opts.dir, err);
                });

            let (writer, g) = tracing_appender::non_blocking(rolling_file_appender);
            guards.push(g);

            // record json format messages to log file,ignore timestamp,log level, target...
            Some(
                Layer::new()
                    .with_writer(writer)
                    .with_level(false)
                    .with_target(false)
                    .without_time()
                    .with_ansi(false)
                    .boxed(),
            )
        } else {
            None
        };

        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_LEVEL));

        let subscriber = Registry::default()
            .with(env_filter)
            .with(stdout_log_layer)
            .with(file_log_layer);

        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber for query engine");
    });

    guards
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogResponse {
    timestamp: String,
    level: String,
    msgs: HashMap<String, String>,
}

#[tokio::main]
async fn main() {
    let opts = LoggingOptions::default();
    let _guards = init_global_logging(&opts);

    let mut map = HashMap::new();
    map.insert("key1".to_string(), "value1".to_string());
    map.insert("key2".to_string(), "value2".to_string());

    let resp = LogResponse {
        timestamp: "2025-02-18T03:15:45.778839Z".to_string(),
        level: "info".to_string(),
        msgs: map,
    };

    warn!("{}", serde_json::to_string(&resp).unwrap());
}
