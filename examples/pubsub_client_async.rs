use anyhow::Context;
use clap::Parser;
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use futures::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use log::{debug, error, info, trace};
use rand::Rng;

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Result;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{cell::RefCell, rc::Rc};

use twoio::executor;
use twoio::file::File;
use twoio::net as unet;
use twoio::sync::wg::WaitGroup;
use twoio::timeout::{sleep_for, ticker};
use twoio::uring;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Size of uring submission queue
    #[arg(short, long, default_value_t = 4096)]
    uring_size: u32,

    /// Number of submissions in the backlog before submitting to uring
    #[arg(short, long, default_value_t = 1024)]
    submissions_threshold: usize,

    /// Interval for kernel submission queue polling. If 0 then sqpoll is disabled. Default 0.
    #[arg(short = 'i', long, default_value_t = 0)]
    sqpoll_interval_ms: u32,

    /// How many publishers to create
    #[arg(short, long, default_value_t = 1)]
    publishers: u32,

    /// How many subscribers per publisher
    #[arg(short = 'm', long, default_value_t = 5)]
    subscribers_per_publisher: u32,

    /// How long to run the test
    #[arg(short = 't', long, default_value = "10s", value_parser = duration_range_value_parse!(min: 1s, max: 1h))]
    timeout: DurationHuman,

    #[arg(short, long, default_value = "127.0.0.1:8080")]
    endpoint: String,

    #[arg(long, default_value_t = 20)]
    tps: u32,

    #[arg(short = 'b', long, default_value_t = 8192)]
    message_size: usize,

    /// Path where JSONL stats should be written
    #[arg(long, default_value = "/tmp/pubsub_client_stats.jsonl")]
    stats_output: PathBuf,
}

#[derive(Clone)]
struct Stats {
    writes: Rc<RefCell<HashMap<String, u64>>>,
}

#[derive(Clone)]
struct SubscriberStats {
    received: Rc<RefCell<HashMap<String, u64>>>,
}

impl Stats {
    fn new() -> Self {
        Self {
            writes: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    fn write(&mut self, channel: &String) {
        let mut m = self.writes.borrow_mut();
        *m.entry(channel.to_string()).or_insert(0) += 1;
    }

    fn total_writes(&self) -> u64 {
        self.writes.borrow().values().sum()
    }
}

impl SubscriberStats {
    fn new() -> Self {
        Self {
            received: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    fn record(&self, subscriber: &str) {
        let mut counts = self.received.borrow_mut();
        *counts.entry(subscriber.to_string()).or_insert(0) += 1;
    }
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut total = 0;
        for (channel, writes) in self.writes.borrow().iter() {
            writeln!(f, "channel={channel} writes={writes}")?;
            total += writes;
        }
        write!(f, "total={total}")?;
        Ok(())
    }
}

impl std::fmt::Display for SubscriberStats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut total = 0;
        for (subscriber, count) in self.received.borrow().iter() {
            writeln!(f, "subscriber={subscriber} messages={count}")?;
            total += count;
        }
        write!(f, "subscriber_total={total}")?;
        Ok(())
    }
}

async fn persist_total_messages_jsonl(stats: Stats, path: PathBuf, end: Instant) -> Result<()> {
    let mut file = File::open(&path)?.await?;
    loop {
        let now = Instant::now();
        if now >= end {
            break;
        }
        let remaining = end - now;
        let wait = if remaining > Duration::from_secs(5) {
            Duration::from_secs(5)
        } else {
            remaining
        };
        sleep_for(wait).await?;
        let total = stats.total_writes();
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let line = format!("{{\"timestamp_ms\":{timestamp_ms},\"total_messages\":{total}}}\n");
        file.write_all(line.as_bytes()).await?;
        file.flush().await?;
    }
    file.close().await?;
    Ok(())
}

struct Publisher {
    tps: u32,
    endpoint: String,
    channel: String,
    stats: Stats,
    message: String,
}

impl Publisher {
    fn new(tps: u32, endpoint: String, channel: String, stats: Stats, message_size: usize) -> Self {
        let message_base = "bring more tacos"; // chatgpt generated 16 character quip :rolleyes:
        let repeated = message_size / message_base.len();
        let message_body = format!("{}_{}", channel, message_base.repeat(repeated));
        let slice_end = message_size.min(message_body.len());
        let message = format!("{}\r\n", &message_body[..slice_end]);

        Self {
            tps,
            endpoint,
            channel,
            stats,
            message,
        }
    }
    async fn run(&mut self, end: Instant) -> Result<()> {
        info!("Connecting task={}", executor::get_task_id());
        let mut stream = unet::TcpStream::connect(self.endpoint.clone()).await?;
        info!("Connected publisher {}", self.channel);

        // Inform the server that we're a publisher
        let publish = format!("PUBLISH {}\r\n", self.channel);
        debug!("Publish: \"{publish}\"");
        stream.write_all(publish.as_bytes()).await?;

        // Read back the OK message we expect
        let mut ok = [0u8; 16];
        debug!("Reading ok");
        let _ = stream.read(&mut ok).await?;
        if !ok.starts_with(b"OK\r\n") {
            return Err(std::io::Error::other("didn't get OK"));
        }
        debug!("Got ok");

        // Start publishing
        let mut n = 0;
        let interval = Duration::from_millis((1000 / self.tps).into());
        let start_time = Instant::now();

        loop {
            if Instant::now() > end {
                info!("Done!");
                return Ok(());
            }

            n += 1;
            trace!(
                "Writing message {} channel={}",
                self.message.replace("\n", "\\n").replace("\r", "\\r"),
                self.channel
            );
            if let Err(e) = stream.write_all(self.message.as_bytes()).await {
                error!("Error writing message {}: {e}", self.message);
                return Ok(());
            }
            self.stats.write(&self.channel);
            trace!(
                "Wrote message {} channel={}",
                self.message.replace("\n", "\\n").replace("\r", "\\r"),
                self.channel
            );

            // Sleep until the next scheduled time
            let next_target = start_time + interval * n;
            let now = Instant::now();
            if next_target > now {
                sleep_for(next_target - now).await?;
            } else {
                // We're running behind - log a warning if needed
                trace!("Running behind schedule by {:?}", now - next_target);
            }
        }
    }
}

async fn handle_subscriber(
    endpoint: String,
    channel: String,
    end: Instant,
    stats: SubscriberStats,
) -> Result<()> {
    let mut stream = unet::TcpStream::connect(endpoint).await?;
    info!("Connected subscriber {channel} fd={}", stream.as_raw_fd());
    let subscribe = format!("SUBSCRIBE {channel}\r\n");
    stream.write_all(subscribe.as_bytes()).await?;
    let mut ok = [0u8; 16];
    debug!("Reading ok");
    let _ = stream.read(&mut ok).await?;
    if !ok.starts_with(b"OK\r\n") {
        return Err(std::io::Error::other("didn't get OK"));
    }

    // Start publishing
    let mut reader = BufReader::new(stream);
    let mut last = Instant::now();
    let mut num_msgs = 0;
    let mut total_msgs = 0;
    let subscriber_id = format!("{channel}-{}", executor::get_task_id());
    loop {
        if Instant::now() > end {
            info!("Done! {total_msgs} received");
            return Ok(());
        }

        let mut line = String::new();
        reader.read_line(&mut line).await?;
        num_msgs += 1;
        total_msgs += 1;
        stats.record(&subscriber_id);
        trace!("Got line {line}");

        if last.elapsed() > Duration::from_secs(1) {
            debug!("Channel {channel} received {num_msgs} in last second");
            num_msgs = 0;
            last = Instant::now();
        }
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default())
        .format_timestamp_millis() // Configure millisecond precision
        .init();

    let args = Args::parse();
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&args.stats_output)
        .with_context(|| format!("creating {}", args.stats_output.display()))?;

    uring::init(uring::UringArgs {
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: args.sqpoll_interval_ms,
    })?;

    executor::init();

    let start = Instant::now();
    let end = args.timeout + start;

    executor::spawn(async {
        let mut timeout = ticker(Duration::from_secs(5));
        loop {
            timeout = timeout.await.expect("REASON");
            let stats = uring::stats().expect("stats");
            info!("Metrics: {}", stats);
        }
    });

    // At the very end display stats
    let stats = Stats::new();
    let subscriber_stats = SubscriberStats::new();
    executor::spawn({
        let stats = stats.clone();
        let stats_path = args.stats_output.clone();
        async move {
            if let Err(e) = persist_total_messages_jsonl(stats, stats_path, end).await {
                error!("Failed to persist stats JSONL: {e}");
            }
        }
    });

    let expected = args.subscribers_per_publisher
        * args.publishers
        * args.tps
        * ((end - start).as_secs() as u32);
    info!(
        "Starting {} subscribers for {} publishers at {}tps every {:?} expecting {} messages",
        args.subscribers_per_publisher, args.publishers, args.tps, args.timeout, expected
    );

    let mut rng = rand::rng();
    let mut wg = WaitGroup::default();
    for n in 0..args.publishers {
        let channel_name = format!("Channel_{n}");
        // Add some jitter here for ramp up
        let jitter_ms = rng.random_range(10..100);
        std::thread::sleep(std::time::Duration::from_millis(jitter_ms));

        // Spawn a publisher
        let g = wg.add();
        executor::spawn({
            let channel_name = channel_name.clone();
            let endpoint = args.endpoint.clone();
            let stats = stats.clone();
            debug!("Starting publisher for {channel_name}");
            async move {
                let _g = g;
                let mut publisher = Publisher::new(
                    args.tps,
                    endpoint,
                    channel_name.clone(),
                    stats,
                    args.message_size,
                );
                if let Err(e) = publisher.run(end).await {
                    error!("Error on publisher {channel_name} {e}");
                }
            }
        });

        // Spawn subscribers
        for _s in 0..args.subscribers_per_publisher {
            executor::spawn({
                let channel_name = channel_name.clone();
                let endpoint = args.endpoint.clone();
                let subscriber_stats = subscriber_stats.clone();
                debug!("Starting subscriber for {channel_name}");
                async move {
                    if let Err(e) =
                        handle_subscriber(endpoint, channel_name.clone(), end, subscriber_stats)
                            .await
                    {
                        error!("Error on publisher {channel_name} {e}");
                    }
                }
            });
        }
    }

    executor::spawn(async move {
        wg.wait().await;
        info!("Publishers are all done, exiting");
        uring::exit();
    });

    executor::run();

    info!("Subscriber stats: {}", subscriber_stats);

    Ok(())
}
