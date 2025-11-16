use clap::Parser;
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use log::{debug, error, info, trace};
use rand::Rng;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::runtime::Builder;
use tokio::task::LocalSet;

use std::collections::HashMap;
use std::io::Result;
use std::time::{Duration, Instant};
use std::{cell::RefCell, rc::Rc};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
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
}

#[derive(Clone)]
struct Stats {
    writes: Rc<RefCell<HashMap<String, u64>>>,
}

impl Stats {
    fn new() -> Self {
        Self {
            writes: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    fn write(&mut self, channel: &String) {
        *self
            .writes
            .borrow_mut()
            .entry(channel.to_string())
            .or_insert(0) += 1;
    }
}

impl std::fmt::Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut total = 0;
        let writes = self.writes.borrow_mut();
        for (channel, writes) in writes.iter() {
            writeln!(f, "channel={channel} writes={writes}")?;
            total += writes;
        }
        write!(f, "total={total}")?;
        Ok(())
    }
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
        println!("{}", message.len());

        Self {
            tps,
            endpoint,
            channel,
            stats,
            message,
        }
    }
    async fn run(&mut self, end: Instant) -> Result<()> {
        info!("Connecting");
        let mut stream = tokio::net::TcpStream::connect(self.endpoint.clone()).await?;
        info!("Connected publisher {}", self.channel);

        // Inform the server that we're a publisher
        let publish = format!("PUBLISH {}\r\n", self.channel);
        debug!("Publish: \"{publish}\"");
        stream.write_all(publish.as_bytes()).await?;

        // Read back the OK message we expect
        let mut ok = [0u8; 4];
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
                tokio::time::sleep(next_target - now).await;
            } else {
                // We're running behind - log a warning if needed
                trace!("Running behind schedule by {:?}", now - next_target);
            }
        }
    }
}

async fn handle_subscriber(endpoint: String, channel: String, end: Instant) -> Result<()> {
    let mut stream = tokio::net::TcpStream::connect(endpoint).await?;
    info!("Connected subscriber {channel}");
    let subscribe = format!("SUBSCRIBE {channel}\r\n");
    stream.write_all(subscribe.as_bytes()).await?;
    let mut ok = [0u8; 4];
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
    loop {
        if Instant::now() > end {
            info!("Done! {total_msgs} received");
            return Ok(());
        }

        let mut line = String::new();
        reader.read_line(&mut line).await?; // TODO: Subscribers never return because they get
        // blocked here
        num_msgs += 1;
        total_msgs += 1;
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

    let runtime = Builder::new_current_thread()
        .worker_threads(1)
        .thread_name("thready")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let local = LocalSet::new();
    runtime.block_on(local.run_until(async {
        // At the very end display stats
        let stats = Stats::new();
        tokio::task::spawn_local({
            let stats = stats.clone();
            async move {
                let end = args.timeout + std::time::Instant::now() + Duration::from_millis(25);
                let _ = tokio::time::sleep(end - std::time::Instant::now()).await;
                info!("Stats: {}", stats);
            }
        });

        let start = std::time::Instant::now();
        let end = args.timeout + start;

        let mut rng = rand::rng();
        let mut set = tokio::task::JoinSet::new();
        for n in 0..args.publishers {
            let channel_name = format!("Channel_{n}");
            // Add some jitter here for ramp up
            let jitter_ms = rng.random_range(10..100);
            std::thread::sleep(std::time::Duration::from_millis(jitter_ms));

            // Spawn a publisher
            set.spawn(tokio::task::spawn_local({
                let channel_name = channel_name.clone();
                let endpoint = args.endpoint.clone();
                let stats = stats.clone();
                debug!("Starting publisher for {channel_name}");
                async move {
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
            }));

            // Spawn subscribers
            for _s in 0..args.subscribers_per_publisher {
                set.spawn(tokio::task::spawn_local({
                    let channel_name = channel_name.clone();
                    let endpoint = args.endpoint.clone();
                    debug!("Starting subscriber for {channel_name}");
                    async move {
                        if let Err(e) = handle_subscriber(endpoint, channel_name.clone(), end).await
                        {
                            error!("Error on publisher {channel_name} {e}");
                        }
                    }
                }));
            }
        }

        let _ = set.join_all().await;

        Ok(())
    }))
}
