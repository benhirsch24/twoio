use clap::Parser;
use futures::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use log::{debug, error, info, trace, warn};

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};

use twoio::executor;
use twoio::net as unet;
use twoio::sync::mpsc;
use twoio::timeout::sleep_for;
use twoio::uring;

static OK: &[u8] = b"OK\r\n";
const SUBSCRIBER_PACING_BYTES_PER_SEC: u32 = 2 * 1024 * 1024 / 8; // ~2 Mbps

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

    /// Enable per-subscriber pacing (~2 Mbps)
    #[arg(long, default_value_t = false)]
    enable_pacing: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ChannelId(u32);

struct SubscriberInfo {
    id: ChannelId,
    senders: HashMap<RawFd, mpsc::Sender<Rc<String>>>,
}

#[derive(Clone)]
enum StatEvent {
    ChannelRegistered { channel_id: ChannelId, name: String },
    ChannelRemoved { channel_id: ChannelId },
    MessagesSent { channel_id: ChannelId, count: u64 },
}

struct ChannelStats {
    name: String,
    sent: u64,
    active: bool,
}

#[derive(Clone)]
struct Subscribers {
    inner: Rc<RefCell<SubscribersInner>>,
}

struct SubscribersInner {
    next_channel_id: u32,
    channels: HashMap<String, SubscriberInfo>,
}

impl Subscribers {
    fn new() -> Self {
        Subscribers {
            inner: Rc::new(RefCell::new(SubscribersInner {
                next_channel_id: 0,
                channels: HashMap::new(),
            })),
        }
    }

    fn senders_for(&self, channel: &str) -> Vec<mpsc::Sender<Rc<String>>> {
        if let Some(info) = self.inner.borrow().channels.get(channel) {
            info.senders.values().cloned().collect()
        } else {
            Vec::new()
        }
    }

    fn channel_id(&self, channel: &str) -> Option<ChannelId> {
        self.inner
            .borrow()
            .channels
            .get(channel)
            .map(|info| info.id)
    }

    fn add_subscriber(
        &self,
        channel: &str,
        subscriber_fd: RawFd,
        sender: mpsc::Sender<Rc<String>>,
    ) -> Option<ChannelId> {
        let mut inner = self.inner.borrow_mut();
        if let Some(info) = inner.channels.get_mut(channel) {
            info.senders.insert(subscriber_fd, sender);
            None
        } else {
            let channel_id = ChannelId(inner.next_channel_id);
            inner.next_channel_id += 1;
            let mut info = SubscriberInfo {
                id: channel_id,
                senders: HashMap::new(),
            };
            info.senders.insert(subscriber_fd, sender);
            inner.channels.insert(channel.to_string(), info);
            Some(channel_id)
        }
    }

    fn remove_subscriber(&self, channel: &str, subscriber_fd: RawFd) -> Option<ChannelId> {
        let mut inner = self.inner.borrow_mut();
        let remove_channel = match inner.channels.get_mut(channel) {
            Some(info) => {
                info.senders.remove(&subscriber_fd);
                if info.senders.is_empty() {
                    Some(info.id)
                } else {
                    None
                }
            }
            None => None,
        };
        if let Some(channel_id) = remove_channel {
            inner.channels.remove(channel);
            Some(channel_id)
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct ChannelStatsStore {
    inner: Rc<RefCell<ChannelStatsStoreInner>>,
}

struct ChannelStatsStoreInner {
    stats: HashMap<ChannelId, ChannelStats>,
}

#[derive(Clone)]
struct ChannelStatsSnapshot {
    name: String,
    sent: u64,
    active: bool,
}

impl ChannelStatsStore {
    fn new() -> Self {
        ChannelStatsStore {
            inner: Rc::new(RefCell::new(ChannelStatsStoreInner {
                stats: HashMap::new(),
            })),
        }
    }

    fn record_registration(&self, channel_id: ChannelId, name: String) {
        self.inner.borrow_mut().stats.insert(
            channel_id,
            ChannelStats {
                name,
                sent: 0,
                active: true,
            },
        );
    }

    fn mark_inactive(&self, channel_id: ChannelId) {
        if let Some(entry) = self.inner.borrow_mut().stats.get_mut(&channel_id) {
            entry.active = false;
        }
    }

    fn add_messages(&self, channel_id: ChannelId, count: u64) {
        if let Some(entry) = self.inner.borrow_mut().stats.get_mut(&channel_id) {
            entry.sent += count;
        }
    }

    fn collect_and_reset(&self) -> Vec<(ChannelId, ChannelStatsSnapshot)> {
        let mut inner = self.inner.borrow_mut();
        inner
            .stats
            .iter_mut()
            .map(|(channel_id, entry)| {
                let snapshot = ChannelStatsSnapshot {
                    name: entry.name.clone(),
                    sent: entry.sent,
                    active: entry.active,
                };
                entry.sent = 0;
                (*channel_id, snapshot)
            })
            .collect()
    }

    fn remove(&self, channel_id: ChannelId) {
        self.inner.borrow_mut().stats.remove(&channel_id);
    }
}

async fn handle_publisher(
    mut reader: BufReader<unet::TcpStream>,
    mut writer: unet::TcpStream,
    channel: String,
    subscribers: Subscribers,
    mut stats_tx: mpsc::Sender<StatEvent>,
) {
    let task_id = executor::get_task_id();
    let fd = writer.as_raw_fd();
    debug!("Handling publisher channel={channel} fd={fd} task_id={task_id}");
    writer.write_all(OK).await.expect("OK");
    trace!("OK fd={fd} task_id={task_id}");

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) => {
                if n == 0 {
                    info!("Publisher left");
                    return;
                }
                debug!(
                    "Got message {} channel={channel} fd={fd} task_id={task_id}",
                    line.replace("\n", "\\n").replace("\r", "\\r")
                );
                let senders = subscribers.senders_for(&channel);
                let payload = Rc::new(line.clone());
                let mut delivered = 0u64;
                for mut sender in senders {
                    match sender.send(payload.clone()) {
                        Ok(()) => delivered += 1,
                        Err(mpsc::SendError::Closed) => {
                            debug!("Sender closed for channel={channel}");
                        }
                        Err(mpsc::SendError::Full) => unreachable!("Channel is unbounded"),
                    }
                }
                if delivered > 0
                    && let Some(channel_id) = subscribers.channel_id(&channel)
                {
                    let _ = stats_tx.send(StatEvent::MessagesSent {
                        channel_id,
                        count: delivered,
                    });
                }
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::ConnectionReset {
                    error!("Got error {e}");
                }
                return;
            }
        }
    }
}

async fn handle_subscriber(
    mut reader: BufReader<unet::TcpStream>,
    mut writer: unet::TcpStream,
    channel: String,
    subscribers: Subscribers,
    mut stats_tx: mpsc::Sender<StatEvent>,
    pacing_rate: Option<u32>,
) {
    let subscriber_fd = writer.as_raw_fd();
    debug!("Handling subscriber channel={channel} fd={subscriber_fd}");
    if let Some(rate) = pacing_rate {
        if let Err(e) = writer.set_pacing_rate(rate) {
            warn!("Failed to set pacing for subscriber fd={subscriber_fd}: {e}");
        }
    }
    writer.write_all(OK).await.expect("OK");

    let (rx_inner, tx) = mpsc::channel::<Rc<String>>();
    let rx = Rc::new(RefCell::new(rx_inner));
    if let Some(channel_id) = subscribers.add_subscriber(&channel, subscriber_fd, tx) {
        let _ = stats_tx.send(StatEvent::ChannelRegistered {
            channel_id,
            name: channel.clone(),
        });
    }

    executor::spawn({
        let rx = rx.clone();
        let mut writer = writer;
        let channel = channel.clone();
        async move {
            let task_id = executor::get_task_id();
            let fd = writer.as_raw_fd();
            loop {
                let fut = { rx.borrow_mut().recv() };
                match fut.await {
                    Some(msg) => {
                        if let Err(e) = writer.sendzc(msg.as_ref().as_ref()).await {
                            error!(
                                "Failed to write line to channel={channel} fd={fd} task_id={task_id}: {e}"
                            );
                            break;
                        }
                    }
                    None => {
                        debug!("Writer exiting channel={channel} fd={fd} task_id={task_id}");
                        break;
                    }
                }
            }
            if let Err(e) = writer.close().await {
                warn!("Failed to close writer fd={fd} channel={channel}: {e}");
            }
        }
    });

    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) => {
                if n == 0 {
                    debug!("Subscriber left");
                    if let Some(channel_id) = subscribers.remove_subscriber(&channel, subscriber_fd)
                    {
                        let _ = stats_tx.send(StatEvent::ChannelRemoved { channel_id });
                    }
                    rx.borrow_mut().close();
                    return;
                }
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::ConnectionReset {
                    error!("Got error {e}");
                }
                if let Some(channel_id) = subscribers.remove_subscriber(&channel, subscriber_fd) {
                    let _ = stats_tx.send(StatEvent::ChannelRemoved { channel_id });
                }
                rx.borrow_mut().close();
                return;
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    uring::init(uring::UringArgs {
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: args.sqpoll_interval_ms,
    })?;

    executor::init();

    let subscribers = Subscribers::new();
    let stats_map = ChannelStatsStore::new();
    let (stats_rx, stats_tx) = mpsc::channel::<StatEvent>();

    executor::spawn({
        let stats_map = stats_map.clone();
        let mut stats_rx = stats_rx;
        async move {
            loop {
                match stats_rx.recv().await {
                    Some(StatEvent::ChannelRegistered { channel_id, name }) => {
                        stats_map.record_registration(channel_id, name);
                    }
                    Some(StatEvent::ChannelRemoved { channel_id }) => {
                        stats_map.mark_inactive(channel_id);
                    }
                    Some(StatEvent::MessagesSent { channel_id, count }) => {
                        stats_map.add_messages(channel_id, count);
                    }
                    None => break,
                }
            }
        }
    });

    // General metrics routine for uring, executor, application metrics every 5s
    executor::spawn({
        let stats_map = stats_map.clone();
        async move {
            loop {
                let _ = sleep_for(Duration::from_secs(5)).await.expect("REASON");
                let stats = uring::stats().expect("stats");
                info!("Uring metrics: {}", stats);

                // Print number of messages sent per subscriber so far
                let mut to_remove = Vec::new();
                for (channel_id, snapshot) in stats_map.collect_and_reset() {
                    if snapshot.active || snapshot.sent > 0 {
                        debug!("{} sent {}", snapshot.name, snapshot.sent);
                    }
                    if !snapshot.active && snapshot.sent == 0 {
                        to_remove.push(channel_id);
                    }
                }
                if !to_remove.is_empty() {
                    for channel_id in to_remove {
                        stats_map.remove(channel_id);
                    }
                }
            }
        }
    });

    executor::block_on(async move {
        let mut listener = unet::TcpListener::bind("0.0.0.0:8080").unwrap();
        let pacing_rate = if args.enable_pacing {
            Some(SUBSCRIBER_PACING_BYTES_PER_SEC)
        } else {
            None
        };
        loop {
            debug!("Accepting");
            let stream = listener.accept_multi_fut().unwrap().await.unwrap();
            let subscriber_map = subscribers.clone();
            let stats_tx = stats_tx.clone();
            let pacing_rate = pacing_rate;
            executor::spawn(async move {
                let task_id = executor::get_task_id();
                debug!("Got stream task_id={task_id} fd={}", stream.as_raw_fd());
                // TODO: idk, it's weird that I'm cloning the TcpStream. I could technically create
                // a second read against it but that would be bad...
                // Be careful!
                let fd = stream.as_raw_fd();
                let writer = unet::TcpStream::new(fd);
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                trace!(
                    "Reading protocol line fd={fd} task_id={}",
                    executor::get_task_id()
                );
                if let Err(e) = reader.read_line(&mut line).await {
                    error!("Failed to read line: {e}");
                    let mut stream = unet::TcpStream::new(fd);
                    stream.close().await.expect("Stream closing");
                    return;
                }
                trace!("Read protocol {line}");
                if line.starts_with("PUBLISH") {
                    let channel = line.trim()[8..].to_string();
                    handle_publisher(reader, writer, channel, subscriber_map.clone(), stats_tx)
                        .await;
                } else if line.starts_with("SUBSCRIBE") {
                    let channel = line.trim()[10..].to_string();
                    handle_subscriber(
                        reader,
                        writer,
                        channel,
                        subscriber_map,
                        stats_tx,
                        pacing_rate,
                    )
                    .await;
                } else {
                    warn!(
                        "Line had length {} but didn't start with expected protocol fd={fd}",
                        line.len()
                    );
                }
                // TODO: Again, weird that I'm re-creating the tcp stream to close it but oh
                // wellsies
                let mut stream = unet::TcpStream::new(fd);
                if let Err(e) = stream.close().await {
                    warn!("Failed to close fd={}: {e}", stream.as_raw_fd());
                }
                debug!("Exiting task_id={task_id} fd={}", stream.as_raw_fd());
            });
        }
    });

    Ok(())
}
