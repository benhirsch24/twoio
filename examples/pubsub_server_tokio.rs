use log::{debug, error, info, trace, warn};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::runtime::Builder;
use tokio::sync::{broadcast, mpsc};
use tokio::task::LocalSet;

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::time::Duration;
use std::{cell::RefCell, rc::Rc};

static OK: &[u8] = b"OK\r\n";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ChannelId(u32);

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
struct ChannelStatsStore {
    inner: Rc<RefCell<HashMap<ChannelId, ChannelStats>>>,
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
            inner: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    fn record_registration(&self, channel_id: ChannelId, name: String) {
        self.inner.borrow_mut().insert(
            channel_id,
            ChannelStats {
                name,
                sent: 0,
                active: true,
            },
        );
    }

    fn mark_inactive(&self, channel_id: ChannelId) {
        if let Some(entry) = self.inner.borrow_mut().get_mut(&channel_id) {
            entry.active = false;
        }
    }

    fn add_messages(&self, channel_id: ChannelId, count: u64) {
        if let Some(entry) = self.inner.borrow_mut().get_mut(&channel_id) {
            entry.sent += count;
        }
    }

    fn collect_and_reset(&self) -> Vec<(ChannelId, ChannelStatsSnapshot)> {
        let mut inner = self.inner.borrow_mut();
        inner
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
        self.inner.borrow_mut().remove(&channel_id);
    }
}

struct SubscriberInfo {
    id: ChannelId,
    tx: broadcast::Sender<String>,
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

    fn sender_for(&self, channel: &str) -> Option<(broadcast::Sender<String>, ChannelId)> {
        self.inner
            .borrow()
            .channels
            .get(channel)
            .map(|info| (info.tx.clone(), info.id))
    }

    fn add_subscriber(
        &self,
        channel: &str,
    ) -> (broadcast::Receiver<String>, Option<(ChannelId, String)>) {
        let mut inner = self.inner.borrow_mut();
        if let Some(info) = inner.channels.get(channel) {
            (info.tx.subscribe(), None)
        } else {
            let channel_id = ChannelId(inner.next_channel_id);
            inner.next_channel_id += 1;
            let (tx, rx) = broadcast::channel(1024);
            inner
                .channels
                .insert(channel.to_string(), SubscriberInfo { id: channel_id, tx });
            (rx, Some((channel_id, channel.to_string())))
        }
    }

    fn remove_subscriber(&self, channel: &str) -> Option<ChannelId> {
        let mut inner = self.inner.borrow_mut();
        let remove_channel = match inner.channels.get(channel) {
            Some(info) if info.tx.receiver_count() == 0 => Some(info.id),
            _ => None,
        };
        if let Some(channel_id) = remove_channel {
            inner.channels.remove(channel);
            Some(channel_id)
        } else {
            None
        }
    }
}

async fn handle_publisher(
    mut stream: tokio::net::TcpStream,
    channel: String,
    subscribers: Subscribers,
    stats_tx: mpsc::UnboundedSender<StatEvent>,
) {
    let fd = stream.as_raw_fd();
    debug!("Handling publisher channel={channel} fd={fd}");
    stream.write_all(OK).await.expect("OK");
    trace!("OK fd={fd}");

    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) => {
                if n == 0 {
                    info!("Publisher left");
                    return;
                }
                debug!(
                    "Got message {} channel={channel} fd={fd}",
                    line.replace("\n", "\\n").replace("\r", "\\r")
                );
                if let Some((tx, channel_id)) = subscribers.sender_for(&channel) {
                    match tx.send(line) {
                        Ok(sent) => {
                            if sent > 0 {
                                let _ = stats_tx.send(StatEvent::MessagesSent {
                                    channel_id,
                                    count: sent as u64,
                                });
                            }
                        }
                        Err(e) => {
                            debug!("Failed to deliver to channel={channel}: {e}");
                        }
                    }
                }
            }
            Err(e) => {
                error!("Got error {e}");
                return;
            }
        }
    }
}

// After reading the subscribe message and sending OK, this task just keeps the subscriber alive until it leaves.
// The publisher is writing directly to the file descriptor which is shared by the shared map.
async fn handle_subscriber(
    mut stream: tokio::net::TcpStream,
    channel: String,
    subscribers: Subscribers,
    stats_tx: mpsc::UnboundedSender<StatEvent>,
) {
    debug!(
        "Handling subscriber channel={channel} fd={}",
        stream.as_raw_fd()
    );
    stream.write_all(OK).await.expect("OK");

    let (read, mut write) = stream.into_split();
    let (mut rx, registration) = subscribers.add_subscriber(&channel);
    if let Some((channel_id, name)) = registration {
        let _ = stats_tx.send(StatEvent::ChannelRegistered { channel_id, name });
    }

    let mut reader = BufReader::new(read);
    loop {
        let mut line = String::new();
        tokio::select! {
            read_res = reader.read_line(&mut line) => {
                match read_res {
                    Ok(n) => {
                        if n == 0 {
                            debug!("Subscriber left");
                            break;
                        }
                    },
                    Err(e) => {
                        error!("Got error {e}");
                        break;
                    },
                }
            },
            msg = rx.recv() => {
                match msg {
                    Ok(msg) => {
                        if let Err(e) = write.write_all(msg.as_bytes()).await {
                            error!("Failed to write line to channel={channel}: {e}");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!("Subscriber lagged channel={channel}; skipped {skipped} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("Channel closed channel={channel}");
                        break;
                    }
                }
            },
        };
    }

    drop(rx);
    if let Some(channel_id) = subscribers.remove_subscriber(&channel) {
        let _ = stats_tx.send(StatEvent::ChannelRemoved { channel_id });
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let runtime = Builder::new_current_thread()
        .worker_threads(1)
        .thread_name("thready")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let local = LocalSet::new();
    runtime.block_on(local.run_until(async {
        let subscribers = Subscribers::new();
        let stats_map = ChannelStatsStore::new();
        let (stats_tx, mut stats_rx) = mpsc::unbounded_channel::<StatEvent>();

        tokio::task::spawn_local({
            let stats_map = stats_map.clone();
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

        // General metrics routine for application metrics every 5s
        tokio::task::spawn_local({
            let stats_map = stats_map.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let mut to_remove = Vec::new();
                    for (channel_id, snapshot) in stats_map.collect_and_reset() {
                        if snapshot.active || snapshot.sent > 0 {
                            info!("{} sent {}", snapshot.name, snapshot.sent);
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

        let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
        loop {
            debug!("Accepting");
            let (stream, _) = listener.accept().await.unwrap();
            let subscribers = subscribers.clone();
            let stats_tx = stats_tx.clone();
            tokio::task::spawn_local(async move {
                debug!("Got stream fd={}", stream.as_raw_fd());
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                trace!("Reading protocol line");
                if let Err(e) = reader.read_line(&mut line).await {
                    error!("Failed to read line: {e}");
                    // TODO: closing?
                    return;
                }
                trace!("Read protocol {line}");
                let stream = reader.into_inner(); // Assume that we only wrote the protocol line
                if line.starts_with("PUBLISH") {
                    let channel = line.trim()[8..].to_string();
                    handle_publisher(stream, channel, subscribers, stats_tx).await;
                } else if line.starts_with("SUBSCRIBE") {
                    let channel = line.trim()[10..].to_string();
                    handle_subscriber(stream, channel, subscribers, stats_tx).await;
                } else {
                    warn!(
                        "Line had length {} but didn't start with expected protocol",
                        line.len()
                    );
                }
                // TODO: Closing?
                debug!("Exiting");
            });
        }

        Ok(())
    }))
}
