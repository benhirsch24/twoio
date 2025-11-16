use log::{debug, error, info, trace, warn};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::runtime::Builder;
use tokio::task::LocalSet;

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::{cell::RefCell, rc::Rc};

struct SubscribersInner {
    sent: u64,
    tx: tokio::sync::broadcast::Sender<String>,
}

#[derive(Clone)]
struct Subscribers {
    inner: Rc<RefCell<HashMap<String, SubscribersInner>>>,
}

impl Subscribers {
    fn get_tx(&mut self, channel: &String) -> tokio::sync::broadcast::Sender<String> {
        let mut inner = self.inner.borrow_mut();
        if !inner.contains_key(channel) {
            let (tx, _rx) = tokio::sync::broadcast::channel(1024);
            inner.insert(
                channel.clone(),
                SubscribersInner {
                    sent: 0,
                    tx: tx.clone(),
                },
            );
            return tx;
        }
        inner.get(channel).unwrap().tx.clone()
    }

    fn get_rx(&mut self, channel: &String) -> tokio::sync::broadcast::Receiver<String> {
        let mut inner = self.inner.borrow_mut();
        if !inner.contains_key(channel) {
            let (tx, rx) = tokio::sync::broadcast::channel(1024);
            inner.insert(channel.clone(), SubscribersInner { sent: 0, tx });
            return rx;
        }
        inner.get(channel).unwrap().tx.subscribe()
    }
}

async fn handle_publisher(
    mut stream: tokio::net::TcpStream,
    channel: String,
    mut subscribers: Subscribers,
) {
    let fd = stream.as_raw_fd();
    debug!("Handling publisher channel={channel} fd={fd}");
    let ok = b"OK\r\n";
    stream.write_all(ok).await.expect("OK");
    trace!("OK fd={fd}");

    let tx = subscribers.get_tx(&channel);

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
                let _ = tx.send(line);
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
    mut subscribers: Subscribers,
) {
    debug!(
        "Handling subscriber channel={channel} fd={}",
        stream.as_raw_fd()
    );
    let ok = b"OK\r\n";
    stream.write_all(ok).await.expect("OK");

    let (read, mut write) = stream.into_split();
    let mut rx = subscribers.get_rx(&channel);

    let mut reader = BufReader::new(read);
    loop {
        let mut line = String::new();
        tokio::select! {
            read_res = reader.read_line(&mut line) => {
                match read_res {
                    Ok(n) => {
                        if n == 0 {
                            debug!("Subscriber left");
                            return;
                        }
                    },
                    Err(e) => {
                        error!("Got error {e}");
                        return;
                    },
                }
            },
            msg = rx.recv() => {
                let msg = msg.expect("unwrapped");
                if let Err(e) = write.write_all(msg.as_bytes()).await {
                    error!("Failed to write line to channel={channel}: {e}");
                }
            },
        };
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
        // Map of channel name to subscriber info
        let subscribers = Subscribers {
            inner: Rc::new(RefCell::new(HashMap::new())),
        };

        // General metrics routine for application metrics every 5s
        tokio::task::spawn_local({
            let subscribers = subscribers.clone();
            async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    {
                        let mut subscribers = subscribers.inner.borrow_mut();
                        // Print number of messages sent per subscriber so far
                        for (channel, subs) in subscribers.iter_mut() {
                            info!("{channel} sent {}", subs.sent);
                            subs.sent = 0;
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
                    handle_publisher(stream, channel, subscribers).await;
                } else if line.starts_with("SUBSCRIBE") {
                    let channel = line.trim()[10..].to_string();
                    handle_subscriber(stream, channel, subscribers).await;
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
