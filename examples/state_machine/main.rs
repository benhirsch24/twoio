use clap::Parser;
use histogram::Histogram;
use io_uring::{opcode, types};
use log::{debug, error, info, trace, warn};

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::rc::Rc;

mod connection;
use connection::*;
mod pubsub;
use pubsub::BufferPool;

use twoio::uring;
use twoio::user_data::{Op, UserData};

const SILLY_TEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/assets/silly_text.txt"
));

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Chunk size to send the file in chunks of
    #[arg(short, long, default_value_t = 4096)]
    chunk_size: usize,

    /// Size of uring submission queue
    #[arg(short, long, default_value_t = 4096)]
    uring_size: u32,

    /// Number of submissions in the backlog before submitting to uring
    #[arg(short, long, default_value_t = 1024)]
    submissions_threshold: usize,

    /// Interval for kernel submission queue polling. If 0 then sqpoll is disabled. Default 0.
    #[arg(short = 'i', long, default_value_t = 0)]
    sqpoll_interval_ms: u32,
}

fn main() -> Result<(), std::io::Error> {
    env_logger::init();
    let args = Args::parse();
    info!("{args:?}");

    // Here's our super simple statically allocated cache
    let mut cache = Rc::new(HashMap::<String, String>::new());
    let ps_state = Rc::new(RefCell::new(pubsub::PubsubState::new()));
    Rc::get_mut(&mut cache)
        .unwrap()
        .insert("1".to_string(), SILLY_TEXT.to_string());

    uring::init(uring::UringArgs {
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: args.sqpoll_interval_ms,
    })?;

    // Arm multi-shot accept so we don't have to continually resubmit
    let listener = std::net::TcpListener::bind("0.0.0.0:8080").expect("tcp listener");
    let lfd = types::Fd(listener.as_raw_fd());
    let accept_ud = UserData::new(Op::Accept, 0);
    let accept_e = opcode::AcceptMulti::new(lfd)
        .build()
        .user_data(accept_ud.into_u64());
    uring::submit(accept_e).expect("arm accept");

    // Add the first timeout
    let timeout_ud = UserData::new(Op::Timeout, 0);
    let ts = types::Timespec::new().sec(5).nsec(0);
    let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .count(0)
        .build()
        .user_data(timeout_ud.into_u64());
    uring::submit(timeout).expect("arm timeout");

    let mut connections = HashMap::new();
    let buffer_pool = BufferPool::new();
    let write_timing_histogram = Rc::new(RefCell::new(Histogram::new(7, 64).expect("histogram")));
    if let Err(e) = uring::run(
        move |ud, res, flags| {
            let ud =
                UserData::try_from(ud).map_err(|_| anyhow::anyhow!("failed userdata extract"))?;
            let fd = ud.fd();
            let op = ud
                .op()
                .map_err(|e| anyhow::anyhow!("unknown op code {e}"))?;
            match op {
                Op::Timeout => {
                    if res != -62 {
                        warn!("Timeout result not 62: {}", res);
                    }

                    let stats = uring::stats()?;
                    info!("Metrics: {}", stats);

                    debug!("submit_and_wait batch sizes");
                    let percentiles = [0.0, 50.0, 90.0, 99.0, 99.9, 99.99, 100.0];
                    if let Some(ps) = stats.submit_and_wait_batch_size.percentiles(&percentiles)? {
                        for p in ps {
                            debug!("p{} range={:?} count={}", p.0, p.1.range(), p.1.count());
                        }
                    }

                    let percentiles = [0.0, 50.0, 90.0, 99.0, 100.0];
                    if let Some(ps) = write_timing_histogram.borrow().percentiles(&percentiles)? {
                        debug!(
                            "Median time betwen writes for a request={}",
                            ps[1].1.count()
                        );
                    }
                }
                Op::Accept => {
                    if res == 127 {
                        error!("Failed to accept");
                        return Ok(());
                    }
                    debug!("Accept! flags: {} result: {} ud: {}", flags, res, ud);

                    // Create a new request object around this file descriptor and enqueue the
                    // first read
                    let mut conn = Connection::new(
                        types::Fd(res),
                        cache.clone(),
                        ps_state.clone(),
                        buffer_pool.clone(),
                        write_timing_histogram.clone(),
                        args.chunk_size,
                    );
                    conn.handle(op, res)?;
                    connections.insert(res, conn);
                }
                Op::Recv => {
                    trace!("Recv CQE result={} flags={} user_data={}", res, flags, ud);
                    if res == -1 {
                        error!("Request error: {}", fd);
                        unsafe {
                            libc::close(fd);
                        };
                        connections.remove(&fd);
                        return Ok(());
                    }

                    if res < 0 {
                        let error = match -res {
                            libc::EFAULT => "efault",
                            libc::EPIPE => "epipe",
                            libc::EIO => "eio",
                            libc::EINVAL => "einval",
                            libc::EBADF => "ebadf",
                            104 => "connection reset by peer",
                            _ => "other",
                        };
                        error!("Error reading: {} {} {}", fd, res, error);
                        unsafe {
                            libc::close(fd);
                        };
                        connections.remove(&fd);
                        return Ok(());
                    }

                    // Get the request out of our outstanding connections hashmap
                    let conn = match connections.get_mut(&fd) {
                        Some(r) => r,
                        None => {
                            warn!(
                                "No outstanding request for flags: {} result: {} ud: {}",
                                flags, res, ud
                            );
                            return Ok(());
                        }
                    };

                    match conn.handle(op, res) {
                        Ok(ConnectionResult::Done) => {
                            unsafe {
                                libc::close(fd);
                            };
                            connections.remove(&fd);
                        }
                        Ok(_) => trace!("handled"),
                        Err(e) => {
                            error!("Error when handling: {}", e);
                            return Ok(());
                        }
                    };
                }
                Op::Send => {
                    trace!("Send CQE result={} flags={} user_data={}", res, flags, ud);
                    if res == -1 {
                        error!("Request error: {}", fd);
                        unsafe {
                            libc::close(fd);
                        };
                        connections.remove(&fd);
                        return Ok(());
                    }
                    if res == 0 {
                        unsafe {
                            libc::close(fd);
                        };
                        connections.remove(&fd);
                        return Ok(());
                    }
                    if res < 0 {
                        let error = match -res {
                            libc::EFAULT => "efault",
                            libc::EPIPE => "epipe",
                            libc::EIO => "eio",
                            libc::EINVAL => "einval",
                            libc::EBADF => "ebadf",
                            104 => "connection reset by peer",
                            _ => "other",
                        };
                        error!("Error writing: {} {} {}", fd, res, error);
                        unsafe {
                            libc::close(fd);
                        };
                        connections.remove(&fd);
                        return Ok(());
                    }

                    // Get the request out of our outstanding connections hashmap
                    let conn = match connections.get_mut(&fd) {
                        Some(r) => r,
                        None => {
                            warn!(
                                "No outstanding request for flags: {} result: {} ud: {}",
                                flags, res, ud
                            );
                            return Ok(());
                        }
                    };

                    match conn.handle(op, res) {
                        Ok(ConnectionResult::Done) => {
                            debug!("Closing after complete send");
                            unsafe {
                                libc::close(fd);
                            };
                            connections.remove(&fd);
                        }
                        Ok(_) => trace!("handled"),
                        Err(e) => {
                            error!("Error when handling: {}", e);
                            return Ok(());
                        }
                    };
                }
                _ => {
                    warn!(
                        "Unrecognized opcode cqe flags={} result={} user_data={}",
                        flags, res, ud
                    );
                }
            }
            Ok(())
        },
        || {},
    ) {
        error!("Error running uring: {}", e);
    }
    Ok(())
}
