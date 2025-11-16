use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use clap_duration::duration_range_value_parse;
use duration_human::{DurationHuman, DurationHumanValidator};
use io_uring::{opcode, types};
use log::{debug, error, info, trace, warn};

use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;

use twoio::uring;

struct CallbackRegistry {
    map: HashMap<u64, Box<dyn FnOnce(i32) -> anyhow::Result<()>>>,
    counter: u64,
}

impl CallbackRegistry {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            // TODO: Reserving special user data 0 for the metrics timeout (for now, until I
            // write a more scalable solution
            counter: 1,
        }
    }

    fn add_callback<T>(&mut self, f: T) -> u64
    where
        T: FnOnce(i32) -> anyhow::Result<()> + 'static,
    {
        let c = self.counter;
        self.map.insert(c, Box::new(f));
        self.counter += 1;
        c
    }

    fn call_back(&mut self, id: u64, res: i32) -> anyhow::Result<()> {
        let cb = self.map.remove(&id);
        match cb {
            Some(cb) => cb(res),
            None => anyhow::bail!("No callback registered for {id} res={res}"),
        }
    }
}

thread_local! {
    static CB: UnsafeCell<CallbackRegistry> = UnsafeCell::new(CallbackRegistry::new());
}

pub fn add_callback<F>(f: F) -> u64
where
    F: FnOnce(i32) -> anyhow::Result<()> + 'static,
{
    CB.with(|cbr| unsafe {
        let cb_ref = &mut *cbr.get();
        cb_ref.add_callback(Box::new(f))
    })
}

pub fn call_back(id: u64, res: i32) -> anyhow::Result<()> {
    CB.with(|cbr| unsafe {
        let cb_ref = &mut *cbr.get();
        cb_ref.call_back(id, res)
    })
}

fn read_line(mut buf: BytesMut) -> Option<Bytes> {
    for idx in 0..buf.len() {
        if buf[idx] == b'\n' && buf[idx - 1] == b'\r' {
            let b = buf.split_to(idx + 1).freeze();
            return Some(b);
        }
    }
    None
}

fn read_message_recursive(
    fd: RawFd,
    done: Rc<RefCell<u32>>,
    end: std::time::Instant,
    msgn: u32,
) -> anyhow::Result<()> {
    let mut read = BytesMut::with_capacity(1024);
    let ptr = read.as_mut_ptr();
    let op = opcode::Recv::new(types::Fd(fd), ptr, read.capacity() as u32);
    let ud = add_callback(move |res| {
        // Expected message1 completion
        if res < 0 {
            anyhow::bail!("Got negative return from receive: {res} fd={}", fd);
        }
        trace!("Received message res={res}");
        unsafe { read.set_len(res.try_into()?) };
        let line = read_line(read).ok_or(anyhow::anyhow!(
            "No message received for subscriber={fd} {msgn}"
        ))?;
        let msg = std::str::from_utf8(line.as_ref())?;
        info!("Subscriber received message {msg} {msgn}");

        if msgn == 2 {
            uring::exit();
            return Ok(());
        }

        if std::time::Instant::now() > end {
            *done.borrow_mut() -= 1;
            if *done.borrow() == 0 {
                uring::exit();
            }
            return Ok(());
        }

        // You lose cancellation but oh well for this example.
        read_message_recursive(fd, done, end, msgn + 1)
    });
    uring::submit(op.build().user_data(ud))?;
    Ok(())
}

fn subscriber(
    channel: String,
    fd: RawFd,
    done: Rc<RefCell<u32>>,
    end: std::time::Instant,
) -> anyhow::Result<()> {
    let send_buffer = BytesMut::with_capacity(4096);
    let mut writer = send_buffer.writer();
    let msg = format!("SUBSCRIBE {channel}\r\n");
    info!("Subscribing \"{}\" fd={fd}", msg);
    let to_send = writer.write(msg.as_bytes())?;
    let mut send_buffer = writer.into_inner();
    let ptr = send_buffer.as_ptr();
    let op = opcode::Send::new(types::Fd(fd), ptr, to_send.try_into().unwrap());
    let ud = add_callback(move |res| {
        // Sending SUBSCRIBE completion
        debug!("Sent subscribe res={res}");
        let _ = send_buffer.split();
        let mut ok = BytesMut::with_capacity(1024);
        let ptr = ok.as_mut_ptr();
        let op = opcode::Recv::new(types::Fd(fd), ptr, ok.capacity() as u32);
        let ud = add_callback(move |res| {
            // Expected OK receive completion
            if res < 0 {
                anyhow::bail!("Got negative return code {res} for fd={fd}");
            }
            debug!("Recv res={res} fd={}", fd);
            unsafe { ok.set_len(res.try_into()?) };
            let line = read_line(ok).expect("There should be one read by now");
            if line != "OK\r\n" {
                anyhow::bail!("Expected OK");
            }
            debug!("Got OK fd={}", fd);

            read_message_recursive(fd, done.clone(), end, 0)
        });
        uring::submit(op.build().user_data(ud))?;
        Ok(())
    });
    let e = op.build().user_data(ud);
    uring::submit(e)?;
    Ok(())
}

fn publish_recursive(fd: RawFd, end: std::time::Instant, pubn: u32) -> anyhow::Result<()> {
    let mut send_buffer = BytesMut::with_capacity(4096);
    send_buffer.put_slice(b"hello\r\n");
    let ptr = send_buffer.as_ptr();
    let to_send = send_buffer.len();
    let send_e = opcode::Send::new(types::Fd(fd), ptr, to_send.try_into().unwrap());
    let ud = add_callback(move |res| {
        let _s = send_buffer;
        info!("Publisher sent message res={res} {pubn}");

        if std::time::Instant::now() > end {
            return Ok(());
        }

        publish_recursive(fd, end, pubn + 1)
    });
    uring::submit(send_e.build().user_data(ud))?;
    Ok(())
}

fn publisher(channel: String, fd: RawFd, end: std::time::Instant) -> anyhow::Result<()> {
    let send_buffer = BytesMut::with_capacity(4096);
    let mut writer = send_buffer.writer();
    let msg = format!("PUBLISH {channel}\r\n");
    info!("Publishing \"{}\"", msg);
    let to_send = writer.write(msg.as_bytes())?;
    let send_buffer = writer.into_inner();
    let ptr = send_buffer.as_ptr();
    debug!("Publisher fd={fd} sending");
    let send_e = opcode::Send::new(types::Fd(fd), ptr, to_send.try_into().unwrap());
    let ud = add_callback(move |res| {
        let _s = send_buffer;
        // TODO: What if send is bigger than res?
        info!("Publisher PUBLISH completed res={res}");
        let mut read_buffer = BytesMut::with_capacity(4096);
        let ptr = read_buffer.as_mut_ptr();
        let read_e = opcode::Recv::new(types::Fd(fd), ptr, read_buffer.capacity() as u32);
        let ud = add_callback(move |res: i32| {
            if res < 0 {
                anyhow::bail!("Got negative return code {res} for fd={fd} for OK read");
            }

            debug!("Publisher OK recv={res}");
            let newlen: usize = read_buffer.len() + (res as usize);
            unsafe { read_buffer.set_len(newlen) };
            let line = read_line(read_buffer).expect("There should be one read by now");
            if line != "OK\r\n" {
                anyhow::bail!("Expected OK");
            }
            debug!("Publisher received ok");

            publish_recursive(fd, end, 0)
        });
        let e = read_e.build().user_data(ud);
        uring::submit(e)?;
        Ok(())
    });
    uring::submit(send_e.build().user_data(ud))?;
    Ok(())
}

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
}
fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    uring::init(uring::UringArgs {
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: args.sqpoll_interval_ms,
    })?;

    // arm a heartbeat timeout
    let ts = types::Timespec::new().sec(5).nsec(0);
    let timeout = opcode::Timeout::new(&ts as *const types::Timespec)
        .flags(io_uring::types::TimeoutFlags::MULTISHOT)
        .count(0)
        .build()
        .user_data(0);
    uring::submit(timeout).expect("arm timeout");

    // Spawn 5 subscribers
    let mut connections = HashMap::new();
    let start = std::time::Instant::now();
    let end = args.timeout + start;
    info!("going until {end:?}");
    let done = Rc::new(RefCell::new(
        args.publishers * args.subscribers_per_publisher,
    ));
    for i in 0..args.publishers {
        // Set up subscribers first
        let channel = format!("Channel{i}");
        for _ in 0..args.subscribers_per_publisher {
            let subscriber_conn = TcpStream::connect("127.0.0.1:8080")?;
            let fd = subscriber_conn.as_raw_fd();
            connections.insert(subscriber_conn.as_raw_fd(), subscriber_conn);
            subscriber(channel.clone(), fd, done.clone(), end)?;
        }

        std::thread::sleep(std::time::Duration::from_millis(50));

        // Then publisher who will send to the subscribers
        let conn = TcpStream::connect("127.0.0.1:8080").expect("publish conn");
        let fd = conn.as_raw_fd();
        connections.insert(fd, conn);
        publisher(channel.clone(), fd, end)?;
        debug!("Publisher {}", fd);
    }

    if let Err(e) = uring::run(
        move |ud, res, _flags| {
            trace!("Got completion event ud={ud} res={res}");
            match ud {
                // Timeout
                0 => {
                    if res != -62 {
                        warn!("Timeout result not 62: {}", res);
                    }

                    let stats = uring::stats()?;
                    info!("Metrics: {}", stats);
                }
                // Callback registry
                _ => call_back(ud, res)?,
            }
            Ok(())
        },
        || {},
    ) {
        error!("Error running uring: {}", e);
    }
    info!("Exiting");
    Ok(())
}
