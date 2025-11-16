use bytes::{Bytes, BytesMut};
use histogram::Histogram;
use io_uring::{opcode, squeue::Entry, types};
use log::{debug, info, trace, warn};

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use twoio::uring;
use twoio::user_data::{Op, UserData};

use crate::pubsub::{Buffer, BufferPool, PubsubState};

static BUFFER_SIZE: usize = 1024 * 1024;

const NOT_FOUND: &str = "HTTP/1.1 404 Not Found\r\nContent-Length: 8\r\n\r\nNotFound";
const HEALTH_OK: &str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

pub enum ConnectionResult {
    Continue,
    Done,
}

#[derive(Debug)]
enum ConnectionState {
    Init,
    Http,
    Publisher(String),
    Subscriber(String),
}

impl ConnectionState {
    fn get_channel(&self) -> String {
        match self {
            ConnectionState::Publisher(c) => c.clone(),
            ConnectionState::Subscriber(c) => c.clone(),
            _ => panic!("No channel"),
        }
    }

    fn is_subscriber(&self) -> bool {
        matches!(self, ConnectionState::Subscriber(_))
    }
}

pub struct Connection {
    fd: types::Fd,
    read_buffer: BytesMut,
    read_start: usize,
    response: Option<Bytes>,
    cache: Rc<HashMap<String, String>>,
    connection_state: ConnectionState,
    ps_state: Rc<RefCell<PubsubState>>,
    buffer_pool: BufferPool,
    sent: usize,
    chunk_size: usize,
    write_timing_histogram: Rc<RefCell<Histogram>>,
    last_write: Option<std::time::Instant>,
}

impl Connection {
    pub fn new(
        fd: types::Fd,
        cache: Rc<HashMap<String, String>>,
        ps_state: Rc<RefCell<PubsubState>>,
        buffer_pool: BufferPool,
        write_timing_histogram: Rc<RefCell<Histogram>>,
        chunk_size: usize,
    ) -> Connection {
        Connection {
            fd,
            read_buffer: BytesMut::with_capacity(BUFFER_SIZE),
            read_start: 0,
            response: None,
            connection_state: ConnectionState::Init,
            cache,
            ps_state,
            buffer_pool,
            sent: 0,
            chunk_size,
            write_timing_histogram,
            last_write: None,
        }
    }

    pub fn set_response(&mut self, resp: &str) {
        // Store response in this connection as it needs to be allocated until the kernel has sent it
        // Eventually this could be a pointer to kernel owned buffer
        self.response = Some(Bytes::copy_from_slice(resp.as_bytes()));
    }

    pub fn read(&mut self) -> Entry {
        let ptr = unsafe { self.read_buffer.as_mut_ptr().add(self.read_buffer.len()) };
        let read_e = opcode::Recv::new(
            self.fd,
            ptr,
            (self.read_buffer.capacity() - self.read_buffer.len()) as u32,
        );
        let ud = UserData::new(Op::Recv, self.fd.0);
        trace!("read: {} {:?}", ud, self.fd);
        read_e.build().user_data(ud.into_u64())
    }

    // Advances the internal read buffer by setting the new length.
    fn advance_read(&mut self, n: usize) {
        unsafe { self.read_buffer.set_len(self.read_buffer.len() + n) };
    }

    // Send from the internal buffer.
    // If there is no data left in the buffer it returns None.
    // Otherwise it returns an sqeueue::Entry.
    fn send(&mut self, n: usize) -> Option<Entry> {
        self.sent += n;
        let len = self.response.as_ref().unwrap().len();
        if self.sent == len {
            return None;
        }

        let start = self.sent;
        let end = if start + self.chunk_size > len {
            len
        } else {
            start + self.chunk_size
        };
        let ptr = self.response.as_ref().unwrap().slice(start..end).as_ptr();
        let to_send: u32 = (end - start) as u32;
        let send_e = opcode::Send::new(self.fd, ptr, to_send);
        let ud = UserData::new(Op::Send, self.fd.0);

        // Stats on time between writes
        if let Some(n) = self.last_write {
            self.write_timing_histogram
                .borrow_mut()
                .increment(n.elapsed().as_micros() as u64)
                .expect("increment");
        }
        self.last_write = Some(std::time::Instant::now());

        Some(send_e.build().user_data(ud.into_u64()))
    }

    fn serve(&mut self) -> anyhow::Result<ConnectionResult> {
        match self.connection_state {
            ConnectionState::Init => {
                // Look at first 4 characters to see if it's PUBL or SUBS or not.
                // TODO: Could probably do more error handling idk
                if self.read_buffer.starts_with(b"PUBL") {
                    self.init_publisher()
                } else if self.read_buffer.starts_with(b"SUBS") {
                    self.init_subscriber()
                } else {
                    self.connection_state = ConnectionState::Http;
                    self.serve_http()
                }
            }
            ConnectionState::Publisher(_) => self.publish_recv(),
            ConnectionState::Subscriber(_) => self.subscribe_recv(),
            ConnectionState::Http => {
                unimplemented!("Keep alive and multiple reads not implemented yet")
            }
        }
    }

    fn init_publisher(&mut self) -> anyhow::Result<ConnectionResult> {
        // Protocol looks like
        // PUBLISH channel\r\n
        // Message1\r\n
        // Message2\r\n
        // CLOSE\r\n

        // First check the protocol matches. Expect at least 8 for "PUBLISH "
        // TODO: Error handling
        // TODO: Will an error here close the socket? It should.
        let line = match self.read_line() {
            Some(l) => l,
            None => {
                return Ok(ConnectionResult::Continue);
            }
        };

        let line = std::str::from_utf8(line.as_ref()).unwrap().trim();
        if !line.starts_with("PUBLISH") {
            anyhow::bail!("Expected publish");
        }

        let channel = line[8..].to_string();

        info!("Found channel {channel}");
        self.read_start += self.read_buffer.len();

        // Let's assume that we don't get any more than just the publish message
        // TODO: Initialize and publish a message.

        // Write back OK
        self.set_response("OK\r\n");
        let send = self
            .send(0)
            .ok_or(anyhow::anyhow!("first send should be some"))?;
        uring::submit(send)?;
        let read = self.read();
        uring::submit(read)?;
        self.connection_state = ConnectionState::Publisher(channel);

        Ok(ConnectionResult::Continue)
    }

    fn init_subscriber(&mut self) -> anyhow::Result<ConnectionResult> {
        // First check the protocol matches. Expect at least 10 for "SUBSCRIBE "
        // TODO: Error handling
        // TODO: Will an error here close the socket? It should.
        let line = match self.read_line() {
            Some(l) => l,
            None => {
                return Ok(ConnectionResult::Continue);
            }
        };

        let line = std::str::from_utf8(line.as_ref()).unwrap().trim();
        if !line.starts_with("SUBSCRIBE") {
            anyhow::bail!("Expected subscribe");
        }

        let channel = line[10..].to_string();
        info!("Subscribing to {channel}");
        self.ps_state
            .borrow_mut()
            .subscribe(channel.clone(), self.fd.0);

        // Write back OK
        self.set_response("OK\r\n");
        let send = self
            .send(0)
            .ok_or(anyhow::anyhow!("first send should be some"))?;
        uring::submit(send)?;
        self.connection_state = ConnectionState::Subscriber(channel);

        // Right now this just checks for the socket closing
        let read = self.read();
        uring::submit(read)?;

        Ok(ConnectionResult::Continue)
    }

    fn read_line(&mut self) -> Option<Bytes> {
        for idx in self.read_start..self.read_buffer.len() {
            if self.read_buffer[idx] == b'\n' && self.read_buffer[idx - 1] == b'\r' {
                let buf = self.read_buffer.split_to(idx + 1).freeze();
                self.read_start = 0;
                return Some(buf);
            }
        }
        None
    }

    fn publish_recv(&mut self) -> anyhow::Result<ConnectionResult> {
        // Get messages from buffer. Each message is one line.
        let mut messages = vec![];
        while let Some(line) = self.read_line() {
            debug!("Read msg {}", std::str::from_utf8(line.as_ref()).unwrap());
            messages.push(line);
        }
        let channel = match &self.connection_state {
            ConnectionState::Publisher(c) => c,
            _ => panic!("unexpected"),
        };
        let subscribers = self.ps_state.borrow().get_subscribers(channel);
        info!(
            "There are {} subscribers for {}, messages: {:?} ({})",
            subscribers.len(),
            channel,
            messages,
            messages.len()
        );

        // Don't bother allocating buffers and such when there's no subscribers
        if subscribers.is_empty() {
            debug!("No subscribers for {channel}, not doing anything");
            let e = self.read();
            uring::submit(e)?;
            return Ok(ConnectionResult::Continue);
        }

        // Send each message to each subscriber
        for message in messages {
            // Create a new buffer with a refcount per subscriber
            let mut buf = Buffer::new(message, subscribers.clone());
            for s in buf.get_sends() {
                uring::submit(s)?;
            }
            self.buffer_pool.register(channel.clone(), buf);
        }

        // Issue read for next message
        let e = self.read();
        uring::submit(e)?;
        Ok(ConnectionResult::Continue)
    }

    fn subscribe_recv(&mut self) -> anyhow::Result<ConnectionResult> {
        // The only valid read result from a subscriber is a BYE message
        if let Some(msg) = self.read_line()
            && &msg == "BYE\r\n"
        {
            let channel = match &self.connection_state {
                ConnectionState::Subscriber(c) => c,
                _ => panic!("unexpected"),
            };
            info!("Bye {channel}");
            return Ok(ConnectionResult::Done);
        }
        Ok(ConnectionResult::Continue)
    }

    fn subscribe_send(&mut self, n: usize) -> anyhow::Result<ConnectionResult> {
        let c = self.connection_state.get_channel();
        let e = self.buffer_pool.get_send_for_buffer(&c, self.fd.0, n);
        match e {
            Some(e) => {
                uring::submit(e)?;
            }
            None => {
                if self.buffer_pool.is_done(&c) {
                    info!("Buffer pool is done for {c}");
                    self.buffer_pool.remove(&c);
                }
            }
        };
        Ok(ConnectionResult::Continue)
    }

    fn close(&mut self) -> anyhow::Result<()> {
        match &self.connection_state {
            ConnectionState::Subscriber(c) => {
                info!("Closing subscriber for {c}");
                self.ps_state.borrow_mut().remove(c, self.fd.0);
            }
            ConnectionState::Publisher(_) => {
                info!("Closing publisher");
            }
            _ => log::error!(
                "Called close for unexpected connection state: {:?}",
                self.connection_state
            ),
        }
        Ok(())
    }

    fn serve_http(&mut self) -> anyhow::Result<ConnectionResult> {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut r = httparse::Request::new(headers.as_mut_slice());
        let request = r
            .parse(&self.read_buffer[..])
            .map_err(|e| anyhow::anyhow!("parse error: {e}"))?;

        // Not finished with request, keep reading
        if !request.is_complete() {
            let e = self.read();
            uring::submit(e)?;
            return Ok(ConnectionResult::Continue);
        }

        // We have a request, let's route
        match r.path {
            Some(path) => match path {
                p if p.starts_with("/object") => {
                    let parts = p.split("/").collect::<Vec<_>>();
                    if parts.len() != 3 {
                        self.set_response("HTTP/1.1 400 Bad Request\r\nContent-Length: 25\r\n\r\nExpected /object/<object>");
                        let send = self
                            .send(0)
                            .ok_or(anyhow::anyhow!("first send should be some"))?;
                        uring::submit(send)?;
                    } else if let Some(o) = self.cache.get(&parts[2].to_string()) {
                        self.set_response(&format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                            o.len(),
                            o
                        ));
                        let send = self
                            .send(0)
                            .ok_or(anyhow::anyhow!("first send should be some"))?;
                        uring::submit(send)?;
                    } else {
                        self.set_response(NOT_FOUND);
                        let send = self
                            .send(0)
                            .ok_or(anyhow::anyhow!("first send should be some"))?;
                        uring::submit(send)?;
                    }
                }
                "/health" => {
                    self.set_response(HEALTH_OK);
                    let send = self
                        .send(0)
                        .ok_or(anyhow::anyhow!("first send should be some"))?;
                    uring::submit(send)?;
                }
                _ => {
                    self.set_response(NOT_FOUND);
                    let send = self
                        .send(0)
                        .ok_or(anyhow::anyhow!("first send should be some"))?;
                    uring::submit(send)?;
                }
            },
            None => {
                self.set_response(NOT_FOUND);
                let send = self
                    .send(0)
                    .ok_or(anyhow::anyhow!("first send should be some"))?;
                uring::submit(send)?;
            }
        }
        Ok(ConnectionResult::Continue)
    }

    pub fn handle(&mut self, op: Op, result: i32) -> anyhow::Result<ConnectionResult> {
        match op {
            Op::Accept => {
                // Just submit a read op
                uring::submit(self.read()).expect("read submit");
            }
            Op::Recv => {
                if result == 0 {
                    self.close()?;
                    return Ok(ConnectionResult::Done);
                }

                // Advance the connection internal offset pointer by how many bytes were read
                // (the result value of the read call)
                self.advance_read(result as usize);

                // Parse the data and submit any calls to the submission queue
                self.serve()?;
            }
            Op::Send => {
                if self.connection_state.is_subscriber() {
                    debug!("Got send for subscriber fd={} res={result}", self.fd.0);
                    // We expect the first send to finish
                    if self.sent < self.response.as_ref().unwrap().len() {
                        self.send(result.try_into().unwrap());
                        return Ok(ConnectionResult::Continue);
                    }

                    // If we've sent the full response then this is a subscribe send
                    return self.subscribe_send(result.try_into().unwrap());
                }

                // TODO: No http keep alive implemented yet
                let res = self.send(result.try_into().unwrap());
                let done = res.is_none();
                if let Some(e) = res {
                    uring::submit(e)?;
                }

                if let ConnectionState::Http = self.connection_state
                    && done
                {
                    return Ok(ConnectionResult::Done);
                }
            }
            _ => warn!("Didn't expect op {:?}", op),
        }
        Ok(ConnectionResult::Continue)
    }
}
