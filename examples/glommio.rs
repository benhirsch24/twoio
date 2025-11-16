use std::collections::HashMap;
use std::io::Result;
use std::rc::Rc;

use bytes::Bytes;
use clap::Parser;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::{net::TcpListener, prelude::*};
use log::{debug, error, info, warn};

static BUFFER_SIZE: usize = 1024 * 1024;
const HEALTH_OK: &str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
const NOT_FOUND: &str = "HTTP/1.1 404 Not Found\r\nContent-Length: 8\r\n\r\nNotFound";
const BAD_REQUEST: &str =
    "HTTP/1.1 400 Bad Request\r\nContent-Length: 25\r\n\r\nExpected /object/<object>";
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
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    info!("Using chunk size {}", args.chunk_size);
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1));
    let handle = builder.name("server").spawn(move || async move {
        let mut cache = Rc::new(HashMap::new());
        Rc::get_mut(&mut cache)
            .unwrap()
            .insert("1".to_string(), SILLY_TEXT.to_string());

        // Server implementation
        let listener = TcpListener::bind("0.0.0.0:8080").unwrap();
        info!("Listening on {}", listener.local_addr().unwrap());
        loop {
            let mut stream = listener.accept().await.unwrap();
            // Detach this stream handling so I don't have to await each one
            let cache = cache.clone();
            glommio::spawn_local(async move {
                let mut buffer = vec![0u8; BUFFER_SIZE];
                let n = stream.read(&mut buffer).await.unwrap();

                let mut headers = [httparse::EMPTY_HEADER; 16];
                let mut r = httparse::Request::new(headers.as_mut_slice());
                let request = r.parse(&buffer[..n]).expect("parse error");

                // Not finished with request, keep reading
                if !request.is_complete() {
                    warn!("request not complete");
                    return;
                }

                // We have a request, let's route
                match r.path {
                    Some(path) => match path {
                        p if p.starts_with("/object") => {
                            let parts = p.split("/").collect::<Vec<_>>();
                            if parts.len() != 3 {
                                stream.write(BAD_REQUEST.as_bytes()).await.unwrap();
                            } else if let Some(o) = cache.get(&parts[2].to_string()) {
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                                    o.len(),
                                    o
                                );
                                let bs = Bytes::copy_from_slice(resp.as_bytes());
                                let mut start = 0;
                                loop {
                                    if start == bs.len() {
                                        return;
                                    }
                                    let end = if start + args.chunk_size > bs.len() {
                                        bs.len()
                                    } else {
                                        start + args.chunk_size
                                    };
                                    match stream.write(&bs.slice(start..end)).await {
                                        Ok(n) => {
                                            debug!("Wrote {n}");
                                            start += n
                                        }
                                        Err(e) => error!("Did nto write all {}", e),
                                    };
                                }
                            } else {
                                stream.write(NOT_FOUND.as_bytes()).await.unwrap();
                            }
                        }
                        "/health" => {
                            stream.write(HEALTH_OK.as_bytes()).await.unwrap();
                        }
                        _ => {
                            stream.write(NOT_FOUND.as_bytes()).await.unwrap();
                        }
                    },
                    None => {
                        stream.write(NOT_FOUND.as_bytes()).await.unwrap();
                    }
                }
            })
            .detach();
        }
    })?;
    handle.join().unwrap();
    Ok(())
}
