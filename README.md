# Twoio

Personal project to learn about `io_uring` and writing an async runtime in Rust. Two because iouring has two queues, io because io. Also I know that there's [monoio](https://docs.rs/monoio/latest/monoio/) which is an actual thing that is useful, this is not.

## How to use

First initialize `io_uring` with the `uring` crate. Then initialize the `executor` and spawn tasks, block on them, and proliferate.

Contrived example (a more complete one is in examples/file_example.rs)

```rust
uring::init(uring::UringArgs::default())?;
executor::init();

executor::block_on({
    async move {
        let mut reader = File::open("file1.txt")
          .context("submitting open for input file")?
          .await
          .with_context(|| format!("opening {}", input.display()))?;
        let mut writer = File::open("file2.txt")
          .context("submitting open for input file")?
          .await
          .with_context(|| format!("opening {}", input.display()))?;
        let mut buf = vec![0u8; 1024];
        let _ = reader.read(&mut buf).await?;
        let _ = writer.write_all(&mut buf).await?;
        println!("Done!");
    }
});
```

What is provided:

* TCP sockets in src/net.rs
* Files (read/write/open/close) in src/file.rs
* WaitGroup in src/sync/wg.rs
* Unbounded channels in src/sync/mpsc.rs

The pubsub example in examples/pubsub_server_async.rs and pubsub_client_async.rs coincidentally implement a small subset of the Redis PubSub protocol.
