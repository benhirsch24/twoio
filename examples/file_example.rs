use std::fs::OpenOptions;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use log::{error, info};

use twoio::executor;
use twoio::file::File;
use twoio::uring;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Minimal example using the io_uring File implementation"
)]
struct Args {
    #[arg(long, default_value = "assets/silly_text.txt")]
    input: PathBuf,

    #[arg(long, default_value = "/tmp/silly_copy.txt")]
    output: PathBuf,

    #[arg(long, default_value_t = 1024)]
    uring_size: u32,

    #[arg(long, default_value_t = 256)]
    submissions_threshold: usize,
}

async fn copy_file(input: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    let mut reader = File::open(&input)
        .context("submitting open for input file")?
        .await
        .with_context(|| format!("opening {}", input.display()))?;
    let mut writer = File::open(&output)
        .context("submitting open for output file")?
        .await
        .with_context(|| format!("opening {}", output.display()))?;

    let mut buffer = Vec::new();
    let read_size = 32;
    let mut len = 0;
    loop {
        buffer.reserve(read_size);
        info!("Buffer len {} cap {}", buffer.len(), buffer.capacity());
        let spare = buffer.spare_capacity_mut();
        let slice =
            unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len()) };
        let n = reader.read(slice).await.context("reading")?;
        info!("Read {n} bytes");
        unsafe {
            buffer.set_len(len + n);
        }
        len += n;
        if n < read_size {
            break;
        }
    }
    info!("Read {} bytes from {}", buffer.len(), input.display());
    info!("{}", std::str::from_utf8(&buffer[..5])?);

    writer
        .write_all(&buffer)
        .await
        .context("writing copied payload")?;
    writer.flush().await.context("flushing writer")?;
    writer.close().await.context("closing writer")?;
    info!("Wrote {} bytes to {}", buffer.len(), output.display());

    Ok(())
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    // Ensure the destination exists before opening it with O_RDWR.
    OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&args.output)
        .with_context(|| format!("creating {}", args.output.display()))?;

    uring::init(uring::UringArgs {
        uring_size: args.uring_size,
        submissions_threshold: args.submissions_threshold,
        sqpoll_interval_ms: 0,
    })?;
    executor::init();

    executor::spawn({
        let input = args.input.clone();
        let output = args.output.clone();
        async move {
            match copy_file(input, output).await {
                Ok(_) => info!("Copy complete"),
                Err(e) => error!("copy failed: {e:?}"),
            }
            uring::exit();
        }
    });

    executor::run();

    Ok(())
}
