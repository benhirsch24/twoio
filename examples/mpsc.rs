use twoio::executor;
use twoio::sync::mpsc::*;
use twoio::uring;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    uring::init(uring::UringArgs {
        uring_size: 1024,
        submissions_threshold: 1024,
        sqpoll_interval_ms: 0,
    })?;
    executor::init();

    let (mut rx, mut tx) = channel();

    const N: i32 = 5;
    executor::spawn(async move {
        for i in 0..N {
            let _ = tx.send(i);
        }
    });

    executor::spawn(async move {
        loop {
            match rx.recv().await {
                Some(i) => {
                    println!("Got {i}");
                    if i == N - 2 {
                        rx.close();
                    }
                }
                None => {
                    uring::exit();
                    return;
                }
            }
        }
    });

    executor::run();

    println!("Done");

    Ok(())
}
