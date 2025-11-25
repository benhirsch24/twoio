use twoio::executor;
use twoio::uring;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    uring::init(uring::UringArgs {
        uring_size: 1024,
        submissions_threshold: 1024,
        sqpoll_interval_ms: 0,
    })?;
    executor::init();

    let mut handles = Vec::new();
    for i in 0..5 {
        handles.push(executor::spawn(async move {
            let mut t = twoio::timeout::ticker(std::time::Duration::from_secs(i));
            let mut n = 0;
            loop {
                t = t.await.unwrap();
                n += 1;
                if n == 2 {
                    println!("{i}");
                    break;
                }
            }
            i
        }));
    }

    executor::block_on(async move {
        let all = futures::future::join_all(handles).await;
        println!("{all:?}");
    });

    executor::run();

    println!("Done");

    Ok(())
}
