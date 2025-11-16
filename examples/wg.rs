use log::info;

use twoio::executor;
use twoio::sync::wg::WaitGroup;
use twoio::uring;

fn main() -> anyhow::Result<()> {
    env_logger::init();

    uring::init(uring::UringArgs {
        uring_size: 1024,
        submissions_threshold: 1024,
        sqpoll_interval_ms: 0,
    })?;
    executor::init();

    executor::spawn({
        async {
            let mut wg = WaitGroup::new();
            let g1 = wg.add();

            executor::spawn({
                async move {
                    info!("G1");
                    let _g = g1;
                    info!("G1 done");
                }
            });
            info!("WG waiting");
            wg.wait().await;
            info!("WG done");
            uring::exit();
        }
    });

    executor::run();

    Ok(())
}
