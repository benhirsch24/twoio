use histogram::Histogram;
use io_uring::{IoUring, squeue::Entry};
use log::{error, trace};

use std::cell::UnsafeCell;

pub struct UringArgs {
    pub uring_size: u32,
    pub submissions_threshold: usize,
    pub sqpoll_interval_ms: u32,
}

impl Default for UringArgs {
    fn default() -> UringArgs {
        UringArgs {
            uring_size: 1024,
            submissions_threshold: 256,
            sqpoll_interval_ms: 0,
        }
    }
}

#[derive(Clone)]
pub struct UringStats {
    pub submitted_last_period: u64,
    pub completions_last_period: u64,
    pub submit_and_wait: u64,
    pub to_submit_backlog: usize,
    pub outstanding: u64,
    pub submission_count: u64,
    pub last_submit: std::time::Instant,
    pub submit_and_wait_batch_size: Histogram,
}

impl std::fmt::Display for UringStats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "submitted_last_period={} completions_last_period={} submit_and_wait={} outstanding={} submission_count={} last_submit={}us",
            self.submitted_last_period,
            self.completions_last_period,
            self.submit_and_wait,
            self.outstanding,
            self.submission_count,
            self.last_submit.elapsed().as_micros()
        )
    }
}

impl UringStats {
    fn new() -> UringStats {
        UringStats {
            submitted_last_period: 0,
            completions_last_period: 0,
            submit_and_wait: 0,
            to_submit_backlog: 0,
            outstanding: 0,
            submission_count: 0,
            last_submit: std::time::Instant::now(),
            submit_and_wait_batch_size: Histogram::new(7, 64).expect("sawbs histo"),
        }
    }
}

pub struct Uring {
    uring: IoUring,
    args: UringArgs,
    to_submit: Vec<Entry>,
    stats: UringStats,
    done: bool,
    outstanding: u64,
    submission_count: u64,
}

// Unsafe cell is used because we are using thread-local storage and there will be only one uring
// active per thread.
// This allows us to call uring::run(...); and then from within the run function or the passed-in
// request factory function call uring::submit(...);
thread_local! {
    static URING: UnsafeCell<Option<Uring>> = const { UnsafeCell::new(None) };
}

pub fn init(args: UringArgs) -> Result<(), std::io::Error> {
    URING.with(|uring| {
        unsafe {
            let uring_ref = &mut *uring.get();
            if uring_ref.is_some() {
                return Ok(());
            }

            let new_uring = Uring::new(args)?;
            *uring_ref = Some(new_uring);
        }
        Ok(())
    })
}

pub fn run<H, F>(handler: H, done: F) -> Result<(), anyhow::Error>
where
    H: FnMut(u64, i32, u32) -> Result<(), anyhow::Error>,
    F: FnMut() -> bool,
{
    URING.with(|uring| unsafe {
        let uring_ref = &mut *uring.get();
        let uring_mut = uring_ref.as_mut().unwrap();
        uring_mut.run(handler, done)
    })?;
    Ok(())
}

pub fn submit(sqe: Entry) -> Result<(), std::io::Error> {
    URING.with(|uring| unsafe {
        let uring_ref = &mut *uring.get();
        match uring_ref.as_mut() {
            Some(u) => u.submit(sqe),
            None => panic!("uring not initialized"),
        }
    });
    Ok(())
}

pub fn submission_count() -> u64 {
    URING.with(|uring| unsafe {
        let uring_ref = &mut *uring.get();
        match uring_ref.as_ref() {
            Some(u) => u.submission_count,
            None => 0,
        }
    })
}

pub fn exit() {
    URING.with(|uring| unsafe {
        let uring_ref = &mut *uring.get();
        match uring_ref.as_mut() {
            Some(u) => u.exit(),
            None => panic!("uring not initialized"),
        }
    });
}

pub fn stats() -> Result<UringStats, std::io::Error> {
    URING.with(|uring| unsafe {
        let uring_ref = &mut *uring.get();
        let uring_mut = uring_ref.as_mut().unwrap();
        let mut stats = uring_mut.stats.clone();
        stats.to_submit_backlog = uring_mut.to_submit.len();
        stats.outstanding = uring_mut.outstanding;
        stats.submission_count = uring_mut.submission_count;
        uring_mut.stats = UringStats::new();
        Ok(stats)
    })
}

impl Uring {
    fn new(args: UringArgs) -> Result<Uring, std::io::Error> {
        let uring: IoUring<io_uring::squeue::Entry, io_uring::cqueue::Entry> =
            if args.sqpoll_interval_ms > 0 {
                IoUring::builder()
                    .setup_cqsize(args.uring_size * 2)
                    .setup_sqpoll(args.sqpoll_interval_ms)
                    .build(args.uring_size)?
            } else {
                IoUring::builder()
                    .setup_cqsize(args.uring_size * 2)
                    .build(args.uring_size)?
            };
        Ok(Uring {
            uring,
            args,
            done: false,
            to_submit: Vec::new(),
            stats: UringStats::new(),
            outstanding: 0,
            submission_count: 0,
        })
    }

    fn exit(&mut self) {
        self.done = true;
    }

    fn run<H, F>(
        &mut self,
        mut completion_handler: H,
        mut completions_done_handler: F,
    ) -> Result<(), anyhow::Error>
    where
        H: FnMut(u64, i32, u32) -> Result<(), anyhow::Error>,
        F: FnMut() -> bool,
    {
        loop {
            let mut completed = 0;

            // Check for completions
            self.uring.completion().sync();
            for e in self.uring.completion() {
                self.stats.completions_last_period += 1;
                completed += 1;
                self.outstanding = self.outstanding.saturating_sub(1);
                trace!("completion result={} op={}", e.result(), e.user_data());
                if let Err(err) = completion_handler(e.user_data(), e.result(), e.flags()) {
                    error!(
                        "Error handling cqe (op={} res={}): {err}",
                        e.user_data(),
                        e.result()
                    );
                }
            }
            let had_submission_activity = completions_done_handler();

            if self.done {
                break;
            }

            // Submit N batches of size threshold
            let num_batches = self.to_submit.len() / self.args.submissions_threshold;
            for _ in 0..num_batches {
                let batch: Vec<io_uring::squeue::Entry> = self
                    .to_submit
                    .drain(0..self.args.submissions_threshold)
                    .collect();
                if batch.is_empty() {
                    continue;
                }
                unsafe { self.uring.submission().push_multiple(&batch) }.expect("push multiple");
                self.outstanding += batch.len() as u64;
                self.uring.submit().expect("Submitted");
                self.stats.last_submit = std::time::Instant::now();
                self.stats.submitted_last_period += 1;
            }

            // Submit and wait for a completion when:
            // 1. We're in SQPOLL mode and we haven't submitted in over the interval, so the kernel
            //    thread may have returned.
            // 2. We haven't submitted in some period of time because we haven't hit the batch threshold
            let should_sqpoll_submit = self.args.sqpoll_interval_ms > 0
                && self.stats.last_submit.elapsed().as_millis()
                    > self.args.sqpoll_interval_ms.into();
            let pending_to_submit = !self.to_submit.is_empty();
            let has_inflight = self.outstanding > 0;
            let should_submit_and_wait = (should_sqpoll_submit || completed == 0)
                && (pending_to_submit || has_inflight || had_submission_activity);
            if should_submit_and_wait {
                let batch: Vec<io_uring::squeue::Entry> = self.to_submit.drain(..).collect();
                if !batch.is_empty() {
                    unsafe { self.uring.submission().push_multiple(&batch) }
                        .expect("push multiple");
                    self.outstanding += batch.len() as u64;
                }
                trace!(
                    "Submit and wait last_submit={} backlog={} batch_size={} t_diff={}",
                    self.stats.last_submit.elapsed().as_millis(),
                    self.to_submit.len(),
                    batch.len(),
                    self.stats.last_submit.elapsed().as_micros()
                );
                self.stats
                    .submit_and_wait_batch_size
                    .increment(batch.len() as u64)
                    .unwrap();
                self.stats.submit_and_wait += 1;
                self.stats.last_submit = std::time::Instant::now();
                self.uring.submit_and_wait(1)?;
            }
        }
        Ok(())
    }

    fn submit(&mut self, sqe: Entry) {
        self.to_submit.push(sqe);
        self.submission_count += 1;
    }
}
