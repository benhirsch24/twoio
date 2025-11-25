use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use futures::future::{FutureExt, LocalBoxFuture};
use log::{trace, warn};

use crate::uring;

struct ExecutorInner<'a> {
    results: HashMap<u64, i32>,
    multi_results: HashMap<u64, Vec<i32>>,
    tasks: HashMap<u64, LocalBoxFuture<'a, ()>>,
    op_to_task: HashMap<u64, (u64, bool)>,
    next_task_id: u64,
    next_op_id: u64,
    ready_queue: Vec<u64>,

    timer_id: u64,
    timers: HashMap<u64, Box<io_uring::types::Timespec>>,
    ready_queue_budget: Option<Duration>,
}

thread_local! {
    static TASK_ID: Cell<u64> = const { Cell::new(0) };
}

pub fn get_task_id() -> u64 {
    TASK_ID.get()
}

fn set_task_id(new_task_id: u64) {
    TASK_ID.set(new_task_id)
}

#[derive(Clone, Copy, Default)]
pub struct ExecutorConfig {
    pub ready_queue_budget: Option<Duration>,
}

impl ExecutorInner<'_> {
    pub fn new(config: ExecutorConfig) -> Self {
        Self {
            results: HashMap::new(),
            multi_results: HashMap::new(),
            tasks: HashMap::new(),
            op_to_task: HashMap::new(),
            next_task_id: 0,
            next_op_id: 0,
            ready_queue: Vec::new(),
            timer_id: 0,
            timers: HashMap::new(),
            ready_queue_budget: config.ready_queue_budget,
        }
    }

    fn register_timer(
        &mut self,
        ts: io_uring::types::Timespec,
    ) -> (u64, *const io_uring::types::Timespec) {
        let id = self.timer_id;
        self.timer_id += 1;
        let boxed = Box::new(ts);
        let ptr = &*boxed as *const io_uring::types::Timespec;
        self.timers.insert(id, boxed);
        (id, ptr)
    }

    pub fn unregister_timer(&mut self, timer_id: u64) {
        self.timers.remove(&timer_id);
    }

    fn handle_completion(&mut self, op: u64, res: i32, _flags: u32) -> Result<(), anyhow::Error> {
        if !self.op_to_task.contains_key(&op) {
            warn!("No op to task {op}");
            anyhow::bail!("No completion {op}");
        }

        let (task_id, is_multi) = self.op_to_task.get(&op).copied().unwrap();
        trace!("handle_completion op={op} res={res} task_id={task_id} is_multi={is_multi}");
        self.wake(task_id);
        if !is_multi {
            self.results.insert(op, res);
        } else {
            self.multi_results.entry(op).or_default().push(res);
        }
        Ok(())
    }

    fn handle_ready_queue(&mut self) -> bool {
        // Handle tasks in the ready queue until there's no more.
        // This is because tasks may depend on other tasks finishing without IO being involved.
        // Otherwise we have to wait for IO to come back from uring.
        // In a production library we'd want to have a maximum number of loops or some sort of
        // budget for how long we spend here before yielding back and seeing if any IO is done.
        let ready_queue_budget = self.ready_queue_budget;
        let ready_queue_start = Instant::now();
        let submission_count_before = crate::uring::submission_count();
        let mut n = 0;
        loop {
            trace!("Ready queue n={n}");
            n += 1;
            if self.ready_queue.is_empty() {
                trace!("No more ready queue");
                break;
            }
            let iteration_start = Instant::now();
            trace!(
                "Ready queue len {}: {:?}",
                self.ready_queue.len(),
                self.ready_queue
            );
            let ready_queue = std::mem::take(&mut self.ready_queue);
            let mut idx = 0;
            while idx < ready_queue.len() {
                let task_id = ready_queue[idx];
                set_task_id(task_id);
                trace!("Set task_id={task_id}");
                if let Some(mut task) = self.tasks.remove(&task_id) {
                    let task_start = Instant::now();
                    let mut ctx = Context::from_waker(Waker::noop());
                    match task.as_mut().poll(&mut ctx) {
                        Poll::Ready(_) => {
                            trace!(
                                "Task complete in {:?} task_id={task_id}",
                                task_start.elapsed()
                            );
                        }
                        Poll::Pending => {
                            trace!(
                                "Task still pending in {:?} task_id={task_id}",
                                task_start.elapsed()
                            );
                            self.tasks.insert(task_id, task);
                        }
                    }
                }
                idx += 1;
                if let Some(budget) = ready_queue_budget {
                    if ready_queue_start.elapsed() >= budget {
                        trace!(
                            "Ready queue budget {:?} elapsed after handling {} tasks",
                            budget, idx
                        );
                        self.ready_queue.extend_from_slice(&ready_queue[idx..]);
                        let submission_count_after = crate::uring::submission_count();
                        return submission_count_after > submission_count_before;
                    }
                }
            }
            trace!(
                "Ready queue handled in {:?}, tasks len={}",
                iteration_start.elapsed(),
                self.tasks.len()
            );
        }
        let submission_count_after = crate::uring::submission_count();
        submission_count_after > submission_count_before
    }

    pub fn run(&mut self) {
        // Run the main uring loop using our callback
        uring::run(handle_completion, handle_ready_queue).expect("running uring");
    }

    fn get_next_op_id(&mut self) -> u64 {
        let op = self.next_op_id;
        self.next_op_id += 1;
        op
    }

    fn get_next_task_id(&mut self) -> u64 {
        let task = self.next_task_id;
        self.next_task_id += 1;
        task
    }

    fn get_result(&mut self, op: u64) -> Option<i32> {
        if !self.op_to_task.contains_key(&op) {
            warn!("No task for op={op}");
            return None;
        }
        let (_, is_multi) = self.op_to_task.get(&op).unwrap();
        if *is_multi {
            if let Some(v) = self.multi_results.get_mut(&op) {
                v.pop()
            } else {
                None
            }
        } else if let Some(res) = self.results.remove(&op) {
            trace!("Removed op={op}");
            self.op_to_task.remove(&op);
            Some(res)
        } else {
            None
        }
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + 'static) {
        let cur_task = get_task_id();
        let task_id = self.get_next_task_id();
        self.tasks.insert(task_id, fut.boxed_local());
        self.ready_queue.push(task_id);
        trace!(
            "Spawning new task task_id={task_id} cur_task={cur_task} rq={}",
            self.ready_queue.len()
        );
    }

    fn wake(&mut self, task_id: u64) {
        trace!("Waking task_id={task_id}");
        self.ready_queue.push(task_id);
    }

    fn schedule_completion(&mut self, op: u64, is_multi: bool) {
        let task_id = get_task_id();
        self.op_to_task.insert(op, (task_id, is_multi));
        trace!("scheduled completion op_id={op} task_id={task_id}");
    }
}

pub struct JoinHandle<T> {
    ret: Rc<RefCell<Option<T>>>,
    task_id: Rc<Cell<Option<u64>>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();
        schedule_completion(get_next_op_id(), false);
        me.task_id.set(Some(get_task_id()));
        let mut r = me.ret.borrow_mut();
        if r.is_none() {
            return Poll::Pending;
        }
        let t = r.take().unwrap();
        Poll::Ready(t)
    }
}

thread_local! {
    static EXECUTOR: UnsafeCell<Option<ExecutorInner>> = const { UnsafeCell::new(None) };
}

pub fn init() {
    init_with_config(ExecutorConfig::default())
}

pub fn init_with_config(config: ExecutorConfig) {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        if exe.is_some() {
            return;
        }

        let new_exe = ExecutorInner::new(config);
        *exe = Some(new_exe);
    })
}

pub fn spawn<T: 'static>(fut: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        let ret = Rc::new(RefCell::new(None));
        let task_id = Rc::new(Cell::new(None));
        let jh = JoinHandle {
            ret: ret.clone(),
            task_id: task_id.clone(),
        };
        exe.as_mut().unwrap().spawn(async move {
            let t = fut.await;
            *ret.borrow_mut() = Some(t);
            if let Some(tid) = task_id.get() {
                wake(tid);
            }
        });
        jh
    })
}

pub fn register_timer(ts: io_uring::types::Timespec) -> (u64, *const io_uring::types::Timespec) {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().register_timer(ts)
    })
}

pub fn unregister_timer(timer_id: u64) {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().unregister_timer(timer_id)
    })
}

pub fn handle_completion(op: u64, res: i32, flags: u32) -> Result<(), anyhow::Error> {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().handle_completion(op, res, flags)
    })
}

pub fn handle_ready_queue() -> bool {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().handle_ready_queue()
    })
}

pub fn get_next_op_id() -> u64 {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().get_next_op_id()
    })
}

pub fn get_result(op_id: u64) -> Option<i32> {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().get_result(op_id)
    })
}

pub fn schedule_completion(op_id: u64, is_multi: bool) {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().schedule_completion(op_id, is_multi)
    })
}

pub fn wake(task_id: u64) {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().wake(task_id)
    })
}

pub fn run() {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().run()
    })
}

pub fn block_on<T: 'static>(fut: impl Future<Output = T> + 'static) -> T {
    let result: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
    let result_ref = result.clone();

    spawn(async move {
        let output = fut.await;
        *result_ref.borrow_mut() = Some(output);
        uring::exit();
    });

    run();

    result
        .borrow_mut()
        .take()
        .expect("block_on finished without producing a result")
}

#[cfg(test)]
mod tests {
    use super::{ExecutorConfig, ExecutorInner};
    use crate::executor;
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    struct ExampleFuture {
        id: u64,
    }

    impl Future for ExampleFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let me = self.as_ref();
            match executor::get_result(me.id) {
                Some(res) => {
                    println!("Got result {res}");
                    Poll::Ready(())
                }
                None => {
                    println!("no result yet");
                    Poll::Pending
                }
            }
        }
    }

    #[test]
    fn test1() {
        executor::init();
        let res = Rc::new(RefCell::new(false));
        let f = {
            let res = res.clone();
            async move {
                let example_future_op = 7;
                let fut = ExampleFuture {
                    id: example_future_op,
                };
                executor::schedule_completion(example_future_op, false);
                fut.await;
                *res.borrow_mut() = true;
            }
        };
        let id: u64 = 5;
        executor::spawn(f);
        executor::schedule_completion(id, false);
        executor::handle_ready_queue();
        executor::handle_completion(5, 0, 0).expect("No error");
        if executor::handle_completion(6, 0, 0).is_ok() {
            panic!("No scheduled completion 6");
        }
        executor::handle_ready_queue();
        executor::handle_completion(7, 0, 0).expect("No error");
        executor::handle_ready_queue();
        if !*res.borrow() {
            panic!("res should be true");
        }
    }

    struct CountOnce {
        count: Rc<Cell<u32>>,
    }

    impl Future for CountOnce {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let me = self.as_ref();
            me.count.set(me.count.get() + 1);
            Poll::Ready(())
        }
    }

    #[test]
    fn ready_queue_budget_limits_processing() {
        let mut executor_inner = ExecutorInner::new(ExecutorConfig {
            ready_queue_budget: Some(Duration::from_millis(0)),
        });

        let first = Rc::new(Cell::new(0));
        let second = Rc::new(Cell::new(0));
        executor_inner.spawn(CountOnce {
            count: first.clone(),
        });
        executor_inner.spawn(CountOnce {
            count: second.clone(),
        });

        executor_inner.handle_ready_queue();
        assert_eq!(first.get() + second.get(), 1);
        assert_eq!(executor_inner.ready_queue.len(), 1);

        executor_inner.handle_ready_queue();
        assert_eq!(first.get() + second.get(), 2);
        assert!(executor_inner.ready_queue.is_empty());
    }
}
