use std::cell::{Cell, UnsafeCell};
use std::collections::HashMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

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

impl ExecutorInner<'_> {
    pub fn new() -> Self {
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

    fn handle_ready_queue(&mut self) {
        // Handle tasks in the ready queue until there's no more.
        // This is because tasks may depend on other tasks finishing without IO being involved.
        // Otherwise we have to wait for IO to come back from uring.
        // In a production library we'd want to have a maximum number of loops or some sort of
        // budget for how long we spend here before yielding back and seeing if any IO is done.
        let mut n = 0;
        loop {
            trace!("Ready queue n={n}");
            n += 1;
            if self.ready_queue.is_empty() {
                trace!("No more ready queue");
                return;
            }
            let start = std::time::Instant::now();
            trace!(
                "Ready queue len {}: {:?}",
                self.ready_queue.len(),
                self.ready_queue
            );
            let ready_queue = std::mem::take(&mut self.ready_queue);
            for task_id in ready_queue.iter() {
                set_task_id(*task_id);
                trace!("Set task_id={task_id}");
                if let Some(mut task) = self.tasks.remove(task_id) {
                    let start = std::time::Instant::now();
                    let mut ctx = Context::from_waker(Waker::noop());
                    match task.as_mut().poll(&mut ctx) {
                        Poll::Ready(_) => {
                            trace!("Task complete in {:?} task_id={task_id}", start.elapsed());
                        }
                        Poll::Pending => {
                            trace!(
                                "Task still pending in {:?} task_id={task_id}",
                                start.elapsed()
                            );
                            self.tasks.insert(*task_id, task);
                        }
                    }
                }
            }
            trace!(
                "Ready queue handled in {:?}, tasks len={}",
                start.elapsed(),
                self.tasks.len()
            );
        }
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

thread_local! {
    static EXECUTOR: UnsafeCell<Option<ExecutorInner>> = const { UnsafeCell::new(None) };
}

pub fn init() {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        if exe.is_some() {
            return;
        }

        let new_exe = ExecutorInner::new();
        *exe = Some(new_exe);
    })
}

pub fn spawn(fut: impl Future<Output = ()> + 'static) {
    EXECUTOR.with(|exe| unsafe {
        let exe = &mut *exe.get();
        exe.as_mut().unwrap().spawn(fut);
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

pub fn handle_ready_queue() {
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

#[cfg(test)]
mod tests {
    use crate::executor;
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};

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
}
