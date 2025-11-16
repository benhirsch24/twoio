use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::executor;

#[derive(Clone, Default)]
struct WgInner {
    waiters: Rc<RefCell<u64>>,
    task_id: Rc<Cell<Option<u64>>>,
}

#[derive(Default)]
pub struct WaitGroup {
    inner: WgInner,
}

impl WaitGroup {
    pub fn add(&mut self) -> Guard {
        let op_id = executor::get_next_op_id();
        *self.inner.waiters.borrow_mut() += 1;
        executor::schedule_completion(op_id, false);
        Guard {
            inner: self.inner.clone(),
        }
    }

    pub fn wait(&self) -> WaitFuture {
        self.inner.task_id.set(Some(executor::get_task_id()));
        WaitFuture {
            inner: self.inner.clone(),
        }
    }
}

pub struct WaitFuture {
    inner: WgInner,
}

impl Future for WaitFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();
        if *me.inner.waiters.borrow() == 0 {
            log::trace!("WaitGroup done");
            return Poll::Ready(());
        }

        log::trace!("WaitGroup poll num_waiters={}", me.inner.waiters.borrow());
        Poll::Pending
    }
}

pub struct Guard {
    inner: WgInner,
}

impl Drop for Guard {
    fn drop(&mut self) {
        *self.inner.waiters.borrow_mut() -= 1;
        if let Some(task_id) = self.inner.task_id.get() {
            log::trace!("Guard being dropped task_id={task_id}");
            executor::wake(task_id);
        }
    }
}
