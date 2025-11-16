use std::cell::RefCell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::executor;

pub enum SendError {
    Closed,
    Full, // We're unbounded right now but maybe one day
}

pub struct Receiver<T: Clone> {
    inner: Inner<T>,
}

impl<T: Clone> Receiver<T> {
    pub fn recv(&mut self) -> RecvFuture<T> {
        *self.inner.receiver_task_id.borrow_mut() = Some(executor::get_task_id());
        RecvFuture {
            inner: self.inner.clone(),
        }
    }

    pub fn close(&mut self) {
        *self.inner.closed.borrow_mut() = true;
    }
}

pub struct RecvFuture<T: Clone> {
    inner: Inner<T>,
}

impl<T: Clone> Future for RecvFuture<T> {
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();

        if *self.inner.closed.borrow() && self.inner.data.borrow().is_empty() {
            return Poll::Ready(None);
        }

        if me.inner.data.borrow().is_empty() {
            return Poll::Pending;
        }

        Poll::Ready(Some(me.inner.data.borrow_mut().pop_front().unwrap()))
    }
}

#[derive(Clone)]
pub struct Sender<T: Clone> {
    inner: Inner<T>,
}

impl<T: Clone> Sender<T> {
    pub fn send(&mut self, t: T) -> Result<(), SendError> {
        if *self.inner.closed.borrow() {
            return Err(SendError::Closed);
        }

        self.inner.data.borrow_mut().push_back(t);
        if let Some(task_id) = *self.inner.receiver_task_id.borrow() {
            executor::wake(task_id);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Inner<T>
where
    T: Clone,
{
    data: Rc<RefCell<VecDeque<T>>>,
    receiver_task_id: Rc<RefCell<Option<u64>>>,
    closed: Rc<RefCell<bool>>,
}

pub fn channel<T: Clone>() -> (Receiver<T>, Sender<T>) {
    let inner = Inner {
        data: Rc::new(RefCell::new(VecDeque::new())),
        receiver_task_id: Rc::new(RefCell::new(None)),
        closed: Rc::new(RefCell::new(false)),
    };
    (
        Receiver {
            inner: inner.clone(),
        },
        Sender {
            inner: inner.clone(),
        },
    )
}
