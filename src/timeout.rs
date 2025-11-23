use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use io_uring::{opcode, types};
use log::trace;

use crate::executor;
use crate::uring;

pub struct TimeoutFuture {
    op_id: u64,
    timer_id: u64,
    repeated: bool,
    done: bool,
}

pub fn sleep_for(dur: Duration) -> TimeoutFuture {
    TimeoutFuture::new(dur, false)
}

pub fn ticker(dur: Duration) -> TimeoutFuture {
    TimeoutFuture::new(dur, true)
}

impl TimeoutFuture {
    fn new(dur: Duration, repeated: bool) -> Self {
        let ts = types::Timespec::new()
            .sec(dur.as_secs())
            .nsec(dur.subsec_nanos());
        let (timer_id, ts) = executor::register_timer(ts);
        let task_id = executor::get_task_id();
        let op_id = executor::get_next_op_id();
        let timeout = if repeated {
            opcode::Timeout::new(ts).flags(types::TimeoutFlags::MULTISHOT)
        } else {
            opcode::Timeout::new(ts)
        }
        .build()
        .user_data(op_id);
        trace!(
            "Scheduling timeout duration={dur:?} repeated={repeated} op={op_id} task_id={task_id}"
        );
        executor::schedule_completion(op_id, repeated);
        uring::submit(timeout).expect("arm timeout");
        Self {
            op_id,
            repeated,
            timer_id,
            done: false,
        }
    }

    pub fn get_id(&self) -> u64 {
        self.op_id
    }
}

impl Future for TimeoutFuture {
    type Output = std::io::Result<TimeoutFuture>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref();
        let task_id = executor::get_task_id();
        let op_id = me.op_id;
        if me.done {
            return Poll::Ready(Err(std::io::Error::other("Timer already expired")));
        }
        match executor::get_result(op_id) {
            Some(res) => {
                trace!("Timeout done res={res} op_id={op_id} task_id={task_id}");
                if !me.repeated {
                    executor::unregister_timer(me.timer_id);
                }
                if res == -62 {
                    Poll::Ready(Ok(TimeoutFuture {
                        op_id: me.op_id,
                        timer_id: me.timer_id,
                        repeated: me.repeated,
                        done: !me.repeated,
                    }))
                } else {
                    Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                }
            }
            None => Poll::Pending,
        }
    }
}
