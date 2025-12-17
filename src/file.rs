use std::ffi::CString;
use std::future::Future;
use std::io::Error;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{AsyncRead, AsyncWrite};
use io_uring::{opcode, types};
use log::trace;

use crate::executor;
use crate::uring;

const CURRENT_POSITION: u64 = u64::MAX;

pub struct File {
    fd: RawFd,
    read_op_id: Option<u64>,
    write_op_id: Option<u64>,
    close_op_id: Option<u64>,
}

pub struct OpenFuture {
    op_id: Option<u64>,
    _path: CString,
    done: bool,
}

impl File {
    pub fn new(fd: RawFd) -> File {
        File {
            fd,
            read_op_id: None,
            write_op_id: None,
            close_op_id: None,
        }
    }

    pub fn as_raw_fd(&self) -> RawFd {
        self.fd
    }

    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<OpenFuture> {
        let op_id = executor::get_next_op_id();
        let path_bytes = path.as_ref().as_os_str().as_bytes();
        let c_path = CString::new(path_bytes)
            .map_err(|_| std::io::Error::other("paths containing NUL bytes are not supported"))?;
        let opcode = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), c_path.as_ptr())
            .flags((libc::O_RDWR | libc::O_CLOEXEC) as _)
            .mode(0);
        executor::schedule_completion(op_id, false);
        trace!(
            "Scheduling open completion for op={op_id} path={} task_id={}",
            path.as_ref().display(),
            executor::get_task_id()
        );
        if let Err(e) = uring::submit(opcode.build().user_data(op_id)) {
            Err(std::io::Error::other(format!("Uring problem: {e}")))
        } else {
            Ok(OpenFuture {
                op_id: Some(op_id),
                _path: c_path,
                done: false,
            })
        }
    }
}

impl Future for OpenFuture {
    type Output = std::io::Result<File>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.as_mut();
        if me.done {
            panic!("shouldn't poll again");
        }

        match me.op_id {
            Some(op_id) => match executor::get_result(op_id) {
                Some((res, _flags)) => {
                    me.op_id = None;
                    me.done = true;
                    trace!("Got open result {res} op={op_id}");
                    if res < 0 {
                        Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                    } else {
                        Poll::Ready(Ok(File::new(res)))
                    }
                }
                None => {
                    trace!("Open pending task_id={}", executor::get_task_id());
                    Poll::Pending
                }
            },
            None => Poll::Pending,
        }
    }
}

impl AsyncRead for File {
    // TODO: This implementation is not accurate as the buffer could be modified when we return
    // Poll::Pending. Really we need to implement an internal buffer. But if you are careful you
    // can use it.
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, futures::io::Error>> {
        let mut me = self.as_mut();
        let task_id = executor::get_task_id();
        let fd = me.fd;

        if let Some(op_id) = me.read_op_id {
            trace!("Read polling op_id={op_id} task_id={task_id} fd={fd}");
            return match executor::get_result(op_id) {
                Some((res, _flags)) => {
                    me.read_op_id = None;
                    trace!("Got read result {res} op_id={op_id} task_id={task_id} fd={fd}");
                    if res < 0 {
                        Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                    } else {
                        Poll::Ready(Ok(res as usize))
                    }
                }
                None => Poll::Pending,
            };
        }

        let op_id = executor::get_next_op_id();
        me.read_op_id = Some(op_id);
        let ptr = buf.as_mut_ptr();
        let len = buf.len() as u32;
        trace!(
            "Scheduling read completion for op={op_id} fd={} task_id={} len={len}",
            fd, task_id
        );
        let op = opcode::Read::new(types::Fd(fd), ptr, len).offset(CURRENT_POSITION);
        executor::schedule_completion(op_id, false);
        match uring::submit(op.build().user_data(op_id)) {
            Ok(_) => Poll::Pending,
            Err(e) => {
                me.read_op_id = None;
                Poll::Ready(Err(Error::other(format!("Uring problem: {e}"))))
            }
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, futures::io::Error>> {
        let mut me = self.as_mut();
        let task_id = executor::get_task_id();
        let fd = me.fd;

        if let Some(op_id) = me.write_op_id {
            trace!("Write polling op_id={op_id} task_id={task_id} fd={fd}");
            return match executor::get_result(op_id) {
                Some((res, _flags)) => {
                    me.write_op_id = None;
                    trace!("Got write result {res} op_id={op_id} task_id={task_id} fd={fd}");
                    if res < 0 {
                        Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                    } else {
                        Poll::Ready(Ok(res as usize))
                    }
                }
                None => Poll::Pending,
            };
        }

        let op_id = executor::get_next_op_id();
        me.write_op_id = Some(op_id);
        let ptr = buf.as_ptr();
        let len = buf.len() as u32;
        trace!(
            "Scheduling write completion for op={op_id} fd={} task_id={} len={len}",
            fd, task_id
        );
        let op = opcode::Write::new(types::Fd(fd), ptr, len).offset(CURRENT_POSITION);
        executor::schedule_completion(op_id, false);
        match uring::submit(op.build().user_data(op_id)) {
            Ok(_) => Poll::Pending,
            Err(e) => {
                me.write_op_id = None;
                Poll::Ready(Err(Error::other(format!("Uring problem: {e}"))))
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), futures::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), futures::io::Error>> {
        let mut me = self.as_mut();

        if let Some(op_id) = me.close_op_id {
            return match executor::get_result(op_id) {
                Some((res, _flags)) => {
                    if res < 0 {
                        Poll::Ready(Err(std::io::Error::from_raw_os_error(-res)))
                    } else {
                        Poll::Ready(Ok(()))
                    }
                }
                None => Poll::Pending,
            };
        }

        let op_id = executor::get_next_op_id();
        me.close_op_id = Some(op_id);
        let op = opcode::Close::new(types::Fd(me.fd));
        trace!(
            "Scheduling close completion for op={op_id} fd={} task_id={}",
            me.fd,
            executor::get_task_id()
        );
        executor::schedule_completion(op_id, false);
        uring::submit(op.build().user_data(op_id)).expect("submit close");

        Poll::Pending
    }
}
