//! A compactor thread that merges the batches for the spine-fueled trace.

use crate::Runtime;
use std::sync::{Arc, Mutex};
use std::thread::{Builder, Thread};

/// Return value for a worker function.
pub enum WorkerStatus {
    /// The worker has more work to do (it only returned to allow other workers
    /// to run).
    Busy,

    /// The worker has no more work to do now, but it might have more later.
    Idle,

    /// The worker has exited.
    Done,
}

struct Inner {
    new_workers: Vec<WorkerConstructorFn>,
    exiting: bool,
    thread: Option<Thread>,
}
pub struct BackgroundThread(Mutex<Inner>);

/// A function that returns a [WorkerFn].
///
/// This exists because the [WorkerFn] that we use constructs a merger, which
/// are not required to be `Send` and in practice are not (because our storage
/// implementations are thread-specific).  This means that the caller of
/// [BackgroundThread::add_worker] can't construct a merger for the worker,
/// because it would then be moved from the caller's thread to the background
/// thread. Thus, instead, the `WorkerConstructorFn` is called once in the
/// background thread to do the construction.
type WorkerConstructorFn = Box<dyn FnOnce() -> WorkerFn + Send>;

/// The worker function, which is called repeatedly until it reports that it is
/// done.
type WorkerFn = Box<dyn FnMut() -> WorkerStatus>;

impl BackgroundThread {
    pub fn new(worker: WorkerConstructorFn) -> Arc<Self> {
        let bg = Arc::new(Self(Mutex::new(Inner {
            new_workers: vec![worker],
            exiting: false,
            thread: None,
        })));
        let name = if let Some(name) = std::thread::current().name() {
            format!("{name}-bg")
        } else {
            String::from("dbsp-bg")
        };
        let thread = Runtime::spawn_background_thread(Builder::new().name(name), {
            let bg = bg.clone();
            move || bg.run()
        });
        bg.0.lock().unwrap().thread = Some(thread);
        bg
    }

    pub fn wake(&self) {
        let inner = self.0.lock().unwrap();
        if let Some(thread) = inner.thread.as_ref() {
            thread.unpark();
        }
    }

    fn run(self: Arc<Self>) {
        let mut workers = Vec::new();
        loop {
            // Gather newly submitted workers.
            let mut inner = self.0.lock().unwrap();
            for new_worker in inner.new_workers.drain(..) {
                workers.push(new_worker());
            }
            if workers.is_empty() {
                inner.exiting = true;
                return;
            }
            drop(inner);

            // Run through workers.
            let mut idle = true;
            workers.retain_mut(|worker| match worker() {
                WorkerStatus::Busy => {
                    idle = false;
                    true
                }
                WorkerStatus::Idle => true,
                WorkerStatus::Done => false,
            });

            // If there's at least one worker and all of them are idle, wait for
            // something to change.
            //
            // If there are no workers, go around again to get a new worker or
            // exit (if we just exit immediately then that could drop a new
            // worker).
            if idle && !workers.is_empty() {
                std::thread::park();
            }
        }
    }
}
