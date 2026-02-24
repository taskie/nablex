use std::{
    collections::VecDeque,
    io::Write,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    thread,
};

use anyhow::{Context as _, Result};
use crossbeam_channel::{Sender, bounded, select};
use log::trace;

#[derive(Debug, Clone)]
pub(crate) struct ParallelOptions {
    pub p2c_capacity_factor: usize,
    pub c2p_rxs_capacity_factor: usize,
}

impl Default for ParallelOptions {
    fn default() -> Self {
        Self {
            p2c_capacity_factor: 2,
            c2p_rxs_capacity_factor: 8,
        }
    }
}

#[derive(Debug)]
enum Request {
    Input(usize, PathBuf),
}

#[derive(Debug)]
enum Response {
    Diff(usize, PathBuf, usize, Vec<u8>),
    Error(usize, PathBuf, usize, anyhow::Error),
}

impl Response {
    fn write_to<W: Write>(self, w: &mut W) -> Result<()> {
        match self {
            Response::Diff(i, file, tid, buf) => {
                trace!("handle_resp: {:?} {:?} {:?}", i, file, tid);
                w.write_all(&buf)
                    .with_context(|| format!("{}", file.to_string_lossy()))?;
            }
            Response::Error(i, file, tid, e) => {
                trace!("handle_resp error: {:?} {:?} {:?}", i, file, tid);
                return Err(e).with_context(|| format!("{}", file.to_string_lossy()));
            }
        }
        Ok(())
    }
}

pub(crate) fn parallel_exec_multiple_files_unordered<W, I, F>(
    mut w: W,
    files: I,
    threads: NonZeroUsize,
    exec_fn: F,
    opts: ParallelOptions,
) -> Result<()>
where
    W: Write,
    I: Iterator<Item = PathBuf>,
    F: Fn(&Path) -> Result<Vec<u8>> + Send + Sync,
{
    trace!("the number of threads: {}", threads.get());
    thread::scope(|s| {
        // parent to children
        let p2c_capacity = threads.get() * opts.p2c_capacity_factor;
        let (p2c_tx, p2c_rx) = bounded::<Request>(p2c_capacity);
        // children to parent
        let c2p_capacity = threads.get() * opts.p2c_capacity_factor;
        let (c2p_tx, c2p_rx) = bounded::<Response>(c2p_capacity);
        // generate workers
        for tid in 0..threads.get() {
            let p2c_rx = p2c_rx.clone();
            let c2p_tx = c2p_tx.clone();
            let exec_fn = &exec_fn;
            s.spawn(move || -> Result<()> {
                loop {
                    let Ok(req) = p2c_rx.recv() else {
                        // disconnected
                        break;
                    };
                    match req {
                        Request::Input(i, file) => {
                            let resp = match exec_fn(&file) {
                                Ok(buf) => Response::Diff(i, file, tid, buf),
                                Err(e) => Response::Error(i, file, tid, e),
                            };
                            c2p_tx.send(resp)?;
                        }
                    }
                }
                Ok(())
            });
        }
        // processing files
        trace!("processing...");
        let mut count = 0usize;
        for (i, file) in files.enumerate() {
            let req = Request::Input(i, file.to_owned());
            trace!("send: {} {:?}", i, req);
            loop {
                // pipelining
                select! {
                    send(p2c_tx, req) -> unit => {
                        unit?;
                        trace!("sent: {}", i);
                        count += 1;
                        break;
                    }
                    recv(c2p_rx) -> resp => {
                        resp?.write_to(&mut w)?;
                        count -= 1;
                    }
                }
            }
        }
        trace!("remains: {}", count);
        while count > 0 {
            let resp = c2p_rx.recv()?;
            resp.write_to(&mut w)?;
            count -= 1;
        }
        Ok(())
    })
}

pub(crate) fn parallel_exec_multiple_files_ordered<W, I, F>(
    mut w: W,
    files: I,
    threads: NonZeroUsize,
    exec_fn: F,
    opts: ParallelOptions,
) -> Result<()>
where
    W: Write,
    I: Iterator<Item = PathBuf>,
    F: Fn(&Path) -> Result<Vec<u8>> + Send + Sync,
{
    trace!("the number of threads: {}", threads.get());
    thread::scope(|s| {
        let exec_fn = &exec_fn;
        // parent to children
        let p2c_capacity = threads.get() * opts.p2c_capacity_factor;
        let (p2c_tx, p2c_rx) = bounded::<(Request, Sender<Response>)>(p2c_capacity);
        // generate workers
        for tid in 0..threads.get() {
            let p2c_rx = p2c_rx.clone();
            s.spawn(move || -> Result<()> {
                loop {
                    let Ok((req, c2p_tx)) = p2c_rx.recv() else {
                        // disconnected
                        break;
                    };
                    match req {
                        Request::Input(i, file) => {
                            let resp = match exec_fn(&file) {
                                Ok(buf) => Response::Diff(i, file, tid, buf),
                                Err(e) => Response::Error(i, file, tid, e),
                            };
                            c2p_tx.send(resp)?;
                        }
                    }
                }
                Ok(())
            });
        }
        // processing files
        trace!("processing...");
        let mut c2p_rxs = VecDeque::new();
        let c2p_rxs_capacity = threads.get() * opts.c2p_rxs_capacity_factor;
        for (i, file) in files.enumerate() {
            let req = Request::Input(i, file.to_owned());
            // child to parent
            let (c2p_tx, c2p_rx) = bounded::<Response>(1);
            trace!("send: {} {:?}", i, req);
            c2p_rxs.push_back(c2p_rx);
            loop {
                // pipelining
                if c2p_rxs.len() > c2p_rxs_capacity {
                    let resp = c2p_rxs[0].recv()?;
                    resp.write_to(&mut w)?;
                    c2p_rxs.pop_front();
                } else {
                    // c2p_rxs is always non-empty here: push_back runs before this loop
                    select! {
                        send(p2c_tx, (req, c2p_tx)) -> unit => {
                            unit?;
                            trace!("sent: {}", i);
                            break;
                        }
                        recv(c2p_rxs[0]) -> resp => {
                            resp?.write_to(&mut w)?;
                            c2p_rxs.pop_front();
                        }
                    }
                }
            }
        }
        trace!("remains: {}", c2p_rxs.len());
        loop {
            let Some(c2p_rx) = c2p_rxs.pop_front() else {
                break;
            };
            let resp = c2p_rx.recv()?;
            resp.write_to(&mut w)?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    fn threads(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).unwrap()
    }

    fn mock_exec(file: &Path) -> Result<Vec<u8>> {
        Ok(format!("output:{}\n", file.display()).into_bytes())
    }

    fn failing_exec(file: &Path) -> Result<Vec<u8>> {
        Err(anyhow::anyhow!("exec failed for {}", file.display()))
    }

    #[test]
    fn test_unordered_basic() {
        let files = vec![PathBuf::from("a.txt"), PathBuf::from("b.txt")];
        let mut out = Vec::new();
        parallel_exec_multiple_files_unordered(
            &mut out,
            files.into_iter(),
            threads(2),
            mock_exec,
            ParallelOptions::default(),
        )
        .unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(output.contains("output:a.txt"));
        assert!(output.contains("output:b.txt"));
    }

    #[test]
    fn test_ordered_preserves_order() {
        let files: Vec<PathBuf> = (0..20)
            .map(|i| PathBuf::from(format!("{:03}.txt", i)))
            .collect();
        let mut out = Vec::new();
        parallel_exec_multiple_files_ordered(
            &mut out,
            files.into_iter(),
            threads(4),
            mock_exec,
            ParallelOptions::default(),
        )
        .unwrap();
        let output = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        // verify ordering: each "output:NNN.txt" should appear in sequence
        let output_lines: Vec<&str> = lines
            .iter()
            .copied()
            .filter(|l| l.starts_with("output:"))
            .collect();
        for (idx, line) in output_lines.iter().enumerate() {
            assert_eq!(*line, format!("output:{:03}.txt", idx));
        }
    }

    #[test]
    fn test_unordered_error() {
        let files = vec![PathBuf::from("fail.txt")];
        let mut out = Vec::new();
        let result = parallel_exec_multiple_files_unordered(
            &mut out,
            files.into_iter(),
            threads(1),
            failing_exec,
            ParallelOptions::default(),
        );
        let err = result.unwrap_err();
        assert!(err.to_string().contains("fail.txt"), "error: {}", err);
    }

    #[test]
    fn test_ordered_error() {
        let files = vec![PathBuf::from("fail.txt")];
        let mut out = Vec::new();
        let result = parallel_exec_multiple_files_ordered(
            &mut out,
            files.into_iter(),
            threads(1),
            failing_exec,
            ParallelOptions::default(),
        );
        let err = result.unwrap_err();
        assert!(err.to_string().contains("fail.txt"), "error: {}", err);
    }

    #[test]
    fn test_response_write_to_diff() {
        let resp = Response::Diff(0, PathBuf::from("x.txt"), 0, b"hello\n".to_vec());
        let mut out = Vec::new();
        resp.write_to(&mut out).unwrap();
        assert_eq!(out, b"hello\n");
    }

    #[test]
    fn test_response_write_to_error() {
        let resp = Response::Error(0, PathBuf::from("x.txt"), 0, anyhow::anyhow!("boom"));
        let mut out = Vec::new();
        let err = resp.write_to(&mut out).unwrap_err();
        assert!(err.to_string().contains("x.txt"));
        // root cause
        assert!(err.root_cause().to_string().contains("boom"));
    }

    #[test]
    fn test_unordered_backpressure() {
        // Tiny capacities force the recv arm in select! to be exercised
        let opts = ParallelOptions {
            p2c_capacity_factor: 1,
            c2p_rxs_capacity_factor: 1,
        };
        let files: Vec<PathBuf> = (0..50)
            .map(|i| PathBuf::from(format!("{}.txt", i)))
            .collect();
        let mut out = Vec::new();
        parallel_exec_multiple_files_unordered(
            &mut out,
            files.into_iter(),
            threads(2),
            |file: &Path| {
                thread::sleep(Duration::from_millis(1));
                mock_exec(file)
            },
            opts,
        )
        .unwrap();
        let output = String::from_utf8(out).unwrap();
        // all 50 files should be processed
        for i in 0..50 {
            assert!(
                output.contains(&format!("output:{}.txt", i)),
                "missing {}",
                i
            );
        }
    }

    #[test]
    fn test_ordered_overflow_drain() {
        // c2p_rxs_capacity_factor=1 with many files forces the overflow drain path
        let opts = ParallelOptions {
            p2c_capacity_factor: 1,
            c2p_rxs_capacity_factor: 1,
        };
        let files: Vec<PathBuf> = (0..30)
            .map(|i| PathBuf::from(format!("{:03}.txt", i)))
            .collect();
        let mut out = Vec::new();
        parallel_exec_multiple_files_ordered(
            &mut out,
            files.into_iter(),
            threads(2),
            |file: &Path| {
                thread::sleep(Duration::from_millis(1));
                mock_exec(file)
            },
            opts,
        )
        .unwrap();
        let output = String::from_utf8(out).unwrap();
        let output_lines: Vec<&str> = output
            .lines()
            .filter(|l| l.starts_with("output:"))
            .collect();
        // verify all 30 files in order
        assert_eq!(output_lines.len(), 30);
        for (idx, line) in output_lines.iter().enumerate() {
            assert_eq!(*line, format!("output:{:03}.txt", idx));
        }
    }
}
