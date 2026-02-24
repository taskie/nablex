use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStringExt;
use std::thread::available_parallelism;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context as _, Result};
use clap::Parser;
use itertools::process_results;
use log::{trace, warn};
use parallel::{
    ParallelOptions, parallel_exec_multiple_files_ordered, parallel_exec_multiple_files_unordered,
};
use similar::TextDiff;

mod parallel;

#[derive(Clone, Debug, Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Read NUL-delimited input
    #[clap(short = '0', long)]
    null: bool,
    /// Approximate number of parallel jobs
    #[clap(short = 'j', long, default_value_t = 0)]
    jobs: u32,
    /// Allow unordered output for faster parallel execution
    #[clap(short = 'u', long)]
    unordered: bool,
    /// Read file paths from a file
    #[clap(short, long)]
    files_from: Option<PathBuf>,
    /// Command to execute
    #[clap(name = "CMD")]
    cmd_name: String,
    /// Command arguments
    #[clap(name = "ARG", trailing_var_arg = true, allow_hyphen_values = true)]
    cmd_args: Vec<String>,
    /// Replace occurrences of REPLACE_STR in arguments with the file path
    #[clap(short = 'I', long)]
    replace_str: Option<String>,
    /// Skip unreadable files with a warning instead of aborting
    #[clap(short = 's', long)]
    skip_unreadable: bool,
    /// Force parallel execution for debugging
    #[doc(hidden)]
    #[clap(long, hide = true)]
    force_parallel: bool,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let stdout = io::stdout();
    let stdout_lock = stdout.lock();
    let bufw = BufWriter::new(stdout_lock);
    if let Some(files_from) = args.files_from.as_ref() {
        if Path::new("-") == files_from {
            run_with_files_from_stdin(&args, bufw)?;
        } else {
            run_with_files_from_file(&args, bufw, files_from)?;
        }
    } else if args.cmd_args.iter().any(|s| s == "--") {
        run_with_files_from_multi_args(&args, bufw)?;
    } else {
        run_with_stdin(&args, bufw)?;
    }
    Ok(())
}

fn run_with_stdin<W: Write>(args: &Args, bufw: W) -> Result<()> {
    let stdin = io::stdin();
    let stdin_lock = stdin.lock();
    let bufr = BufReader::new(stdin_lock);
    exec_with_buf_read(args, bufr, bufw)?;
    Ok(())
}

fn run_with_files_from_stdin<W: Write>(args: &Args, bufw: W) -> Result<()> {
    let stdin = io::stdin();
    let stdin_lock = stdin.lock();
    let bufr = BufReader::new(stdin_lock);
    run_with_files_from_buf_reader(args, bufw, bufr)?;
    Ok(())
}

fn run_with_files_from_file<W: Write>(args: &Args, bufw: W, path: &Path) -> Result<()> {
    let file = File::open(path).with_context(|| format!("failed to open: {}", path.display()))?;
    let bufr = BufReader::new(file);
    run_with_files_from_buf_reader(args, bufw, bufr)
        .with_context(|| format!("failed to read: {}", path.display()))?;
    Ok(())
}

fn run_with_files_from_buf_reader<W: Write, R: BufRead>(
    args: &Args,
    mut bufw: W,
    bufr: R,
) -> Result<()> {
    if args.null {
        process_results(bufr.split(0), |lines| {
            exec_multiple_files(
                args,
                &mut bufw,
                &args.cmd_args,
                lines.map(|line| OsString::from_vec(line).into()),
            )
        })??;
    } else {
        process_results(bufr.lines(), |lines| {
            exec_multiple_files(
                args,
                &mut bufw,
                &args.cmd_args,
                lines.map(|line| line.into()),
            )
        })??;
    }
    Ok(())
}

fn run_with_files_from_multi_args<W: Write>(args: &Args, mut bufw: W) -> Result<()> {
    let cmd_args = args.cmd_args.as_slice();
    let last_components = cmd_args.split(|s| s == "--").next_back();
    if let Some(filestrs) = last_components {
        // Strip the "--" separator from cmd_opts
        let cmd_opts = &cmd_args[..cmd_args.len() - filestrs.len() - 1];
        exec_multiple_files(
            args,
            &mut bufw,
            cmd_opts,
            filestrs.iter().map(|line| line.into()),
        )?;
    } else {
        unreachable!("split().next_back() always returns Some");
    }
    Ok(())
}

fn exec_multiple_files<W: Write, I: Iterator<Item = PathBuf>>(
    args: &Args,
    w: W,
    cmd_args: &[String],
    files: I,
) -> Result<()> {
    let threads = if args.jobs > 0 {
        NonZeroUsize::new(args.jobs as usize).unwrap()
    } else {
        available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap())
    };
    if threads <= NonZeroUsize::new(1).unwrap() && !args.force_parallel {
        serial_exec_multiple_files(args, w, cmd_args, files)
    } else {
        let exec_fn = |file: &Path| -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            exec_one_file(args, &mut buf, cmd_args, file)?;
            Ok(buf)
        };
        if args.unordered {
            parallel_exec_multiple_files_unordered(
                w,
                files,
                threads,
                exec_fn,
                ParallelOptions::default(),
            )
        } else {
            parallel_exec_multiple_files_ordered(
                w,
                files,
                threads,
                exec_fn,
                ParallelOptions::default(),
            )
        }
    }
}

fn serial_exec_multiple_files<W: Write, I: Iterator<Item = PathBuf>>(
    args: &Args,
    mut w: W,
    cmd_args: &[String],
    files: I,
) -> Result<()> {
    let mut count = 0usize;
    for file in files {
        exec_one_file(args, &mut w, cmd_args, &file)?;
        count += 1;
    }
    trace!("processed: {}", count);
    Ok(())
}

fn exec_one_file<W: Write>(args: &Args, w: W, cmd_args: &[String], file: &Path) -> Result<()> {
    let mut command = Command::new(&args.cmd_name);
    let inf = match File::open(file) {
        Ok(f) => f,
        Err(e) => {
            if args.skip_unreadable {
                eprintln!("{}: {}", file.display(), e);
                return Ok(());
            }
            return Err(e).with_context(|| format!("failed to open: {}", file.display()));
        }
    };
    let mut inbr = BufReader::new(inf);
    let mut inb = Vec::<u8>::new();
    match inbr.read_to_end(&mut inb) {
        Ok(_) => {}
        Err(e) => {
            if args.skip_unreadable {
                eprintln!("{}: {}", file.display(), e);
                return Ok(());
            }
            return Err(e).with_context(|| format!("failed to read: {}", file.display()));
        }
    }
    let child = if let Some(ref placeholder) = args.replace_str {
        let file_str = file.to_string_lossy();
        let replaced: Vec<String> = cmd_args
            .iter()
            .map(|a| a.replace(placeholder, &file_str))
            .collect();
        command.args(&replaced)
    } else {
        command.args(cmd_args).arg(file)
    }
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .spawn()
    .with_context(|| format!("{}: failed to execute: {}", file.display(), args.cmd_name))?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        let name = file.to_string_lossy();
        diff(args, w, &name, &inb, &name, &output.stdout)?;
    } else {
        warn!("{}: command exited with {}", file.display(), output.status);
    }
    Ok(())
}

fn exec_with_buf_read<R: BufRead, W: Write>(args: &Args, mut r: R, w: W) -> Result<()> {
    let mut command = Command::new(&args.cmd_name);
    let mut inb = Vec::<u8>::new();
    r.read_to_end(&mut inb)?;
    let mut child = command
        .args(&args.cmd_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to execute: {}", args.cmd_name))?;
    // Take and drop stdin to signal EOF after writing
    child.stdin.take().unwrap().write_all(&inb)?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        diff(args, w, "<stdin>", &inb, "<stdout>", &output.stdout)?;
    } else {
        warn!("command exited with {}", output.status);
    }
    Ok(())
}

fn diff<W: Write>(_args: &Args, w: W, aname: &str, a: &[u8], bname: &str, b: &[u8]) -> Result<()> {
    let diff = TextDiff::from_lines(a, b);
    let mut udiff = diff.unified_diff();
    let udiff = udiff.header(aname, bname);
    udiff.to_writer(w)?;
    Ok(())
}
