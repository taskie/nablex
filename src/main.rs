//! CLI entry point: parse arguments, select operating mode, and produce unified diffs.

use std::ffi::OsString;
use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStringExt;
use std::thread;
use std::thread::available_parallelism;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anstream::ColorChoice;
use anstyle::{AnsiColor, Color, Style};
use anyhow::{Context as _, Result};
use clap::{Parser, ValueEnum};
use itertools::process_results;
use log::trace;
use parallel::{ParallelOptions, parallel_diff_files_ordered, parallel_diff_files_unordered};
use similar::{ChangeTag, TextDiff};

mod parallel;

/// When to colorize output.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum ColorWhen {
    #[default]
    Auto,
    Always,
    Never,
}

/// Command-line arguments.
#[derive(Clone, Debug, Parser)]
#[clap(
    author,
    version,
    about,
    long_about = None,
    after_long_help = "\
Modes:
  Filter (default)  Pipe stdin through CMD, diff stdin vs stdout
  File              Open each file, run CMD with file path appended, diff result
  File list (-f)    Like File mode, read file paths from FILE

File mode example:
  nablex sed 's/foo/bar/g' ::: file.txt *.md

File list mode example:
  find . -name '*.txt' | nablex -f - sed 's/foo/bar/g'"
)]
struct Args {
    /// Color output
    #[clap(long, default_value = "auto", value_name = "WHEN")]
    color: ColorWhen,
    /// Resolved color flag (not a CLI argument)
    #[clap(skip)]
    use_color: bool,
    /// Use NUL as the path delimiter instead of newline (for use with -f or find -print0)
    #[clap(short = '0', long)]
    null: bool,
    /// Number of parallel jobs (0 = auto-detect)
    #[clap(short = 'j', long, default_value_t = 0)]
    jobs: u32,
    /// Allow unordered output for faster parallel execution
    #[clap(short = 'u', long)]
    unordered: bool,
    /// Read file paths from FILE ('-' for stdin)
    #[clap(short, long, value_name = "FILE")]
    files_from: Option<PathBuf>,
    /// Command to execute
    #[clap(name = "CMD")]
    cmd_name: String,
    /// Arguments for CMD; use ':::' to separate CMD args from file paths
    #[clap(name = "ARG", trailing_var_arg = true, allow_hyphen_values = true)]
    cmd_args: Vec<String>,
    /// Replace occurrences of REPLACE_STR in arguments with the file path
    #[clap(short = 'I', long)]
    replace_str: Option<String>,
    /// Skip unreadable files with a warning instead of aborting
    #[clap(short = 's', long)]
    skip_unreadable: bool,
    /// Exit with status 1 if any differences are found
    #[clap(short = 'c', long)]
    check: bool,
    /// Override diff header labels (can be given up to 2 times: old and new)
    #[clap(short = 'L', long = "label", num_args = 1)]
    labels: Vec<String>,
    /// Force parallel execution for debugging
    #[doc(hidden)]
    #[clap(long, hide = true)]
    force_parallel: bool,
}

fn main() {
    env_logger::init();
    match run() {
        Ok(diff_checked) => {
            if diff_checked {
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("nablex: {e:#}");
            std::process::exit(2);
        }
    }
}

/// Parse arguments, set up output, and dispatch to the appropriate mode.
fn run() -> Result<bool> {
    let mut args = Args::parse();
    if args.labels.len() > 2 {
        anyhow::bail!("--label can be specified at most 2 times");
    }
    let color_choice = match args.color {
        ColorWhen::Always => ColorChoice::Always,
        ColorWhen::Never => ColorChoice::Never,
        ColorWhen::Auto => ColorChoice::Auto,
    };
    color_choice.write_global();
    args.use_color = match color_choice {
        ColorChoice::Always | ColorChoice::AlwaysAnsi => true,
        ColorChoice::Never => false,
        ColorChoice::Auto => io::stdout().is_terminal(),
    };
    let stdout = anstream::stdout();
    let stdout_lock = stdout.lock();
    let bufw = BufWriter::new(stdout_lock);
    let has_diff = if let Some(files_from) = args.files_from.as_ref() {
        if Path::new("-") == files_from {
            run_file_list_stdin(&args, bufw)?
        } else {
            run_file_list_file(&args, bufw, files_from)?
        }
    } else if args.cmd_args.iter().any(|s| s == ":::") {
        run_file_args(&args, bufw)?
    } else {
        run_filter(&args, bufw)?
    };
    Ok(args.check && has_diff)
}

/// Filter mode entry point: read stdin and pipe through the command.
fn run_filter<W: Write>(args: &Args, bufw: W) -> Result<bool> {
    let stdin = io::stdin();
    let stdin_lock = stdin.lock();
    let bufr = BufReader::new(stdin_lock);
    diff_filter(args, bufr, bufw)
}

/// File list mode entry point: read file paths from stdin (`-f -`).
fn run_file_list_stdin<W: Write>(args: &Args, bufw: W) -> Result<bool> {
    let stdin = io::stdin();
    let stdin_lock = stdin.lock();
    let bufr = BufReader::new(stdin_lock);
    run_file_list(args, bufw, bufr)
}

/// File list mode entry point: read file paths from a file (`-f FILE`).
fn run_file_list_file<W: Write>(args: &Args, bufw: W, path: &Path) -> Result<bool> {
    let file = File::open(path).with_context(|| format!("failed to open: {}", path.display()))?;
    let bufr = BufReader::new(file);
    run_file_list(args, bufw, bufr).with_context(|| format!("failed to read: {}", path.display()))
}

/// Parse file paths from a reader and process them.
fn run_file_list<W: Write, R: BufRead>(args: &Args, mut bufw: W, bufr: R) -> Result<bool> {
    if args.null {
        Ok(process_results(bufr.split(0), |lines| {
            diff_files(
                args,
                &mut bufw,
                &args.cmd_args,
                lines.map(|line| OsString::from_vec(line).into()),
            )
        })??)
    } else {
        Ok(process_results(bufr.lines(), |lines| {
            diff_files(
                args,
                &mut bufw,
                &args.cmd_args,
                lines.map(|line| line.into()),
            )
        })??)
    }
}

/// File args mode entry point: extract file paths after `:::` separator.
fn run_file_args<W: Write>(args: &Args, mut bufw: W) -> Result<bool> {
    let cmd_args = args.cmd_args.as_slice();
    let last_components = cmd_args.split(|s| s == ":::").next_back();
    if let Some(filestrs) = last_components {
        // Strip the ":::" separator from cmd_opts
        let cmd_opts = &cmd_args[..cmd_args.len() - filestrs.len() - 1];
        diff_files(
            args,
            &mut bufw,
            cmd_opts,
            filestrs.iter().map(|line| line.into()),
        )
    } else {
        unreachable!("split().next_back() always returns Some");
    }
}

/// Dispatch multiple files to serial or parallel execution.
fn diff_files<W: Write, I: Iterator<Item = PathBuf>>(
    args: &Args,
    w: W,
    cmd_args: &[String],
    files: I,
) -> Result<bool> {
    let threads = if args.jobs > 0 {
        NonZeroUsize::new(args.jobs as usize).unwrap()
    } else {
        available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap())
    };
    if threads <= NonZeroUsize::new(1).unwrap() && !args.force_parallel {
        diff_files_serial(args, w, cmd_args, files)
    } else {
        let exec_fn = |file: &Path| -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            diff_file(args, &mut buf, cmd_args, file)?;
            Ok(buf)
        };
        if args.unordered {
            parallel_diff_files_unordered(w, files, threads, exec_fn, ParallelOptions::default())
        } else {
            parallel_diff_files_ordered(w, files, threads, exec_fn, ParallelOptions::default())
        }
    }
}

/// Process files sequentially on the current thread.
fn diff_files_serial<W: Write, I: Iterator<Item = PathBuf>>(
    args: &Args,
    mut w: W,
    cmd_args: &[String],
    files: I,
) -> Result<bool> {
    let mut count = 0usize;
    let mut has_diff = false;
    for file in files {
        has_diff |= diff_file(args, &mut w, cmd_args, &file)?;
        count += 1;
    }
    trace!("processed: {}", count);
    Ok(has_diff)
}

/// Read one file, run the command, and write the unified diff.
fn diff_file<W: Write>(args: &Args, w: W, cmd_args: &[String], file: &Path) -> Result<bool> {
    let mut command = Command::new(&args.cmd_name);
    let inf = match File::open(file) {
        Ok(f) => f,
        Err(e) => {
            if args.skip_unreadable {
                eprintln!("{}: {}", file.display(), e);
                return Ok(false);
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
                return Ok(false);
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
        let aname = args.labels.first().map(|s| s.as_str()).unwrap_or(&name);
        let bname = args.labels.get(1).map(|s| s.as_str()).unwrap_or(&name);
        return diff(args, w, aname, &inb, bname, &output.stdout);
    } else {
        eprintln!("{}: command exited with {}", file.display(), output.status);
    }
    Ok(false)
}

/// Pipe input through the command and write the unified diff of stdin vs stdout.
fn diff_filter<R: BufRead, W: Write>(args: &Args, mut r: R, w: W) -> Result<bool> {
    let mut command = Command::new(&args.cmd_name);
    let mut child = command
        .args(&args.cmd_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to execute: {}", args.cmd_name))?;
    let mut child_stdin = child.stdin.take().unwrap();
    let mut child_stdout = child.stdout.take().unwrap();
    let mut inb = Vec::<u8>::new();
    // Read child's stdout in a scoped thread to prevent deadlock while streaming stdin
    let (child_out, stream_result) = thread::scope(|s| {
        let stdout_handle = s.spawn(|| -> io::Result<Vec<u8>> {
            let mut buf = Vec::new();
            child_stdout.read_to_end(&mut buf)?;
            Ok(buf)
        });
        let stream_result = (|| -> io::Result<()> {
            let mut chunk = [0u8; 8192];
            loop {
                let n = r.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                inb.extend_from_slice(&chunk[..n]);
                child_stdin.write_all(&chunk[..n])?;
            }
            Ok(())
        })();
        drop(child_stdin); // signal EOF to child regardless of stream_result
        let child_out = stdout_handle.join().expect("stdout reader thread panicked");
        (child_out, stream_result)
    });
    let child_out = child_out?;
    let status = child.wait()?;
    stream_result?;
    if status.success() {
        let aname = args.labels.first().map(|s| s.as_str()).unwrap_or("<stdin>");
        let bname = args.labels.get(1).map(|s| s.as_str()).unwrap_or("<stdout>");
        return diff(args, w, aname, &inb, bname, &child_out);
    } else {
        eprintln!("command exited with {}", status);
    }
    Ok(false)
}

/// Compute and write a unified diff, with optional color output.
fn diff<W: Write>(
    args: &Args,
    mut w: W,
    aname: &str,
    a: &[u8],
    bname: &str,
    b: &[u8],
) -> Result<bool> {
    let diff = TextDiff::from_lines(a, b);
    if !args.use_color {
        let mut udiff = diff.unified_diff();
        let udiff = udiff.header(aname, bname);
        let has_diff = udiff.iter_hunks().next().is_some();
        udiff.to_writer(w)?;
        return Ok(has_diff);
    }
    let mut udiff = diff.unified_diff();
    let udiff = udiff.header(aname, bname);
    let mut has_diff = false;
    let bold = Style::new().bold();
    let hunk_style = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Cyan)));
    let del = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Red)));
    let ins = Style::new().fg_color(Some(Color::Ansi(AnsiColor::Green)));
    let reset = anstyle::Reset;
    for hunk in udiff.iter_hunks() {
        if !has_diff {
            writeln!(w, "{bold}--- {aname}{reset}")?;
            writeln!(w, "{bold}+++ {bname}{reset}")?;
            has_diff = true;
        }
        writeln!(w, "{hunk_style}{}{reset}", hunk.header())?;
        for change in hunk.iter_changes() {
            match change.tag() {
                ChangeTag::Delete => {
                    write!(w, "{del}-{change}{reset}")?;
                }
                ChangeTag::Insert => {
                    write!(w, "{ins}+{change}{reset}")?;
                }
                ChangeTag::Equal => {
                    write!(w, " {change}")?;
                }
            }
            if change.missing_newline() {
                writeln!(w, "\\ No newline at end of file")?;
            }
        }
    }
    Ok(has_diff)
}
