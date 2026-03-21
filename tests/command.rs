use std::io::BufRead;

use itertools::Itertools;

macro_rules! test_filter {
    ($args: expr, $stdin: expr, $stdout: expr) => {
        test_filter!($args, $stdin, $stdout, "")
    };
    ($args: expr, $stdin: expr, $stdout: expr, $stderr: expr) => {
        let mut cmd = ::assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("nablex"));
        let assert = cmd.args($args).write_stdin($stdin).assert();
        assert.success().stdout($stdout).stderr($stderr);
    };
}

// These test cases require `sed` command.

#[test]
fn test() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["-f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt",
        include_str!("fixtures/example.txt.patch")
    );
    Ok(())
}

#[test]
fn test_filter() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["sed", "s/e/E/g"],
        include_str!("fixtures/example.txt"),
        include_str!("fixtures/example.filter.patch")
    );
    Ok(())
}

#[test]
fn test_args_separator() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["sed", "s/e/E/g", ":::", "tests/fixtures/example.txt"],
        "",
        include_str!("fixtures/example.txt.patch")
    );
    Ok(())
}

#[test]
fn test_multi() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["-f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_multi_null() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["-0f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt\0tests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_multi_args() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        [
            "sed",
            "s/e/E/g",
            ":::",
            "tests/fixtures/example.txt",
            "tests/fixtures/example2.txt"
        ],
        "",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_multi_single_thread() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["-j", "1", "-f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_multi_unordered() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = ::assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("nablex"));
    let assert = cmd
        .args(["-u", "-f", "-", "sed", "s/e/E/g"])
        .write_stdin("tests/fixtures/example.txt\ntests/fixtures/example2.txt")
        .assert()
        .success()
        .stderr("");
    let output = assert.get_output();
    let expected_sort: Vec<_> = include_str!("fixtures/example.multi.patch")
        .lines()
        .sorted()
        .collect();
    let actual_sort: Vec<_> = output
        .stdout
        .lines()
        .map_while(Result::ok)
        .sorted()
        .collect();
    assert_eq!(actual_sort, expected_sort);
    Ok(())
}

#[test]
fn test_multi_single_thread_unordered_force_parallel() -> Result<(), Box<dyn std::error::Error>> {
    // a hidden CLI option
    test_filter!(
        [
            "-j",
            "1",
            "-u",
            "--force-parallel",
            "-f",
            "-",
            "sed",
            "s/e/E/g"
        ],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_replace_str() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        [
            "-I",
            "{}",
            "sed",
            "s/e/E/g",
            "{}",
            ":::",
            "tests/fixtures/example.txt"
        ],
        "",
        include_str!("fixtures/example.txt.patch")
    );
    Ok(())
}

#[test]
fn test_replace_str_multi() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        [
            "-I",
            "{}",
            "sed",
            "s/e/E/g",
            "{}",
            ":::",
            "tests/fixtures/example.txt",
            "tests/fixtures/example2.txt"
        ],
        "",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_replace_str_files_from() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["-I", "{}", "-f", "-", "sed", "s/e/E/g", "{}"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_files_from() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["sed", "-f", "tests/fixtures/example_files.txt", "s/e/E/g"],
        "",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_files_from_stdin() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["sed", "-f", "-", "s/e/E/g"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_files_from_stdin_null() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["sed", "-0f", "-", "s/e/E/g"],
        "tests/fixtures/example.txt\0tests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
    Ok(())
}

#[test]
fn test_color_always_has_ansi_codes() -> Result<(), Box<dyn std::error::Error>> {
    test_filter!(
        ["--color", "always", "cat", "tests/fixtures/example.txt"],
        include_str!("fixtures/example.nolf.txt"),
        include_str!("fixtures/example.color.patch")
    );
    Ok(())
}

// Exit code tests

fn nablex() -> assert_cmd::Command {
    assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("nablex"))
}

#[test]
fn test_exit_0_no_check() {
    // Without --check, exit 0 even when differences exist
    nablex()
        .args(["sed", "s/e/E/g"])
        .write_stdin(include_str!("fixtures/example.txt"))
        .assert()
        .code(0);
}

#[test]
fn test_exit_0_check_no_diff() {
    // --check with no differences → exit 0
    nablex()
        .args(["--check", "cat"])
        .write_stdin("hello\n")
        .assert()
        .code(0)
        .stdout("");
}

#[test]
fn test_exit_1_check_with_diff() {
    // --check with differences → exit 1
    nablex()
        .args(["--check", "sed", "s/e/E/g"])
        .write_stdin(include_str!("fixtures/example.txt"))
        .assert()
        .code(1)
        .stdout(include_str!("fixtures/example.filter.patch"));
}

#[test]
fn test_exit_1_check_file_mode() {
    // --check in file mode
    nablex()
        .args([
            "--check",
            "sed",
            "s/e/E/g",
            ":::",
            "tests/fixtures/example.txt",
        ])
        .assert()
        .code(1)
        .stdout(include_str!("fixtures/example.txt.patch"));
}

#[test]
fn test_exit_2_command_not_found() {
    // Nonexistent command → exit 2
    nablex()
        .args(["nonexistent_cmd_12345"])
        .write_stdin("hello\n")
        .assert()
        .code(2)
        .stdout("");
}

#[test]
fn test_exit_2_file_not_found() {
    // Nonexistent file → exit 2
    nablex()
        .args(["cat", ":::", "nonexistent_file_12345.txt"])
        .assert()
        .code(2)
        .stdout("");
}

#[test]
fn test_exit_0_skip_unreadable() {
    // --skip-unreadable skips missing files instead of erroring
    nablex()
        .args(["-s", "cat", ":::", "nonexistent_file_12345.txt"])
        .assert()
        .code(0)
        .stdout("");
}

#[test]
fn test_exit_0_skip_unreadable_with_valid_file() {
    // --skip-unreadable processes valid files and skips missing ones
    nablex()
        .args([
            "-s",
            "sed",
            "s/e/E/g",
            ":::",
            "nonexistent_file_12345.txt",
            "tests/fixtures/example.txt",
        ])
        .assert()
        .code(0)
        .stdout(include_str!("fixtures/example.txt.patch"));
}

#[test]
fn test_exit_1_check_skip_unreadable_with_diff() {
    // --check + --skip-unreadable: skips bad files, still reports diff
    nablex()
        .args([
            "--check",
            "-s",
            "sed",
            "s/e/E/g",
            ":::",
            "nonexistent_file_12345.txt",
            "tests/fixtures/example.txt",
        ])
        .assert()
        .code(1)
        .stdout(include_str!("fixtures/example.txt.patch"));
}
