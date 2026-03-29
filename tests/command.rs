use std::io::BufRead;

use itertools::Itertools;

fn nablex() -> assert_cmd::Command {
    assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("nablex"))
}

macro_rules! assert_nablex {
    ($args: expr, $stdin: expr, $stdout: expr) => {
        assert_nablex!($args, $stdin, $stdout, "")
    };
    ($args: expr, $stdin: expr, $stdout: expr, $stderr: expr) => {
        nablex()
            .args($args)
            .write_stdin($stdin)
            .assert()
            .success()
            .stdout($stdout)
            .stderr($stderr);
    };
}

// These test cases require `sed` command.

// File list mode

#[test]
fn test_file_list_from_stdin() {
    assert_nablex!(
        ["-f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt",
        include_str!("fixtures/example.txt.patch")
    );
}

#[test]
fn test_files_from() {
    assert_nablex!(
        ["sed", "-f", "tests/fixtures/example_files.txt", "s/e/E/g"],
        "",
        include_str!("fixtures/example.multi.patch")
    );
}

#[test]
fn test_files_from_stdin() {
    assert_nablex!(
        ["sed", "-f", "-", "s/e/E/g"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
}

#[test]
fn test_files_from_stdin_null() {
    assert_nablex!(
        ["sed", "-0f", "-", "s/e/E/g"],
        "tests/fixtures/example.txt\0tests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
}

// Filter mode

#[test]
fn test_filter() {
    assert_nablex!(
        ["sed", "s/e/E/g"],
        include_str!("fixtures/example.txt"),
        include_str!("fixtures/example.filter.patch")
    );
}

// Context (-U)

#[test]
fn test_context_0() {
    assert_nablex!(
        ["-U", "0", "sed", "s/e/E/g"],
        include_str!("fixtures/example.txt"),
        include_str!("fixtures/example.context0.patch")
    );
}

// File args mode (:::)

#[test]
fn test_args_separator() {
    assert_nablex!(
        ["sed", "s/e/E/g", ":::", "tests/fixtures/example.txt"],
        "",
        include_str!("fixtures/example.txt.patch")
    );
}

#[test]
fn test_multi_args() {
    assert_nablex!(
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
}

// Multiple files

#[test]
fn test_multi() {
    assert_nablex!(
        ["-f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
}

#[test]
fn test_multi_null() {
    assert_nablex!(
        ["-0f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt\0tests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
}

#[test]
fn test_multi_single_thread() {
    assert_nablex!(
        ["-j", "1", "-f", "-", "sed", "s/e/E/g"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
}

#[test]
fn test_multi_unordered() {
    let assert = nablex()
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
}

#[test]
fn test_multi_single_thread_unordered_force_parallel() {
    assert_nablex!(
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
}

// Replace string (-I)

#[test]
fn test_replace_str() {
    assert_nablex!(
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
}

#[test]
fn test_replace_str_multi() {
    assert_nablex!(
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
}

#[test]
fn test_replace_str_files_from() {
    assert_nablex!(
        ["-I", "{}", "-f", "-", "sed", "s/e/E/g", "{}"],
        "tests/fixtures/example.txt\ntests/fixtures/example2.txt",
        include_str!("fixtures/example.multi.patch")
    );
}

// Color

#[test]
fn test_color_always_has_ansi_codes() {
    assert_nablex!(
        ["--color", "always", "cat", "tests/fixtures/example.txt"],
        include_str!("fixtures/example.nolf.txt"),
        include_str!("fixtures/example.color.patch")
    );
}

// Exit codes

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
    nablex()
        .args(["nonexistent_cmd_12345"])
        .write_stdin("hello\n")
        .assert()
        .code(2)
        .stdout("");
}

#[test]
fn test_exit_2_file_not_found() {
    nablex()
        .args(["cat", ":::", "nonexistent_file_12345.txt"])
        .assert()
        .code(2)
        .stdout("");
}

// Skip unreadable

#[test]
fn test_exit_0_skip_unreadable() {
    nablex()
        .args(["-s", "cat", ":::", "nonexistent_file_12345.txt"])
        .assert()
        .code(0)
        .stdout("");
}

#[test]
fn test_exit_0_skip_unreadable_with_valid_file() {
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

// Labels (-L)

#[test]
fn test_label_both() {
    assert_nablex!(
        ["-L", "original", "-L", "modified", "sed", "s/e/E/g"],
        include_str!("fixtures/example.txt"),
        include_str!("fixtures/example.label.patch")
    );
}

#[test]
fn test_label_old_only() {
    assert_nablex!(
        ["-L", "original", "sed", "s/e/E/g"],
        include_str!("fixtures/example.txt"),
        include_str!("fixtures/example.label_old_only.patch")
    );
}

#[test]
fn test_label_file_mode() {
    assert_nablex!(
        [
            "-L",
            "original",
            "-L",
            "modified",
            "sed",
            "s/e/E/g",
            ":::",
            "tests/fixtures/example.txt"
        ],
        "",
        include_str!("fixtures/example.label.patch")
    );
}

#[test]
fn test_label_too_many() {
    // -L given 3 times → exit 2 (error)
    nablex()
        .args(["-L", "a", "-L", "b", "-L", "c", "cat"])
        .write_stdin("hello\n")
        .assert()
        .code(2);
}

// Apply mode (--apply)

fn test_tmpdir() -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("test-tmp");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_apply_writes_file() {
    use std::fs;
    let path = test_tmpdir().join("nablex_test_apply.txt");
    let original = include_str!("fixtures/example.txt");
    let expected: String = original.replace('e', "E");
    fs::write(&path, original).unwrap();
    nablex()
        .args([
            "--apply",
            "-L",
            "original",
            "-L",
            "modified",
            "sed",
            "s/e/E/g",
            ":::",
            path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(include_str!("fixtures/example.label.patch"));
    let result = fs::read_to_string(&path).unwrap();
    assert_eq!(result, expected);
}

#[test]
fn test_apply_no_diff_leaves_file_unchanged() {
    use std::fs;
    let path = test_tmpdir().join("nablex_test_apply_no_diff.txt");
    let content = "hello\n";
    fs::write(&path, content).unwrap();
    nablex()
        .args(["--apply", "cat", ":::", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout("");
    let result = fs::read_to_string(&path).unwrap();
    assert_eq!(result, content);
}
