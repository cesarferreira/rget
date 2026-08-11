//! First-run download-folder prompt and the `config` command.
//!
//! Every test redirects `HOME` at the child process, so the platform default
//! resolves inside a scratch directory and no test can ever write into the
//! developer's real `~/Downloads`.

mod harness;

use std::process::Stdio;
use std::time::Duration;

use harness::{Config, Server, Workspace};
use tokio::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_rget");

/// A child `rget` with an isolated state database *and* an isolated home, so
/// `platform_download_dir()` lands in the workspace.
fn rget(ws: &Workspace, args: &[&str]) -> Command {
    let home = ws.path("home");
    std::fs::create_dir_all(&home).unwrap();
    let mut cmd = Command::new(BIN);
    cmd.args(args)
        .env("RGET_DB", ws.db())
        .env("HOME", &home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}

fn fake_home(ws: &Workspace) -> std::path::PathBuf {
    ws.path("home")
}

#[tokio::test]
async fn non_interactive_first_run_uses_the_platform_default_without_hanging() {
    let ws = Workspace::new("cfg-default");
    let server = Server::start(Config::with_body(64 * 1024)).await;
    let url = server.url("/thing.bin");

    // stdin is a pipe, not a terminal: the prompt must not appear and must not
    // block. A regression here would hang CI forever, so the timeout is the
    // assertion that matters most.
    let out = tokio::time::timeout(
        Duration::from_secs(30),
        rget(&ws, &["--quiet", &url]).output(),
    )
    .await
    .expect("a piped first run must never block on the prompt")
    .unwrap();

    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("Where should rget save"),
        "must not prompt when stdin is not a terminal: {stderr}"
    );

    // It landed in the platform Downloads folder, created on demand.
    let expected = fake_home(&ws).join("Downloads").join("thing.bin");
    assert!(
        expected.exists(),
        "expected the download at {}",
        expected.display()
    );
    assert_eq!(std::fs::read(&expected).unwrap(), server.body());
}

#[tokio::test]
async fn a_silent_fallback_is_not_remembered() {
    let ws = Workspace::new("cfg-nosave");
    let server = Server::start(Config::with_body(4096)).await;

    let out = rget(&ws, &["--quiet", &server.url("/a.bin")])
        .output()
        .await
        .unwrap();
    assert!(out.status.success());

    // Nothing was saved, so the first interactive run still gets to ask.
    let store = ws.store();
    assert_eq!(
        store.get_meta(rget::config::DOWNLOAD_DIR_KEY).unwrap(),
        None,
        "a non-interactive fallback must not silently become the saved setting"
    );
}

#[tokio::test]
async fn dir_flag_overrides_everything_and_skips_the_prompt() {
    let ws = Workspace::new("cfg-flag");
    let server = Server::start(Config::with_body(4096)).await;
    let target = ws.path("explicit");

    // Even with a saved setting, --dir wins.
    let out = rget(
        &ws,
        &["config", "--dir", &ws.path("saved").to_string_lossy()],
    )
    .output()
    .await
    .unwrap();
    assert!(out.status.success());

    let out = rget(
        &ws,
        &[
            "--quiet",
            &server.url("/b.bin"),
            "--dir",
            &target.to_string_lossy(),
        ],
    )
    .output()
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(target.join("b.bin").exists());
    assert!(!ws.path("saved").join("b.bin").exists());
}

#[tokio::test]
async fn saved_folder_is_used_for_later_downloads() {
    let ws = Workspace::new("cfg-saved");
    let server = Server::start(Config::with_body(4096)).await;
    let chosen = ws.path("my-downloads");

    let out = rget(&ws, &["config", "--dir", &chosen.to_string_lossy()])
        .output()
        .await
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Downloads will be saved to"), "{stdout}");

    let out = rget(&ws, &["--quiet", &server.url("/c.bin")])
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        chosen.join("c.bin").exists(),
        "should have used the saved folder"
    );
    // And not the platform default.
    assert!(!fake_home(&ws).join("Downloads").join("c.bin").exists());
}

#[tokio::test]
async fn config_shows_and_resets() {
    let ws = Workspace::new("cfg-show");

    // Before anything is saved, it reports the platform default as unsaved.
    let out = rget(&ws, &["config"]).output().await.unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(stdout.contains("download folder"), "{stdout}");
    assert!(stdout.contains("not saved yet"), "{stdout}");
    assert!(stdout.contains("Downloads"), "{stdout}");

    // Save one, and the "not saved" note goes away.
    let chosen = ws.path("picked");
    rget(&ws, &["config", "--dir", &chosen.to_string_lossy()])
        .output()
        .await
        .unwrap();
    let out = rget(&ws, &["config"]).output().await.unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("picked"), "{stdout}");
    assert!(!stdout.contains("not saved yet"), "{stdout}");

    // Reset puts us back to being asked.
    let out = rget(&ws, &["config", "--reset"]).output().await.unwrap();
    assert!(out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("will ask again"),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert_eq!(
        ws.store().get_meta(rget::config::DOWNLOAD_DIR_KEY).unwrap(),
        None
    );
}

#[tokio::test]
async fn config_json_is_machine_readable() {
    let ws = Workspace::new("cfg-json");
    let out = rget(&ws, &["config", "--json"]).output().await.unwrap();
    assert!(out.status.success());

    let value: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("config --json should emit valid JSON");
    assert_eq!(value["download_dir_is_saved"], serde_json::json!(false));
    assert!(
        value["download_dir"]
            .as_str()
            .unwrap()
            .ends_with("Downloads"),
        "{value}"
    );
}

#[tokio::test]
async fn config_rejects_a_path_that_is_not_a_directory() {
    let ws = Workspace::new("cfg-badpath");
    let file = ws.path("a-file");
    std::fs::write(&file, b"x").unwrap();

    let out = rget(&ws, &["config", "--dir", &file.to_string_lossy()])
        .output()
        .await
        .unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("not a directory"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test]
async fn json_and_quiet_downloads_never_prompt() {
    let ws = Workspace::new("cfg-machine");
    let server = Server::start(Config::with_body(4096)).await;

    for flag in ["--json", "--quiet"] {
        let out = tokio::time::timeout(
            Duration::from_secs(30),
            rget(&ws, &[flag, &server.url("/d.bin"), "--overwrite"]).output(),
        )
        .await
        .unwrap_or_else(|_| panic!("{flag} blocked on the prompt"))
        .unwrap();
        assert!(
            out.status.success(),
            "{flag}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&out.stderr).contains("Where should rget save"),
            "{flag} prompted"
        );
    }
}

#[tokio::test]
async fn resume_never_prompts_and_keeps_the_original_destination() {
    let ws = Workspace::new("cfg-resume");
    let server = Server::start(Config {
        throttle: Some((32 * 1024, Duration::from_millis(20))),
        ..Config::with_body(8 << 20)
    })
    .await;
    let url = server.url("/resumed.bin");
    let target = ws.path("first-choice");

    let mut child = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &target.to_string_lossy(),
            "-c",
            "2",
        ],
    )
    .spawn()
    .unwrap();

    // Wait for durable progress, then kill.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let done = ws
            .store()
            .list()
            .unwrap()
            .first()
            .map(|d| d.durable_bytes)
            .unwrap_or(0);
        if done > 128 * 1024 || std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    child.kill().await.unwrap();
    let _ = child.wait().await;

    let id = ws.store().list().unwrap()[0].id.clone();
    server.set(|c| c.throttle = None);

    // `resume` takes the destination from the record, so it must not consult
    // the download folder setting or ask about it.
    let out = tokio::time::timeout(
        Duration::from_secs(60),
        rget(&ws, &["--quiet", "resume", &id]).output(),
    )
    .await
    .expect("resume must not block on a prompt")
    .unwrap();

    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read(target.join("resumed.bin")).unwrap(),
        server.body()
    );
    assert!(
        !fake_home(&ws)
            .join("Downloads")
            .join("resumed.bin")
            .exists()
    );
}
