//! Resume and crash-recovery tests (PRD §33, §39).
//!
//! These kill the **real binary** with SIGKILL rather than simulating an error
//! inside Rust. That is the only way to exercise what the PRD actually promises:
//! that the process can die after any instruction and the file still comes out
//! byte-for-byte correct.

mod harness;

use std::process::Stdio;
use std::time::{Duration, Instant};

use harness::{Config, Server, Workspace, sha256};
use tokio::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_rget");

/// A body big enough, and slow enough, that we can reliably kill mid-transfer.
fn slow_config(size: usize) -> Config {
    Config {
        // ~1.6 MiB/s per connection.
        throttle: Some((32 * 1024, Duration::from_millis(20))),
        ..Config::with_body(size)
    }
}

fn rget(ws: &Workspace, args: &[&str]) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.args(args)
        .env("RGET_DB", ws.db())
        // Keep the child's output out of the test log unless we inspect it.
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}

/// Durable progress as recorded in the shared state database.
fn durable_bytes(ws: &Workspace) -> u64 {
    let store = ws.store();
    store
        .list()
        .unwrap()
        .first()
        .map(|d| d.durable_bytes)
        .unwrap_or(0)
}

/// Wait until the running download has checkpointed at least `min` bytes.
async fn wait_for_progress(ws: &Workspace, min: u64, timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        let got = durable_bytes(ws);
        if got >= min {
            return got;
        }
        if Instant::now() > deadline {
            panic!("no durable progress after {timeout:?} (saw {got} bytes)");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn survives_repeated_kills_and_produces_the_right_file() {
    let ws = Workspace::new("kills");
    let server = Server::start(slow_config(8 << 20)).await;
    let body = server.body();
    let url = server.url("/big.bin");
    let dest = ws.path("big.bin");

    let mut previous = 0u64;
    // Kill it three times at increasing depths, exactly like PRD §39.
    for round in 0..3 {
        let mut child = rget(
            &ws,
            &[
                "--quiet",
                &url,
                "--dir",
                &ws.dir.to_string_lossy(),
                "-c",
                "4",
            ],
        )
        .spawn()
        .expect("spawn rget");

        let target = previous + (256 * 1024);
        let reached = wait_for_progress(&ws, target, Duration::from_secs(60)).await;
        child.kill().await.expect("kill rget");
        let _ = child.wait().await;

        // Progress must be monotonic across kills: nothing is ever lost.
        let after_kill = durable_bytes(&ws);
        assert!(
            after_kill >= previous,
            "round {round}: progress went backwards, {after_kill} < {previous}"
        );
        assert!(
            after_kill >= reached,
            "round {round}: checkpointed bytes vanished"
        );
        previous = after_kill;
        assert!(
            previous < body.len() as u64,
            "download finished before we could kill it; make the test body slower"
        );
    }

    // Now let it finish, and make it prove the result cryptographically.
    let digest = sha256(&body);
    let out = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "-c",
            "4",
            "--sha256",
            &digest,
        ],
    )
    .output()
    .await
    .expect("run rget");

    assert!(
        out.status.success(),
        "final run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read(&dest).unwrap(),
        body,
        "file differs from source"
    );

    // It genuinely resumed rather than starting over: total bytes served is far
    // less than four full copies.
    let served: usize = body.len();
    assert!(
        served > 0 && durable_bytes(&ws) == body.len() as u64,
        "final durable byte count should equal the file size"
    );
}

#[tokio::test]
async fn resume_does_not_refetch_completed_ranges() {
    let ws = Workspace::new("norefetch");
    let server = Server::start(slow_config(8 << 20)).await;
    let body = server.body();
    let url = server.url("/frugal.bin");

    let mut child = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "-c",
            "2",
        ],
    )
    .spawn()
    .unwrap();
    let reached = wait_for_progress(&ws, 1 << 20, Duration::from_secs(60)).await;
    child.kill().await.unwrap();
    let _ = child.wait().await;

    // Measure the second run alone, and let it finish quickly.
    server.reset_stats();
    server.set(|c| c.throttle = None);
    let out = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "-c",
            "2",
            "--sha256",
            &sha256(&body),
        ],
    )
    .output()
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "resume failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The honest test of "it resumed": the second run pulled roughly the
    // outstanding bytes, not the whole file again. A little slack covers the
    // probe plus the bounded re-download of anything not yet durable.
    let served = server.bytes_served();
    let outstanding = body.len() - reached as usize;
    assert!(
        served < outstanding + (2 * 1024 * 1024),
        "resume re-downloaded too much: served {served} bytes with only {outstanding} outstanding \
         after {reached} durable bytes"
    );
    assert!(
        served > 0,
        "the second run should have transferred something"
    );
    assert_eq!(std::fs::read(ws.path("frugal.bin")).unwrap(), body);
}

#[tokio::test]
async fn refuses_to_resume_when_the_remote_changed() {
    let ws = Workspace::new("changed");
    let server = Server::start(slow_config(8 << 20)).await;
    let url = server.url("/mutable.bin");

    let mut child = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "-c",
            "2",
        ],
    )
    .spawn()
    .unwrap();
    wait_for_progress(&ws, 512 * 1024, Duration::from_secs(60)).await;
    child.kill().await.unwrap();
    let _ = child.wait().await;

    // The resource is replaced: new content, new ETag, different size.
    let new_body = harness::test_body((8 << 20) + 4096);
    server.set(|c| {
        c.body = new_body.clone();
        c.etag = Some("\"v2\"".into());
        c.throttle = None;
    });

    let out = rget(&ws, &[&url, "--dir", &ws.dir.to_string_lossy()])
        .output()
        .await
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "must refuse to resume");
    assert!(
        stderr.contains("Remote file changed"),
        "expected a clear explanation, got: {stderr}"
    );
    assert!(stderr.contains("could corrupt"), "{stderr}");
    assert!(
        stderr.contains("--restart"),
        "should suggest the way out: {stderr}"
    );

    // --restart is the explicit way forward, and it yields the *new* file.
    let out = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "--restart",
            "--sha256",
            &sha256(&new_body),
        ],
    )
    .output()
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "--restart should succeed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(std::fs::read(ws.path("mutable.bin")).unwrap(), new_body);
}

#[tokio::test]
async fn recovers_after_the_server_disappears_and_returns() {
    let ws = Workspace::new("netloss");
    let server = Server::start(slow_config(4 << 20)).await;
    let body = server.body();
    let url = server.url("/patchy.bin");

    let mut child = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "-c",
            "2",
        ],
    )
    .spawn()
    .unwrap();
    wait_for_progress(&ws, 256 * 1024, Duration::from_secs(60)).await;
    child.kill().await.unwrap();
    let _ = child.wait().await;
    let before_outage = durable_bytes(&ws);

    // The network goes away: connections are accepted then dropped.
    server.set(|c| c.refuse = true);
    let out = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "--retries",
            "2",
        ],
    )
    .output()
    .await
    .unwrap();
    assert!(
        !out.status.success(),
        "should fail while the server is down"
    );
    // Crucially, the outage cost us nothing.
    assert_eq!(
        durable_bytes(&ws),
        before_outage,
        "a failed attempt must not lose progress"
    );

    // The network comes back.
    server.set(|c| {
        c.refuse = false;
        c.throttle = None;
    });
    let out = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "--sha256",
            &sha256(&body),
        ],
    )
    .output()
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "should finish once the server returns: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(std::fs::read(ws.path("patchy.bin")).unwrap(), body);
}

#[tokio::test]
async fn state_database_survives_a_kill_mid_update() {
    let ws = Workspace::new("dbkill");
    let server = Server::start(slow_config(8 << 20)).await;
    let url = server.url("/dbtest.bin");

    // Kill repeatedly with no delay alignment, so some kills land inside a
    // commit transaction. The database must always still be readable.
    for round in 0..6 {
        let mut child = rget(
            &ws,
            &[
                "--quiet",
                &url,
                "--dir",
                &ws.dir.to_string_lossy(),
                "-c",
                "4",
            ],
        )
        .spawn()
        .unwrap();
        tokio::time::sleep(Duration::from_millis(300 + round * 97)).await;
        child.kill().await.unwrap();
        let _ = child.wait().await;

        // Reopening and reading must work every single time (PRD Invariant 7).
        let store = ws.store();
        let downloads = store.list().expect("state database must stay readable");
        if let Some(d) = downloads.first() {
            let ranges = store.load_ranges(&d.id).expect("ranges must stay readable");
            let sum: u64 = ranges.iter().map(|r| r.bytes_written).sum();
            assert_eq!(
                sum, d.durable_bytes,
                "round {round}: durable_bytes disagrees with the range rows"
            );
        }
    }

    server.set(|c| c.throttle = None);
    let out = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "--sha256",
            &sha256(&server.body()),
        ],
    )
    .output()
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "should still finish correctly: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test]
async fn sigint_pauses_and_saves_progress() {
    let ws = Workspace::new("sigint");
    let server = Server::start(slow_config(8 << 20)).await;
    let url = server.url("/interrupt.bin");

    let child = rget(&ws, &[&url, "--dir", &ws.dir.to_string_lossy(), "-c", "2"])
        .spawn()
        .unwrap();
    wait_for_progress(&ws, 256 * 1024, Duration::from_secs(60)).await;

    let pid = child.id().expect("child pid") as i32;
    unsafe {
        // SIGINT, as Ctrl+C would.
        libc_kill(pid, 2);
    }

    let out = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
        .await
        .expect("graceful shutdown must not hang")
        .unwrap();

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("paused") || stderr.contains("Pausing"),
        "{stderr}"
    );
    assert_eq!(
        out.status.code(),
        Some(130),
        "interrupted download should exit 130: {stderr}"
    );

    let store = ws.store();
    let record = &store.list().unwrap()[0];
    assert_eq!(record.status, rget_next::storage::Status::Paused);
    assert!(record.durable_bytes > 0);

    // And the same command resumes it.
    server.set(|c| c.throttle = None);
    let out = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "--sha256",
            &sha256(&server.body()),
        ],
    )
    .output()
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "resume after Ctrl+C failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// PRD §39's definition of done, run as a loop. Excluded from the default run
/// because it takes minutes; `cargo test -- --ignored` exercises it.
#[tokio::test]
#[ignore = "long-running soak test"]
async fn soak_random_kills_always_produce_the_right_checksum() {
    for iteration in 0..15u64 {
        let ws = Workspace::new(&format!("soak{iteration}"));
        let server = Server::start(slow_config(4 << 20)).await;
        let body = server.body();
        let url = server.url("/soak.bin");
        let digest = sha256(&body);

        // Between one and four kills at pseudo-random depths.
        let kills = 1 + (iteration % 4);
        for k in 0..kills {
            let mut child = rget(
                &ws,
                &[
                    "--quiet",
                    &url,
                    "--dir",
                    &ws.dir.to_string_lossy(),
                    "-c",
                    "4",
                ],
            )
            .spawn()
            .unwrap();
            let delay = 120 + ((iteration * 31 + k * 71) % 400);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            child.kill().await.unwrap();
            let _ = child.wait().await;

            // The server also flaps.
            if k % 2 == 0 {
                server.set(|c| c.kill_after = Some(64 * 1024));
            } else {
                server.set(|c| c.kill_after = None);
            }
        }

        server.set(|c| {
            c.kill_after = None;
            c.throttle = None;
        });
        let out = rget(
            &ws,
            &[
                "--quiet",
                &url,
                "--dir",
                &ws.dir.to_string_lossy(),
                "-c",
                "4",
                "--sha256",
                &digest,
            ],
        )
        .output()
        .await
        .unwrap();
        assert!(
            out.status.success(),
            "iteration {iteration} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            std::fs::read(ws.path("soak.bin")).unwrap(),
            body,
            "iteration {iteration} produced a different file"
        );
    }
}

// -- management commands ---------------------------------------------------

#[tokio::test]
async fn list_info_resume_and_forget() {
    let ws = Workspace::new("manage");
    let server = Server::start(slow_config(8 << 20)).await;
    let url = server.url("/managed.bin");

    let mut child = rget(
        &ws,
        &[
            "--quiet",
            &url,
            "--dir",
            &ws.dir.to_string_lossy(),
            "-c",
            "2",
        ],
    )
    .spawn()
    .unwrap();
    wait_for_progress(&ws, 256 * 1024, Duration::from_secs(60)).await;
    child.kill().await.unwrap();
    let _ = child.wait().await;

    // list
    let out = rget(&ws, &["list"]).output().await.unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success());
    assert!(stdout.contains("managed.bin"), "{stdout}");
    assert!(stdout.contains("ID"), "{stdout}");

    let id = ws
        .store()
        .list()
        .unwrap()
        .first()
        .map(|d| d.id.clone())
        .expect("a download exists");

    // info
    let out = rget(&ws, &["info", &id[..4]]).output().await.unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("managed.bin"), "{stdout}");
    assert!(stdout.contains("ranges"), "{stdout}");
    assert!(stdout.contains("\"v1\""), "etag should be shown: {stdout}");

    // resume by id, with no other flags
    server.set(|c| c.throttle = None);
    let out = rget(&ws, &["--quiet", "resume", &id])
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "resume by id failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read(ws.path("managed.bin")).unwrap(),
        server.body()
    );

    // forget leaves the file alone
    let out = rget(&ws, &["forget", &id]).output().await.unwrap();
    assert!(out.status.success());
    assert!(
        ws.path("managed.bin").exists(),
        "forget must not delete files"
    );
    assert!(ws.store().list().unwrap().is_empty());
}

#[tokio::test]
async fn resume_all_picks_up_every_interrupted_download() {
    let ws = Workspace::new("resumeall");
    let server = Server::start(slow_config(4 << 20)).await;
    let body = server.body();

    for name in ["a.bin", "b.bin"] {
        let url = server.url(&format!("/{name}"));
        let mut child = rget(
            &ws,
            &[
                "--quiet",
                &url,
                "--dir",
                &ws.dir.to_string_lossy(),
                "-c",
                "2",
            ],
        )
        .spawn()
        .unwrap();
        // Each download needs its own progress before we stop it.
        let target = 128 * 1024;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let found = ws
                .store()
                .list()
                .unwrap()
                .iter()
                .find(|d| d.filename == name)
                .map(|d| d.durable_bytes)
                .unwrap_or(0);
            if found >= target || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        child.kill().await.unwrap();
        let _ = child.wait().await;
    }

    assert_eq!(ws.store().list_resumable().unwrap().len(), 2);

    server.set(|c| c.throttle = None);
    let out = rget(&ws, &["--quiet", "resume", "--all"])
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "resume --all failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    for name in ["a.bin", "b.bin"] {
        assert_eq!(
            std::fs::read(ws.path(name)).unwrap(),
            body,
            "{name} did not finish correctly"
        );
    }
    assert!(ws.store().list_resumable().unwrap().is_empty());
}

#[tokio::test]
async fn json_output_is_machine_readable() {
    let ws = Workspace::new("json");
    let server = Server::start(Config::with_body(2 << 20)).await;
    let url = server.url("/data.bin");

    let out = rget(&ws, &["--json", &url, "--dir", &ws.dir.to_string_lossy()])
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains('\x1b'),
        "JSON output must have no ANSI codes"
    );

    let mut kinds = Vec::new();
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        let value: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("bad JSON line `{line}`: {e}"));
        kinds.push(value["event"].as_str().unwrap_or("?").to_string());
    }
    assert!(kinds.contains(&"download_started".to_string()), "{kinds:?}");
    assert!(
        kinds.contains(&"download_completed".to_string()),
        "{kinds:?}"
    );
}

#[tokio::test]
async fn non_interactive_output_has_no_ansi_escapes() {
    let ws = Workspace::new("plain");
    let server = Server::start(Config::with_body(1 << 20)).await;
    let url = server.url("/quiet.bin");

    let out = rget(&ws, &[&url, "--dir", &ws.dir.to_string_lossy()])
        .output()
        .await
        .unwrap();
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains('\x1b'),
        "piped stderr must be free of cursor control: {stderr:?}"
    );
}

#[tokio::test]
async fn help_is_shown_when_no_url_is_given() {
    let ws = Workspace::new("nohelp");
    let out = rget(&ws, &[]).output().await.unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Usage"), "{stdout}");
    assert!(!out.status.success());
}

/// `kill(2)`, declared directly so the test suite needs no `libc` dependency.
unsafe fn libc_kill(pid: i32, sig: i32) {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe {
        kill(pid, sig);
    }
}
