//! End-to-end download tests against the hostile test server (PRD §32).

mod harness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use harness::{Config, Server, Workspace, sha256};
use rget_next::engine::{self, DownloadReport, DownloadRequest};
use rget_next::http::HttpConfig;
use rget_next::integrity::{Algorithm, Checksum};
use rget_next::progress::{Event, Reporter};
use rget_next::shutdown::Cancel;
use url::Url;

fn request(server: &Server, path: &str, ws: &Workspace) -> DownloadRequest {
    DownloadRequest {
        urls: vec![Url::parse(&server.url(path)).unwrap()],
        output: None,
        dir: Some(ws.dir.to_string_lossy().to_string()),
        connections: 8,
        checksum: None,
        limit: None,
        http: HttpConfig {
            timeout: Duration::from_secs(5),
            ..Default::default()
        },
        retries: 6,
        overwrite: false,
        restart: false,
        preallocate: true,
    }
}

async fn run(ws: &Workspace, req: DownloadRequest) -> (anyhow::Result<DownloadReport>, Vec<Event>) {
    let store = Arc::new(ws.store());
    let (reporter, mut rx) = Reporter::new();
    let result = engine::download(store, req, reporter.clone(), Cancel::new()).await;
    drop(reporter);
    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    (result, events)
}

#[tokio::test]
async fn downloads_in_parallel_and_reassembles_exactly() {
    let ws = Workspace::new("parallel");
    // Big enough that the planner creates several chunks.
    let server = Server::start(Config::with_body(40 << 20)).await;
    let body = server.body();

    let mut req = request(&server, "/linux.iso", &ws);
    req.checksum = Some(Checksum::parse(Algorithm::Sha256, &sha256(&body)).unwrap());

    let (result, _) = run(&ws, req).await;
    let report = result.expect("download should succeed");

    assert_eq!(report.verified, Some(true));
    assert_eq!(report.filename, "linux.iso");
    assert_eq!(std::fs::read(&report.path).unwrap(), body);
    assert!(
        server.stats().ranges.len() >= 8,
        "expected parallel ranges, saw {}",
        server.stats().ranges.len()
    );
}

#[tokio::test]
async fn single_connection_still_works() {
    let ws = Workspace::new("single");
    let server = Server::start(Config::with_body(512 * 1024)).await;
    let body = server.body();

    let mut req = request(&server, "/one.bin", &ws);
    req.connections = 1;

    let (result, _) = run(&ws, req).await;
    let report = result.unwrap();
    assert_eq!(std::fs::read(&report.path).unwrap(), body);
}

#[tokio::test]
async fn falls_back_to_sequential_without_range_support() {
    let ws = Workspace::new("noranges");
    let server = Server::start(Config {
        accept_ranges: false,
        ..Config::with_body(512 * 1024)
    })
    .await;
    let body = server.body();

    let (result, _) = run(&ws, request(&server, "/plain.bin", &ws)).await;
    let report = result.expect("sequential fallback should need no intervention");
    assert_eq!(std::fs::read(&report.path).unwrap(), body);

    // The probe legitimately asks for `bytes=0-0` to find out whether ranges
    // work. Once it knows they do not, no transfer may ask for a range.
    let stats = server.stats();
    assert!(
        stats.ranges.iter().all(|r| *r == (0, Some(0))),
        "transfer sent a Range to a server that does not support it: {:?}",
        stats.ranges
    );
    assert!(
        stats.plain_requests >= 1,
        "expected a plain GET for the body"
    );
}

#[tokio::test]
async fn detects_a_server_that_ignores_range() {
    let ws = Workspace::new("ignorerange");
    // Advertises Accept-Ranges but serves the whole body regardless.
    let server = Server::start(Config {
        ignore_range: true,
        ..Config::with_body(512 * 1024)
    })
    .await;
    let body = server.body();

    let (result, _) = run(&ws, request(&server, "/liar.bin", &ws)).await;
    let report = result.expect("should fall back rather than corrupt the file");
    assert_eq!(std::fs::read(&report.path).unwrap(), body);
}

#[tokio::test]
async fn survives_an_unparseable_content_range() {
    let ws = Workspace::new("badrange");
    let server = Server::start(Config {
        malformed_content_range: true,
        ..Config::with_body(256 * 1024)
    })
    .await;
    let body = server.body();

    let (result, _) = run(&ws, request(&server, "/bad.bin", &ws)).await;
    let report = result.expect("should degrade to a sequential download");
    assert_eq!(std::fs::read(&report.path).unwrap(), body);
}

#[tokio::test]
async fn handles_unknown_content_length() {
    let ws = Workspace::new("unknownlen");
    // No Content-Length at all: the server closes the connection to signal EOF.
    let server = Server::start(Config {
        accept_ranges: false,
        content_length_delta: 0,
        ..Config::with_body(128 * 1024)
    })
    .await;
    let body = server.body();

    let (result, _) = run(&ws, request(&server, "/stream.bin", &ws)).await;
    let report = result.unwrap();
    assert_eq!(std::fs::read(&report.path).unwrap(), body);
}

#[tokio::test]
async fn follows_redirects_and_names_from_the_final_url() {
    let ws = Workspace::new("redirect");
    let server = Server::start(Config::with_body(64 * 1024)).await;
    let body = server.body();

    let (result, _) = run(&ws, request(&server, "/redirect/3", &ws)).await;
    let report = result.unwrap();
    assert_eq!(report.filename, "final-name.bin");
    assert_eq!(std::fs::read(&report.path).unwrap(), body);
}

#[tokio::test]
async fn refuses_a_redirect_loop() {
    let ws = Workspace::new("redirectloop");
    let server = Server::start(Config {
        redirect_loop: true,
        ..Config::with_body(1024)
    })
    .await;

    let (result, _) = run(&ws, request(&server, "/loop.bin", &ws)).await;
    assert!(result.is_err(), "a redirect loop must not hang or succeed");
}

#[tokio::test]
async fn uses_content_disposition_for_the_filename() {
    let ws = Workspace::new("disposition");
    let server = Server::start(Config {
        content_disposition: Some("attachment; filename=\"named.tar.gz\"".into()),
        ..Config::with_body(4096)
    })
    .await;

    let (result, _) = run(&ws, request(&server, "/download", &ws)).await;
    let report = result.unwrap();
    assert_eq!(report.filename, "named.tar.gz");
    assert_eq!(report.path.parent().unwrap(), ws.dir);
}

#[tokio::test]
async fn neutralises_a_malicious_content_disposition() {
    let ws = Workspace::new("traversal");
    let server = Server::start(Config {
        content_disposition: Some("attachment; filename=\"../../../../tmp/rget-pwned.txt\"".into()),
        ..Config::with_body(4096)
    })
    .await;

    let (result, _) = run(&ws, request(&server, "/evil", &ws)).await;
    let report = result.unwrap();
    assert_eq!(report.filename, "rget-pwned.txt");
    assert_eq!(
        report.path,
        ws.path("rget-pwned.txt"),
        "server must not escape the download directory"
    );
    assert!(!std::path::Path::new("/tmp/rget-pwned.txt").exists());
}

#[tokio::test]
async fn explicit_output_wins_over_the_server() {
    let ws = Workspace::new("output");
    let server = Server::start(Config {
        content_disposition: Some("attachment; filename=\"server-choice.bin\"".into()),
        ..Config::with_body(4096)
    })
    .await;

    let mut req = request(&server, "/whatever", &ws);
    req.output = Some("mine.bin".into());
    let (result, _) = run(&ws, req).await;
    assert_eq!(result.unwrap().filename, "mine.bin");
}

#[tokio::test]
async fn retries_a_429_and_honours_retry_after() {
    let ws = Workspace::new("throttled");
    let server = Server::start(Config {
        fail_next: Some(harness::Failure {
            remaining: 2,
            status: 429,
            retry_after: Some(1),
        }),
        ..Config::with_body(64 * 1024)
    })
    .await;
    let body = server.body();

    let started = Instant::now();
    let (result, events) = run(&ws, request(&server, "/busy.bin", &ws)).await;
    let report = result.expect("429 is transient and must be retried");

    assert_eq!(std::fs::read(&report.path).unwrap(), body);
    // Retry-After: 1 was respected at least once.
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "Retry-After was ignored"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::RetryScheduled { .. })),
        "a retry should be reported to the UI"
    );
}

#[tokio::test]
async fn retries_server_errors() {
    let ws = Workspace::new("5xx");
    for status in [500u16, 502, 503] {
        let server = Server::start(Config {
            fail_next: Some(harness::Failure {
                remaining: 1,
                status,
                retry_after: None,
            }),
            ..Config::with_body(32 * 1024)
        })
        .await;
        let body = server.body();
        let mut req = request(&server, &format!("/e{status}.bin"), &ws);
        req.output = Some(format!("e{status}.bin"));

        let (result, _) = run(&ws, req).await;
        let report = result.unwrap_or_else(|e| panic!("HTTP {status} should be retried: {e:#}"));
        assert_eq!(std::fs::read(&report.path).unwrap(), body);
    }
}

#[tokio::test]
async fn gives_up_on_a_permanent_error() {
    let ws = Workspace::new("404");
    let server = Server::start(Config {
        fail_next: Some(harness::Failure {
            remaining: 1000,
            status: 404,
            retry_after: None,
        }),
        ..Config::with_body(1024)
    })
    .await;

    let (result, _) = run(&ws, request(&server, "/missing.bin", &ws)).await;
    assert!(result.is_err());
    // A 404 must not be retried at all: one probe, no more.
    assert!(
        server.request_count() <= 2,
        "404 was retried {} times",
        server.request_count()
    );
}

#[tokio::test]
async fn recovers_when_the_connection_dies_mid_range() {
    let ws = Workspace::new("midkill");
    // Every response hangs up after 4 KiB, so finishing takes many reconnects.
    let server = Server::start(Config {
        kill_after: Some(4096),
        ..Config::with_body(64 * 1024)
    })
    .await;
    let body = server.body();

    let mut req = request(&server, "/flaky.bin", &ws);
    req.connections = 2;
    req.retries = 4;

    let (result, _) = run(&ws, req).await;
    let report = result.expect("a connection that keeps making progress must not be abandoned");
    assert_eq!(std::fs::read(&report.path).unwrap(), body);
    assert!(
        server.request_count() > 8,
        "expected many reconnects, saw {}",
        server.request_count()
    );
}

#[tokio::test]
async fn times_out_a_stalled_response() {
    let ws = Workspace::new("stall");
    let server = Server::start(Config {
        delay_before_body: Some(Duration::from_secs(30)),
        ..Config::with_body(64 * 1024)
    })
    .await;

    let mut req = request(&server, "/slow.bin", &ws);
    req.http.timeout = Duration::from_millis(300);
    req.retries = 1;

    let started = Instant::now();
    let (result, _) = run(&ws, req).await;
    assert!(result.is_err(), "a stalled body must time out");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "timeout did not fire promptly"
    );
}

#[tokio::test]
async fn checksum_mismatch_is_a_failure() {
    let ws = Workspace::new("badsum");
    let server = Server::start(Config::with_body(32 * 1024)).await;

    let mut req = request(&server, "/data.bin", &ws);
    req.checksum = Some(Checksum::parse(Algorithm::Sha256, &"a".repeat(64)).unwrap());

    let (result, events) = run(&ws, req).await;
    let err = result.expect_err("a checksum mismatch must never be reported as success");
    assert!(err.to_string().contains("checksum mismatch"), "{err:#}");

    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::VerificationCompleted { ok: false, .. }))
    );
    // And the record is marked failed, not complete.
    let store = ws.store();
    let all = store.list().unwrap();
    assert_eq!(all[0].status, rget_next::storage::Status::Failed);
}

#[tokio::test]
async fn verifies_blake3_and_sha512_too() {
    let ws = Workspace::new("algos");
    let server = Server::start(Config::with_body(96 * 1024)).await;
    let body = server.body();

    for algo in [Algorithm::Blake3, Algorithm::Sha512] {
        let digest = rget_next::integrity::hash_bytes(algo, &body);
        let mut req = request(&server, "/multi.bin", &ws);
        req.output = Some(format!("{algo}.bin"));
        req.checksum = Some(Checksum::parse(algo, &digest).unwrap());
        let (result, _) = run(&ws, req).await;
        assert_eq!(result.unwrap().verified, Some(true), "{algo} failed");
    }
}

#[tokio::test]
async fn refuses_to_overwrite_an_unrelated_file() {
    let ws = Workspace::new("existing");
    let server = Server::start(Config::with_body(4096)).await;
    std::fs::write(ws.path("keep.bin"), b"precious").unwrap();

    let mut req = request(&server, "/keep.bin", &ws);
    let (result, _) = run(&ws, req.clone()).await;
    let err = result.expect_err("must not clobber an existing file");
    assert!(err.to_string().contains("already exists"), "{err:#}");
    assert_eq!(std::fs::read(ws.path("keep.bin")).unwrap(), b"precious");

    // --overwrite is the explicit opt-in.
    req.overwrite = true;
    let (result, _) = run(&ws, req).await;
    let report = result.expect("--overwrite should proceed");
    assert_eq!(std::fs::read(&report.path).unwrap(), server.body());
}

#[tokio::test]
async fn applies_a_global_bandwidth_limit() {
    let ws = Workspace::new("limit");
    let server = Server::start(Config::with_body(300 * 1024)).await;

    let mut req = request(&server, "/capped.bin", &ws);
    // The bucket starts with one second of budget, so ~200 KiB must be earned
    // at 100 KiB/s: at least two seconds of transfer.
    req.limit = Some(100 * 1024);
    req.connections = 8;

    let started = Instant::now();
    let (result, _) = run(&ws, req).await;
    result.unwrap();
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(1500),
        "limit applied per connection rather than globally: finished in {elapsed:?}"
    );
}

#[tokio::test]
async fn reports_the_expected_event_sequence() {
    let ws = Workspace::new("events");
    let server = Server::start(Config::with_body(8 << 20)).await;
    let body = server.body();

    let mut req = request(&server, "/events.bin", &ws);
    req.checksum = Some(Checksum::parse(Algorithm::Sha256, &sha256(&body)).unwrap());

    let (result, events) = run(&ws, req).await;
    result.unwrap();

    let started = events
        .iter()
        .filter(|e| matches!(e, Event::DownloadStarted { .. }))
        .count();
    assert_eq!(started, 1);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::RangeStarted { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::RangeCompleted { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::BytesWritten { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Checkpointed { .. }))
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::VerificationStarted { .. }))
    );
    assert!(matches!(
        events.last(),
        Some(Event::DownloadCompleted { .. })
    ));
}

#[tokio::test]
async fn records_state_in_the_store() {
    let ws = Workspace::new("state");
    let server = Server::start(Config::with_body(20 << 20)).await;

    let (result, _) = run(&ws, request(&server, "/tracked.bin", &ws)).await;
    let report = result.unwrap();

    let store = ws.store();
    let record = store.get(&report.id).unwrap().expect("record persisted");
    assert_eq!(record.status, rget_next::storage::Status::Complete);
    assert_eq!(record.total_size, Some(20 << 20));
    assert_eq!(record.durable_bytes, 20 << 20);
    assert_eq!(record.etag.as_deref(), Some("\"v1\""));
    assert!(record.accept_ranges);
    assert!(
        record.file_ino.is_some(),
        "file identity should be recorded"
    );

    let ranges = store.load_ranges(&report.id).unwrap();
    assert!(!ranges.is_empty());
    assert!(
        ranges
            .iter()
            .all(|r| r.state == rget_next::storage::RangeState::Complete),
        "every range should be complete"
    );
    // The plan still partitions the file exactly.
    let mut cursor = 0u64;
    let mut sorted = ranges.clone();
    sorted.sort_by_key(|r| r.start);
    for r in &sorted {
        assert_eq!(r.start, cursor);
        cursor = r.end + 1;
    }
    assert_eq!(cursor, 20 << 20);
}

#[tokio::test]
async fn does_not_leak_credentials_into_errors() {
    let ws = Workspace::new("creds");
    let server = Server::start(Config {
        refuse: true,
        ..Config::with_body(1024)
    })
    .await;

    let mut req = request(&server, "/secret.bin?token=SUPERSECRET", &ws);
    req.http.basic_auth = Some(("alice".into(), "hunter2".into()));

    let (result, _) = run(&ws, req).await;
    let err = format!("{:#}", result.expect_err("server refuses connections"));
    assert!(!err.contains("SUPERSECRET"), "query leaked: {err}");
    assert!(!err.contains("hunter2"), "password leaked: {err}");
}
