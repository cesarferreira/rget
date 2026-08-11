//! Mirror handling (PRD §15).

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::{Config, Server, Workspace, sha256};
use rget::engine::{self, DownloadRequest};
use rget::http::HttpConfig;
use rget::integrity::{Algorithm, Checksum};
use rget::progress::{Event, NoteLevel, Reporter};
use rget::shutdown::Cancel;
use url::Url;

fn request(urls: Vec<String>, ws: &Workspace) -> DownloadRequest {
    DownloadRequest {
        urls: urls.iter().map(|u| Url::parse(u).unwrap()).collect(),
        output: Some("mirrored.bin".into()),
        dir: Some(ws.dir.to_string_lossy().to_string()),
        connections: 4,
        checksum: None,
        limit: None,
        http: HttpConfig {
            timeout: Duration::from_secs(5),
            ..Default::default()
        },
        retries: 6,
        overwrite: true,
        restart: false,
        preallocate: true,
    }
}

async fn run(ws: &Workspace, req: DownloadRequest) -> (anyhow::Result<()>, Vec<Event>) {
    let store = Arc::new(ws.store());
    let (reporter, mut rx) = Reporter::new();
    let result = engine::download(store, req, reporter.clone(), Cancel::new())
        .await
        .map(|_| ());
    drop(reporter);
    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    (result, events)
}

fn warnings(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Note {
                level: NoteLevel::Warn,
                message,
            } => Some(message.clone()),
            _ => None,
        })
        .collect()
}

/// Two servers serving identical bytes with identical strong ETags.
async fn twin_servers(size: usize) -> (Server, Server, Vec<u8>) {
    let a = Server::start(Config::with_body(size)).await;
    let body = a.body();
    let b = Server::start(Config {
        body: body.clone(),
        ..Config::with_body(0)
    })
    .await;
    (a, b, body)
}

#[tokio::test]
async fn falls_over_to_a_verified_mirror_when_the_primary_dies() {
    let ws = Workspace::new("failover");
    let (primary, mirror, body) = twin_servers(8 << 20).await;

    // The primary answers the probe, then refuses every transfer.
    let req = request(vec![primary.url("/f.bin"), mirror.url("/f.bin")], &ws);
    primary.set(|c| c.kill_after = Some(0));

    let (result, _) = run(&ws, req).await;
    result.expect("should complete via the mirror");

    assert_eq!(std::fs::read(ws.path("mirrored.bin")).unwrap(), body);
    assert!(
        mirror.request_count() > 1,
        "the mirror should have carried the transfer"
    );
}

#[tokio::test]
async fn rejects_a_mirror_that_cannot_be_proven_equivalent() {
    let ws = Workspace::new("unproven");
    let (primary, mirror, body) = twin_servers(1 << 20).await;
    // Same size and bytes, but a different ETag: unprovable without a checksum.
    mirror.set(|c| c.etag = Some("\"other\"".into()));

    let req = request(vec![primary.url("/f.bin"), mirror.url("/f.bin")], &ws);
    let (result, events) = run(&ws, req).await;
    result.expect("primary alone is enough");

    let warns = warnings(&events);
    assert!(
        warns.iter().any(|w| w.contains("ignoring mirror")),
        "expected a rejection warning, got {warns:?}"
    );
    assert_eq!(mirror.request_count(), 1, "only the probe should have run");
    assert_eq!(std::fs::read(ws.path("mirrored.bin")).unwrap(), body);
}

#[tokio::test]
async fn rejects_a_mirror_of_a_different_size() {
    let ws = Workspace::new("wrongsize");
    let primary = Server::start(Config::with_body(1 << 20)).await;
    let mirror = Server::start(Config::with_body((1 << 20) + 512)).await;

    let req = request(vec![primary.url("/f.bin"), mirror.url("/f.bin")], &ws);
    let (result, events) = run(&ws, req).await;
    result.unwrap();

    let warns = warnings(&events);
    assert!(
        warns.iter().any(|w| w.contains("size differs")),
        "a size mismatch must be called out, got {warns:?}"
    );
}

#[tokio::test]
async fn a_checksum_admits_an_otherwise_unprovable_mirror() {
    let ws = Workspace::new("guarded");
    let (primary, mirror, body) = twin_servers(8 << 20).await;
    mirror.set(|c| c.etag = Some("\"different-but-identical\"".into()));

    let mut req = request(vec![primary.url("/f.bin"), mirror.url("/f.bin")], &ws);
    req.checksum = Some(Checksum::parse(Algorithm::Sha256, &sha256(&body)).unwrap());
    // Force the primary out of the picture for transfers.
    primary.set(|c| c.kill_after = Some(0));

    let (result, events) = run(&ws, req).await;
    result.expect("the checksum makes the mirror safe to use");

    let notes: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            Event::Note { message, .. } => Some(message.clone()),
            _ => None,
        })
        .collect();
    assert!(
        notes
            .iter()
            .any(|n| n.contains("strength of the supplied checksum")),
        "should say why the mirror was admitted, got {notes:?}"
    );
    assert_eq!(std::fs::read(ws.path("mirrored.bin")).unwrap(), body);
}

#[tokio::test]
async fn an_unreachable_mirror_is_simply_skipped() {
    let ws = Workspace::new("unreachable");
    let (primary, _, body) = twin_servers(1 << 20).await;

    let req = request(
        vec![
            primary.url("/f.bin"),
            // Nothing is listening on this port.
            "http://127.0.0.1:1/f.bin".to_string(),
        ],
        &ws,
    );
    let (result, events) = run(&ws, req).await;
    result.expect("one dead mirror must not fail the download");

    let warns = warnings(&events);
    assert!(
        warns.iter().any(|w| w.contains("unreachable mirror")),
        "got {warns:?}"
    );
    assert_eq!(std::fs::read(ws.path("mirrored.bin")).unwrap(), body);
}
