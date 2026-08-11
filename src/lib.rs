//! `rget` — a resumable, parallel HTTP download manager.
//!
//! The crate is split so that each concern can be tested on its own:
//!
//! | module | responsibility |
//! |---|---|
//! | [`cli`] | argument parsing, command dispatch |
//! | [`http`] | requests, redirects, metadata probing, range requests |
//! | [`engine`] | orchestration: probe → plan → transfer → verify |
//! | [`scheduler`] | range planning, worker assignment, dynamic splitting |
//! | [`worker`] | one range transfer at a time |
//! | [`storage`] | SQLite persistence |
//! | [`file`] | random-access writes, preallocation |
//! | [`resume`] | recovery, reconciliation, remote validation |
//! | [`retry`] | retry policy and backoff |
//! | [`integrity`] | checksums |
//! | [`progress`] | telemetry model, speed and ETA maths |
//! | [`ui`] | terminal rendering |
//! | [`mirror`] | mirror equivalence and source selection |
//! | [`naming`] | destination filename selection and sanitisation |
//! | [`limit`] | global bandwidth limiting |
//!
//! The one design document worth reading before changing anything:
//! `docs/CRASH_CONSISTENCY.md`.

pub mod cli;
pub mod engine;
pub mod error;
pub mod file;
pub mod fmt;
pub mod http;
pub mod integrity;
pub mod limit;
pub mod mirror;
pub mod naming;
pub mod progress;
pub mod resume;
pub mod retry;
pub mod scheduler;
pub mod shutdown;
pub mod storage;
pub mod ui;
pub mod worker;
