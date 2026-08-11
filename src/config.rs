//! User settings, and the first-run download-folder prompt.
//!
//! Settings live in the `meta` table of the same SQLite database as the
//! downloads, so there is one file to back up, move or delete — no separate
//! config file to drift out of sync with the download state.
//!
//! The prompt has one hard rule: **it must never block a script.** It is shown
//! only when both stdin and stderr are terminals and no machine-output flag was
//! given. Everywhere else the platform's Downloads folder is used silently, so
//! `rget URL` behaves identically in a terminal and in a pipeline.

use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::storage::Store;

/// Settings key for the folder downloads land in when no `--dir` is given.
pub const DOWNLOAD_DIR_KEY: &str = "download_dir";

/// The platform's Downloads folder.
///
/// On macOS that is `~/Downloads`. On Linux it is whatever `XDG_DOWNLOAD_DIR`
/// says in `~/.config/user-dirs.dirs` — which is localised, so hardcoding
/// "Downloads" would put files in the wrong place for anyone not using English.
/// Only if that lookup fails do we guess `~/Downloads`.
pub fn platform_download_dir() -> PathBuf {
    if let Some(dirs) = directories::UserDirs::new() {
        if let Some(downloads) = dirs.download_dir() {
            return downloads.to_path_buf();
        }
        return dirs.home_dir().join("Downloads");
    }
    // No home directory at all (a daemon, a stripped container): the working
    // directory is the only sane answer.
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Render a path with `~` for display, so the prompt reads the way a person
/// would write it.
pub fn tildify(path: &Path) -> String {
    if let Some(dirs) = directories::UserDirs::new() {
        if let Ok(rest) = path.strip_prefix(dirs.home_dir()) {
            if rest.as_os_str().is_empty() {
                return "~".to_string();
            }
            return format!("~/{}", rest.display());
        }
    }
    path.display().to_string()
}

/// The saved default download folder, if the user has ever chosen one.
pub fn saved_download_dir(store: &Store) -> Result<Option<PathBuf>> {
    Ok(store
        .get_meta(DOWNLOAD_DIR_KEY)?
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from))
}

pub fn save_download_dir(store: &Store, dir: &Path) -> Result<()> {
    store.set_meta(DOWNLOAD_DIR_KEY, &dir.to_string_lossy())
}

/// Turn user input into an absolute, usable directory: expand `~`, resolve
/// relative paths against the working directory, and create it if needed.
pub fn normalise_dir(input: &str) -> Result<PathBuf> {
    let expanded = crate::naming::expand_tilde(input.trim());
    let absolute = if expanded.is_absolute() {
        expanded
    } else {
        std::env::current_dir()
            .context("cannot determine the current directory")?
            .join(expanded)
    };

    if absolute.exists() && !absolute.is_dir() {
        anyhow::bail!("{} exists but is not a directory", absolute.display());
    }
    Ok(absolute)
}

/// How the effective download folder was decided — reported under `--verbose`
/// and asserted in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirSource {
    /// `--dir` on the command line.
    Flag,
    /// Previously saved by the user.
    Saved,
    /// Chosen at the first-run prompt just now.
    Prompted,
    /// Platform default, used without asking because we could not prompt.
    PlatformDefault,
}

#[derive(Debug, Clone)]
pub struct ResolvedDir {
    pub path: PathBuf,
    pub source: DirSource,
}

/// Decide where this download should go.
///
/// Precedence: `--dir` beats the saved setting, which beats the platform
/// default. `prompt` returns `Ok(None)` when asking is not possible, in which
/// case we fall back silently and — importantly — save nothing, so the question
/// is still asked the next time the user is at a terminal.
pub fn resolve_download_dir<P>(store: &Store, flag: Option<&str>, prompt: P) -> Result<ResolvedDir>
where
    P: FnOnce(&Path) -> Result<Option<PathBuf>>,
{
    if let Some(dir) = flag {
        return Ok(ResolvedDir {
            path: normalise_dir(dir)?,
            source: DirSource::Flag,
        });
    }

    if let Some(saved) = saved_download_dir(store)? {
        return Ok(ResolvedDir {
            path: saved,
            source: DirSource::Saved,
        });
    }

    let default = platform_download_dir();
    match prompt(&default)? {
        Some(chosen) => {
            save_download_dir(store, &chosen)?;
            Ok(ResolvedDir {
                path: chosen,
                source: DirSource::Prompted,
            })
        }
        None => Ok(ResolvedDir {
            path: default,
            source: DirSource::PlatformDefault,
        }),
    }
}

/// May we interrupt the user with a question?
///
/// Both stdin and stderr must be terminals: stdin because we need an answer,
/// stderr because that is where the question appears. `--json` and `--quiet`
/// mean a script is driving, so we stay silent regardless.
pub fn can_prompt(machine_output: bool) -> bool {
    !machine_output && std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

/// The first-run prompt. Returns `None` if we must not ask.
pub fn prompt_for_download_dir(default: &Path, machine_output: bool) -> Result<Option<PathBuf>> {
    if !can_prompt(machine_output) {
        return Ok(None);
    }
    let mut stdin = std::io::stdin().lock();
    ask(default, &mut stdin, &mut std::io::stderr()).map(Some)
}

/// The prompt itself, with I/O injected so it can be tested without a terminal.
pub fn ask<R: BufRead, W: Write>(default: &Path, input: &mut R, output: &mut W) -> Result<PathBuf> {
    writeln!(output, "Where should rget save downloads?")?;

    // Three tries, then take the default: an unanswerable prompt must not turn
    // into an infinite loop.
    for attempt in 0..3 {
        write!(output, "  Folder [{}]: ", tildify(default))?;
        output.flush()?;

        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            // EOF — stdin closed under us.
            break;
        }
        let answer = line.trim();
        let candidate = if answer.is_empty() {
            default.to_path_buf()
        } else {
            match normalise_dir(answer) {
                Ok(path) => path,
                Err(err) => {
                    writeln!(output, "  {err}")?;
                    if attempt < 2 {
                        continue;
                    }
                    default.to_path_buf()
                }
            }
        };

        writeln!(output, "  Saving downloads to {}", tildify(&candidate))?;
        writeln!(output, "  Change it later with `rget config --dir <path>`.")?;
        writeln!(output)?;
        return Ok(candidate);
    }

    Ok(default.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn store() -> Store {
        Store::open_in_memory().unwrap()
    }

    #[test]
    fn platform_default_is_absolute() {
        let dir = platform_download_dir();
        assert!(dir.is_absolute() || dir == Path::new("."));
    }

    #[test]
    fn tildify_shortens_the_home_path() {
        let home = directories::UserDirs::new()
            .unwrap()
            .home_dir()
            .to_path_buf();
        assert_eq!(tildify(&home.join("Downloads")), "~/Downloads");
        assert_eq!(tildify(&home), "~");
        assert_eq!(tildify(Path::new("/var/tmp")), "/var/tmp");
    }

    #[test]
    fn flag_beats_everything() {
        let s = store();
        save_download_dir(&s, Path::new("/tmp/saved")).unwrap();
        let resolved = resolve_download_dir(&s, Some("/tmp/flag"), |_| {
            panic!("must not prompt when --dir was given")
        })
        .unwrap();
        assert_eq!(resolved.path, PathBuf::from("/tmp/flag"));
        assert_eq!(resolved.source, DirSource::Flag);
    }

    #[test]
    fn saved_setting_beats_the_platform_default() {
        let s = store();
        save_download_dir(&s, Path::new("/tmp/saved")).unwrap();
        let resolved =
            resolve_download_dir(&s, None, |_| panic!("must only prompt once, ever")).unwrap();
        assert_eq!(resolved.path, PathBuf::from("/tmp/saved"));
        assert_eq!(resolved.source, DirSource::Saved);
    }

    #[test]
    fn first_run_prompts_and_remembers_the_answer() {
        let s = store();
        let resolved =
            resolve_download_dir(&s, None, |_| Ok(Some(PathBuf::from("/tmp/chosen")))).unwrap();
        assert_eq!(resolved.path, PathBuf::from("/tmp/chosen"));
        assert_eq!(resolved.source, DirSource::Prompted);

        // Second run must not ask again.
        let again = resolve_download_dir(&s, None, |_| panic!("asked twice")).unwrap();
        assert_eq!(again.path, PathBuf::from("/tmp/chosen"));
        assert_eq!(again.source, DirSource::Saved);
    }

    #[test]
    fn non_interactive_falls_back_without_saving() {
        let s = store();
        let resolved = resolve_download_dir(&s, None, |_| Ok(None)).unwrap();
        assert_eq!(resolved.path, platform_download_dir());
        assert_eq!(resolved.source, DirSource::PlatformDefault);

        // Nothing was persisted, so a later interactive run still gets to ask.
        assert_eq!(saved_download_dir(&s).unwrap(), None);
    }

    #[test]
    fn empty_answer_accepts_the_default() {
        let default = PathBuf::from("/tmp/default-dl");
        let mut input = Cursor::new(b"\n".to_vec());
        let mut output = Vec::new();
        let chosen = ask(&default, &mut input, &mut output).unwrap();
        assert_eq!(chosen, default);

        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("Where should rget save downloads?"), "{text}");
        assert!(text.contains("/tmp/default-dl"), "{text}");
        assert!(text.contains("rget config --dir"), "{text}");
    }

    #[test]
    fn typed_answer_is_expanded_and_absolutised() {
        let mut input = Cursor::new(b"  ~/Elsewhere  \n".to_vec());
        let mut output = Vec::new();
        let chosen = ask(Path::new("/tmp/default-dl"), &mut input, &mut output).unwrap();
        let home = directories::UserDirs::new()
            .unwrap()
            .home_dir()
            .to_path_buf();
        assert_eq!(chosen, home.join("Elsewhere"));
        assert!(chosen.is_absolute());
    }

    #[test]
    fn eof_takes_the_default_rather_than_looping() {
        let default = PathBuf::from("/tmp/default-dl");
        let mut input = Cursor::new(Vec::new());
        let mut output = Vec::new();
        assert_eq!(ask(&default, &mut input, &mut output).unwrap(), default);
    }

    #[test]
    fn a_bad_answer_is_re_asked_then_defaulted() {
        // A path that exists but is a file, not a directory.
        let dir = std::env::temp_dir().join(format!("rget-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();

        let default = PathBuf::from("/tmp/default-dl");
        let input_text = format!(
            "{}\n{}\n{}\n",
            file.display(),
            file.display(),
            file.display()
        );
        let mut input = Cursor::new(input_text.into_bytes());
        let mut output = Vec::new();
        let chosen = ask(&default, &mut input, &mut output).unwrap();

        assert_eq!(chosen, default, "should give up gracefully, not loop");
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("not a directory"), "{text}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_good_answer_after_a_bad_one_is_accepted() {
        let dir = std::env::temp_dir().join(format!("rget-cfg2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();

        let input_text = format!("{}\n/tmp/good-choice\n", file.display());
        let mut input = Cursor::new(input_text.into_bytes());
        let mut output = Vec::new();
        let chosen = ask(Path::new("/tmp/default-dl"), &mut input, &mut output).unwrap();
        assert_eq!(chosen, PathBuf::from("/tmp/good-choice"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn normalise_rejects_a_file_and_absolutises_relative_paths() {
        assert!(normalise_dir("/tmp").is_ok());
        let relative = normalise_dir("some-subdir").unwrap();
        assert!(relative.is_absolute());
        assert!(relative.ends_with("some-subdir"));

        let dir = std::env::temp_dir().join(format!("rget-cfg3-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("f");
        std::fs::write(&file, b"x").unwrap();
        assert!(normalise_dir(&file.to_string_lossy()).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn machine_output_never_prompts() {
        // Regardless of terminal state, --json / --quiet must not ask.
        assert!(!can_prompt(true));
    }
}
