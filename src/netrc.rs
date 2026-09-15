//! `.netrc` credential lookup, so `rget` picks up saved logins the same way
//! `wget` and `curl` do (PRD-adjacent: no flag should be required for a host
//! the user has already told the rest of their tools how to authenticate to).

use std::path::PathBuf;

use directories::UserDirs;

/// `user:password` for `host`, read from `$NETRC` or `~/.netrc` (`_netrc` on
/// Windows). `None` if there is no file, no entry for this host, and no
/// `default` entry.
pub fn lookup(host: &str) -> Option<(String, String)> {
    let path = netrc_path()?;
    let content = std::fs::read_to_string(path).ok()?;
    find(&content, host)
}

fn netrc_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("NETRC") {
        return Some(PathBuf::from(p));
    }
    let home = UserDirs::new()?.home_dir().to_path_buf();
    #[cfg(windows)]
    let name = "_netrc";
    #[cfg(not(windows))]
    let name = ".netrc";
    Some(home.join(name))
}

/// A `login`/`password` pair, as collected while scanning one `machine` or
/// `default` block.
#[derive(Default)]
struct Entry {
    login: Option<String>,
    password: Option<String>,
}

impl Entry {
    fn into_pair(self) -> Option<(String, String)> {
        Some((self.login?, self.password?))
    }
}

/// Find the credentials for `host`, falling back to a `default` entry.
///
/// Exact match wins even if it appears after `default` in the file, since a
/// specific entry is always more useful than a catch-all.
fn find(content: &str, host: &str) -> Option<(String, String)> {
    let tokens = tokenize(content);
    let mut matched: Option<Entry> = None;
    let mut default_entry: Option<Entry> = None;

    let mut i = 0;
    while i < tokens.len() {
        match tokens[i].as_str() {
            "machine" => {
                i += 1;
                let name = tokens.get(i).cloned();
                i += 1;
                let (entry, next) = read_entry(&tokens, i);
                i = next;
                if name.as_deref() == Some(host) {
                    matched = Some(entry);
                }
            }
            "default" => {
                i += 1;
                let (entry, next) = read_entry(&tokens, i);
                i = next;
                default_entry = Some(entry);
            }
            _ => i += 1,
        }
    }

    matched
        .and_then(Entry::into_pair)
        .or_else(|| default_entry.and_then(Entry::into_pair))
}

/// Consume `login`/`password`/`account` pairs until the next `machine`,
/// `default`, or `macdef` keyword (or end of input).
fn read_entry(tokens: &[String], mut i: usize) -> (Entry, usize) {
    let mut entry = Entry::default();
    while i < tokens.len() {
        match tokens[i].as_str() {
            "machine" | "default" | "macdef" => break,
            "login" => {
                entry.login = tokens.get(i + 1).cloned();
                i += 2;
            }
            "password" => {
                entry.password = tokens.get(i + 1).cloned();
                i += 2;
            }
            _ => i += 1, // `account`, or anything unrecognised: skip the keyword
        }
    }
    (entry, i)
}

/// Split into whitespace-separated words, skipping comment lines and the
/// unstructured body of `macdef` macros (terminated by a blank line), which
/// otherwise would be misread as more `machine` blocks.
fn tokenize(content: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut lines = content.lines();
    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') {
            continue;
        }
        let words: Vec<&str> = trimmed.split_whitespace().collect();
        if words.first() == Some(&"macdef") {
            for l in lines.by_ref() {
                if l.trim().is_empty() {
                    break;
                }
            }
            continue;
        }
        out.extend(words.into_iter().map(str::to_string));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_matching_machine() {
        let content = "machine example.com\n  login alice\n  password s3cret\n";
        assert_eq!(
            find(content, "example.com"),
            Some(("alice".into(), "s3cret".into()))
        );
    }

    #[test]
    fn ignores_other_machines() {
        let content = "machine a.example\nlogin a\npassword pa\n\
                        machine b.example\nlogin b\npassword pb\n";
        assert_eq!(find(content, "b.example"), Some(("b".into(), "pb".into())));
        assert_eq!(find(content, "c.example"), None);
    }

    #[test]
    fn falls_back_to_default() {
        let content = "machine a.example\nlogin a\npassword pa\n\
                        default\nlogin anon\npassword anon-pass\n";
        assert_eq!(
            find(content, "unlisted.example"),
            Some(("anon".into(), "anon-pass".into()))
        );
        // An exact match still wins over `default`.
        assert_eq!(find(content, "a.example"), Some(("a".into(), "pa".into())));
    }

    #[test]
    fn handles_single_line_form() {
        let content = "machine example.com login alice password s3cret";
        assert_eq!(
            find(content, "example.com"),
            Some(("alice".into(), "s3cret".into()))
        );
    }

    #[test]
    fn skips_account_field() {
        let content = "machine example.com\nlogin alice\naccount ignored\npassword s3cret\n";
        assert_eq!(
            find(content, "example.com"),
            Some(("alice".into(), "s3cret".into()))
        );
    }

    #[test]
    fn skips_macdef_body() {
        let content = "macdef init\ncurl something\nmachine fake.example\n\n\
                        machine example.com\nlogin alice\npassword s3cret\n";
        // The `machine fake.example` line inside the macro body must not be
        // parsed as a real entry.
        assert_eq!(find(content, "fake.example"), None);
        assert_eq!(
            find(content, "example.com"),
            Some(("alice".into(), "s3cret".into()))
        );
    }

    #[test]
    fn ignores_comment_lines() {
        let content = "# a comment\nmachine example.com\nlogin alice\npassword s3cret\n";
        assert_eq!(
            find(content, "example.com"),
            Some(("alice".into(), "s3cret".into()))
        );
    }

    #[test]
    fn incomplete_entry_yields_nothing() {
        let content = "machine example.com\nlogin alice\n";
        assert_eq!(find(content, "example.com"), None);
    }
}
