//! The holder name an engine run (and its leases) records for the process that runs it when no
//! job name is given: `mrsm:<host>:<pid>`.
//!
//! Holders are opaque wherever they are compared (leases, `show runs`): only equality matters.
//! The run event stream reads them to tell whether a run's process is gone, which is only provable
//! for a process of the reading host. 5.0 release candidates wrote `mrsm:<pid>` without the host;
//! such holders still parse, as a process of an unknown host ([`HolderProcess::Elsewhere`]).

use std::sync::OnceLock;

/// What a holder says about the process holding a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HolderProcess {
    /// A process of this host, by PID.
    Local(u32),
    /// A process of another host, or of a host the holder does not name (`mrsm:<pid>`).
    Elsewhere,
    /// Not a process holder (a job name, a lease token, ...).
    NotAProcess,
}

/// This host's name as holders record it (no `:` or whitespace), `None` when it is unknown.
pub fn host_name() -> Option<&'static str> {
    static HOST: OnceLock<Option<String>> = OnceLock::new();
    HOST.get_or_init(|| sanitize(&raw_host_name()?)).as_deref()
}

fn sanitize(name: &str) -> Option<String> {
    let name: String = name
        .trim()
        .chars()
        .map(|c| {
            if c == ':' || c.is_whitespace() {
                '_'
            } else {
                c
            }
        })
        .collect();
    (!name.is_empty()).then_some(name)
}

#[cfg(unix)]
fn raw_host_name() -> Option<String> {
    let uname = rustix::system::uname();
    Some(uname.nodename().to_string_lossy().into_owned())
}

#[cfg(not(unix))]
fn raw_host_name() -> Option<String> {
    std::env::var("COMPUTERNAME").ok()
}

/// The holder of this process: `mrsm:<host>:<pid>` (`mrsm:<pid>` when the host is unknown).
pub fn process_holder() -> String {
    holder_for(host_name(), std::process::id())
}

fn holder_for(host: Option<&str>, pid: u32) -> String {
    match host {
        Some(host) => format!("mrsm:{host}:{pid}"),
        None => format!("mrsm:{pid}"),
    }
}

/// Read a holder against this host.
pub fn holder_process(holder: &str) -> HolderProcess {
    parse(holder, host_name())
}

fn parse(holder: &str, local_host: Option<&str>) -> HolderProcess {
    let Some(rest) = holder.strip_prefix("mrsm:") else {
        return HolderProcess::NotAProcess;
    };
    let (host, pid) = match rest.rsplit_once(':') {
        Some((host, pid)) => (Some(host), pid),
        None => (None, rest),
    };
    let Ok(pid) = pid.parse::<u32>() else {
        return HolderProcess::NotAProcess;
    };
    match (host, local_host) {
        (Some(host), Some(local)) if !host.is_empty() && host == local => HolderProcess::Local(pid),
        _ => HolderProcess::Elsewhere,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holders_name_their_host() {
        assert_eq!(holder_for(Some("box"), 42), "mrsm:box:42");
        assert_eq!(holder_for(None, 42), "mrsm:42");
        assert_eq!(parse("mrsm:box:42", Some("box")), HolderProcess::Local(42));
        assert_eq!(
            parse("mrsm:other:42", Some("box")),
            HolderProcess::Elsewhere
        );
        // A release candidate's holder names no host: it may be anywhere.
        assert_eq!(parse("mrsm:42", Some("box")), HolderProcess::Elsewhere);
        assert_eq!(parse("mrsm:box:42", None), HolderProcess::Elsewhere);
        assert_eq!(parse("mrsm::42", Some("")), HolderProcess::Elsewhere);
        assert_eq!(parse("my-job", Some("box")), HolderProcess::NotAProcess);
        assert_eq!(parse("mrsm:box:x", Some("box")), HolderProcess::NotAProcess);
        assert_eq!(sanitize(" a:b c "), Some("a_b_c".into()));
        assert_eq!(sanitize("  "), None);
    }

    #[test]
    fn this_process_is_local() {
        let holder = process_holder();
        if host_name().is_some() {
            assert_eq!(
                holder_process(&holder),
                HolderProcess::Local(std::process::id())
            );
        }
    }
}
