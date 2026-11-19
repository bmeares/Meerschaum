//! The host side of the worker transport.
//!
//! - **Unix** (5.1): a socket pair. The host keeps one end and the worker process inherits the
//!   other (its descriptor number is passed as `--socket fd:<n>`), so nothing listens: no other
//!   process, of this user or another, can connect to the channel, impersonate the worker or
//!   receive its calls (an isolated team's sandboxed code included, on kernels where Landlock
//!   does not restrict connecting to pathname Unix sockets).
//! - **Windows**: a named pipe the host creates; the worker opens the name passed as `--socket`.

use std::io;
use std::path::Path;

use tokio::io::{AsyncRead, AsyncWrite};

/// The read half of a connection.
pub type ReadHalf = Box<dyn AsyncRead + Send + Unpin>;
/// The write half of a connection.
pub type WriteHalf = Box<dyn AsyncWrite + Send + Unpin>;

/// The address prefix of an inherited descriptor (`fd:<n>`).
pub const FD_PREFIX: &str = "fd:";

#[cfg(windows)]
fn unique_name() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("mrsm-w-{}-{}", std::process::id(), &id[..12])
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::os::fd::{AsFd, AsRawFd, OwnedFd};

    /// A worker's channel: the host's end of a socket pair and the end the worker inherits.
    pub struct Channel {
        parent: Option<std::os::unix::net::UnixStream>,
        child: Option<OwnedFd>,
        child_fd: i32,
    }

    impl Channel {
        /// A new socket pair (`base` is not used: nothing is bound).
        pub fn new(_base: Option<&Path>) -> io::Result<Channel> {
            let (parent, child) = std::os::unix::net::UnixStream::pair()?;
            // Above the standard streams, which the child's spawn replaces.
            let child: OwnedFd = if child.as_raw_fd() < 3 {
                rustix::io::fcntl_dupfd_cloexec(child.as_fd(), 3)?
            } else {
                child.into()
            };
            let child_fd = child.as_raw_fd();
            Ok(Channel {
                parent: Some(parent),
                child: Some(child),
                child_fd,
            })
        }

        /// The address to pass to the worker (`fd:<n>`).
        pub fn address(&self) -> String {
            format!("{FD_PREFIX}{}", self.child_fd)
        }

        /// Make the worker's end survive into `command`'s process (it is closed on `exec` in
        /// every other process this one starts).
        #[allow(unsafe_code)]
        pub fn prepare(&self, command: &mut tokio::process::Command) {
            let fd = self.child_fd;
            // SAFETY: the closure runs in the forked child before `exec` and only calls
            // `fcntl(fd, F_SETFD, 0)`, which is async-signal-safe and allocates nothing. `fd` is
            // the child's copy of the descriptor this `Channel` holds open until after the spawn
            // (`spawned`), so it is a valid open descriptor there.
            unsafe {
                command.pre_exec(move || {
                    let fd = std::os::fd::BorrowedFd::borrow_raw(fd);
                    rustix::io::fcntl_setfd(fd, rustix::io::FdFlags::empty())?;
                    Ok(())
                });
            }
        }

        /// The worker was started: close this process's copy of its end.
        pub fn spawned(&mut self) {
            self.child = None;
        }

        /// The host's end (once).
        pub async fn connect(&mut self) -> io::Result<(ReadHalf, WriteHalf)> {
            self.child = None;
            let parent = self
                .parent
                .take()
                .ok_or_else(|| io::Error::other("the worker's channel was already taken"))?;
            parent.set_nonblocking(true)?;
            let stream = tokio::net::UnixStream::from_std(parent)?;
            let (r, w) = stream.into_split();
            Ok((Box::new(r), Box::new(w)))
        }
    }
}

// The Windows named-pipe transport compiles but is not exercised by CI yet (the Python side
// opens the pipe as a file).
#[cfg(windows)]
mod imp {
    use super::*;
    use tokio::net::windows::named_pipe::ServerOptions;

    /// A named pipe server waiting for the worker.
    pub struct Channel {
        name: String,
        server: tokio::sync::Mutex<Option<tokio::net::windows::named_pipe::NamedPipeServer>>,
    }

    impl Channel {
        /// Create a new named pipe (`base` is ignored on Windows).
        pub fn new(_base: Option<&Path>) -> io::Result<Channel> {
            let name = format!(r"\\.\pipe\{}", unique_name());
            let server = ServerOptions::new()
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .create(&name)?;
            Ok(Channel {
                name,
                server: tokio::sync::Mutex::new(Some(server)),
            })
        }

        /// The address to pass to the worker.
        pub fn address(&self) -> String {
            self.name.clone()
        }

        /// Nothing to do on Windows.
        pub fn prepare(&self, _command: &mut tokio::process::Command) {}

        /// Nothing to do on Windows.
        pub fn spawned(&mut self) {}

        /// Accept the worker's connection.
        pub async fn connect(&mut self) -> io::Result<(ReadHalf, WriteHalf)> {
            let server = self
                .server
                .lock()
                .await
                .take()
                .ok_or_else(|| io::Error::other("the pipe was already accepted"))?;
            server.connect().await?;
            let (r, w) = tokio::io::split(server);
            Ok((Box::new(r), Box::new(w)))
        }
    }
}

#[cfg(not(any(unix, windows)))]
mod imp {
    use super::*;

    /// No transport on this platform.
    pub struct Channel;

    impl Channel {
        /// Always fails: the worker needs Unix sockets or Windows named pipes.
        pub fn new(_base: Option<&Path>) -> io::Result<Channel> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the Python worker needs Unix domain sockets or Windows named pipes",
            ))
        }

        /// Unreachable.
        pub fn address(&self) -> String {
            String::new()
        }

        /// Unreachable.
        pub fn prepare(&self, _command: &mut tokio::process::Command) {}

        /// Unreachable.
        pub fn spawned(&mut self) {}

        /// Unreachable.
        pub async fn connect(&mut self) -> io::Result<(ReadHalf, WriteHalf)> {
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }
}

pub use imp::Channel;
