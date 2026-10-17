//! The host side of the worker transport: a Unix domain socket in a private (`0700`) directory
//! on Unix, a named pipe on Windows. The host listens; the worker connects to the address passed
//! as `--socket`.

use std::io;
use std::path::Path;

use tokio::io::{AsyncRead, AsyncWrite};

/// The read half of an accepted connection.
pub type ReadHalf = Box<dyn AsyncRead + Send + Unpin>;
/// The write half of an accepted connection.
pub type WriteHalf = Box<dyn AsyncWrite + Send + Unpin>;

#[cfg_attr(not(any(unix, windows)), allow(dead_code))]
fn unique_name() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("mrsm-w-{}-{}", std::process::id(), &id[..12])
}

#[cfg(unix)]
mod imp {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    /// A listening Unix socket. The socket's directory is removed on drop.
    pub struct Listener {
        inner: tokio::net::UnixListener,
        dir: PathBuf,
        path: PathBuf,
    }

    impl Listener {
        /// Bind a new socket under `base` (default: the system temporary directory).
        pub fn bind(base: Option<&Path>) -> io::Result<Listener> {
            let base = base
                .map(Path::to_path_buf)
                .unwrap_or_else(std::env::temp_dir);
            let dir = base.join(unique_name());
            std::fs::create_dir_all(&dir)?;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
            let path = dir.join("s.sock");
            let inner = tokio::net::UnixListener::bind(&path)?;
            Ok(Listener { inner, dir, path })
        }

        /// The address to pass to the worker.
        pub fn address(&self) -> String {
            self.path.to_string_lossy().into_owned()
        }

        /// Accept the worker's connection.
        pub async fn accept(&self) -> io::Result<(ReadHalf, WriteHalf)> {
            let (stream, _) = self.inner.accept().await?;
            let (r, w) = stream.into_split();
            Ok((Box::new(r), Box::new(w)))
        }
    }

    impl Drop for Listener {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_dir(&self.dir);
        }
    }
}

// Unix first; the Windows named-pipe transport compiles but is not exercised by CI yet
// (the Python side opens the pipe as a file).
#[cfg(windows)]
mod imp {
    use super::*;
    use tokio::net::windows::named_pipe::ServerOptions;

    /// A named pipe server waiting for the worker.
    pub struct Listener {
        name: String,
        server: tokio::sync::Mutex<Option<tokio::net::windows::named_pipe::NamedPipeServer>>,
    }

    impl Listener {
        /// Create a new named pipe (`base` is ignored on Windows).
        pub fn bind(_base: Option<&Path>) -> io::Result<Listener> {
            let name = format!(r"\\.\pipe\{}", unique_name());
            let server = ServerOptions::new()
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .create(&name)?;
            Ok(Listener {
                name,
                server: tokio::sync::Mutex::new(Some(server)),
            })
        }

        /// The address to pass to the worker.
        pub fn address(&self) -> String {
            self.name.clone()
        }

        /// Accept the worker's connection.
        pub async fn accept(&self) -> io::Result<(ReadHalf, WriteHalf)> {
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
    pub struct Listener;

    impl Listener {
        /// Always fails: the worker needs Unix sockets or Windows named pipes.
        pub fn bind(_base: Option<&Path>) -> io::Result<Listener> {
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
        pub async fn accept(&self) -> io::Result<(ReadHalf, WriteHalf)> {
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }
}

pub use imp::Listener;
