//! The harness's error type.

use std::fmt;

/// What can go wrong talking to a target or reading a corpus.
#[derive(Debug)]
pub enum Error {
    /// The request never got an HTTP answer (connection refused, reset, …).
    Transport {
        /// The URL called.
        url: String,
        /// What the HTTP client said.
        message: String,
    },
    /// The gateway answered with an error status. `errors` is the body's
    /// `errors` array (Flink puts the server-side exception in it).
    Api {
        /// The URL called.
        url: String,
        /// The HTTP status.
        status: u16,
        /// The `errors` array, or the raw body when there is none.
        errors: Vec<String>,
    },
    /// A 2xx body that is not what the API says it is.
    Decode {
        /// The URL called.
        url: String,
        /// What did not decode.
        message: String,
    },
    /// The call needs a newer API version than the client speaks.
    Unsupported(String),
    /// A bad argument (a URL, a handle, a case file).
    Invalid(String),
    /// Waited too long.
    Timeout(String),
    /// The target cannot run the case (a required artefact is missing); not a failure.
    Skipped(String),
    /// A local file could not be read or written.
    Io {
        /// The path.
        path: String,
        /// The OS error.
        message: String,
    },
}

impl Error {
    /// The first line of the server-side exception's root cause, when the
    /// error carries one (Flink's `Caused by:` chain), else the message.
    pub fn summary(&self) -> String {
        match self {
            Error::Api { errors, status, .. } => {
                let text = errors.join("\n");
                let root = text
                    .lines()
                    .filter_map(|l| l.trim().strip_prefix("Caused by: "))
                    .next_back()
                    .map(str::to_owned);
                root.unwrap_or_else(|| {
                    format!(
                        "HTTP {status}: {}",
                        errors.first().map(String::as_str).unwrap_or("")
                    )
                })
            }
            other => other.to_string(),
        }
    }

    pub(crate) fn io(path: impl fmt::Display, err: impl fmt::Display) -> Self {
        Error::Io {
            path: path.to_string(),
            message: err.to_string(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Transport { url, message } => write!(f, "{url}: {message}"),
            Error::Api {
                url,
                status,
                errors,
            } => write!(
                f,
                "{url}: HTTP {status}: {}",
                errors.first().map(String::as_str).unwrap_or("")
            ),
            Error::Decode { url, message } => write!(f, "{url}: cannot decode: {message}"),
            Error::Unsupported(m) | Error::Invalid(m) | Error::Timeout(m) => f.write_str(m),
            Error::Skipped(m) => write!(f, "skipped: {m}"),
            Error::Io { path, message } => write!(f, "{path}: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// The harness's result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
