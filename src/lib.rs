//! Eventglass's application contracts and embedded server implementation.

pub mod alerts;
pub mod app;
pub mod auth;
pub mod config;
pub mod db;
pub mod http;
pub mod indexer;
pub mod model;
pub mod search;
pub mod sentry;
pub mod storage;

/// Version of the running package, also exposed by the CLI.
///
/// ```
/// assert!(!eventglass::VERSION.is_empty());
/// ```
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_a_nonempty_release_identifier() {
        assert_eq!(super::VERSION.split('.').count(), 3);
    }
}
