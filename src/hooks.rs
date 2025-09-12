// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};

use crate::be::Client;

pub fn execute_apt_hook<T: Client>(client: &T) -> Result<()> {
    for msg in apthooks::socket()? {
        match msg? {
            apthooks::HookMessage::InstallStatistics(params) => {
                if params.packages.is_empty() {
                    return Ok(());
                }

                // Create a description for the snapshot from the APT command
                // invocation.
                let mut description = String::from("before apt");
                if let Some(cmd) = &params.command {
                    description.push(' ');
                    description.push_str(cmd);
                }
                if !params.search_terms.is_empty() {
                    description.push(' ');
                    description.push_str(&params.search_terms.join(" "));
                }

                eprint!("Backing up system prior to changes... ");

                let snapshot = client
                    .snapshot(None, Some(&description))
                    .context("Failed to create boot environment snapshot")?;

                eprintln!("done. name={:?} desc={:?}", snapshot, description);
            }
            apthooks::HookMessage::InstallPost(_) => {
                if let Some(snapshot) = find_newest_apt_snapshot(client)? {
                    eprintln!(
                        "Boot into your system prior to these changes as \x1b]8;;be://{}\x1b\\{}\x1b]8;;\x1b\\.",
                        snapshot, snapshot
                    );
                } else {
                    eprintln!("Could not determine latest snapshot.");
                }
            }
            apthooks::HookMessage::InstallFail(_) => {
                if let Some(snapshot) = find_newest_apt_snapshot(client)? {
                    // In a real implementation, we could destroy the
                    // uncommitted snapshot. For now, just inform the user.
                    eprintln!(
                        "Installation failed. Snapshot available for rollback: {}",
                        snapshot
                    );
                } else {
                    eprintln!("Could not determine latest snapshot.");
                }
            }
        }
    }
    Ok(())
}

/// Find the most recent snapshot of the active boot environment created by
/// this APT hook.
fn find_newest_apt_snapshot<T: Client>(client: &T) -> Result<Option<String>> {
    let boot_envs = client
        .get_boot_environments()
        .context("Failed to determine active boot environment")?;

    let active_be = boot_envs.iter().find(|be| be.active);
    let active_be = match active_be {
        Some(be) => be,
        None => return Ok(None), // No active boot environment found
    };

    let snapshots = client
        .get_snapshots(&active_be.name)
        .context("Failed to list snapshots for the active boot environment")?;

    // Find the newest snapshot with "before apt" in the description.
    let mut most_recent: Option<(String, i64)> = None;
    for snapshot in snapshots {
        if let Some(desc) = &snapshot.description {
            if desc.starts_with("before apt") {
                match most_recent {
                    None => most_recent = Some((snapshot.name.clone(), snapshot.created)),
                    Some((_, created)) if snapshot.created > created => {
                        most_recent = Some((snapshot.name.clone(), snapshot.created));
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(most_recent.map(|(name, _)| name))
}

/// Internal module for handling APT's JSON RPC hook protocol, version 0.2.
///
/// See: https://salsa.debian.org/apt-team/apt/-/raw/main/doc/json-hooks-protocol.md
mod apthooks {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::io::{FromRawFd, RawFd};
    use std::os::unix::net::UnixStream;
    use std::{env, fmt, io};

    use serde::Deserialize;
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum Error {
        #[error("APT_HOOK_SOCKET environment variable not set")]
        NoSocket,

        #[error("APT_HOOK_SOCKET is not a valid file descriptor: {0}")]
        InvalidSocket(#[from] std::num::ParseIntError),

        #[error("Failed to open socket connection: {0}")]
        SocketError(#[from] io::Error),

        #[error("Unexpected EOF in RPC stream")]
        UnexpectedEof,

        #[error("Unexpected method in RPC stream: {0}")]
        UnexpectedMethod(String),

        #[error("Protocol error: {0}")]
        Protocol(String),

        #[error("JSON parsing error: {0}")]
        Json(#[from] serde_json::Error),
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    pub enum HookMethod {
        #[serde(rename = "org.debian.apt.hooks.hello")]
        Hello,
        #[serde(rename = "org.debian.apt.hooks.bye")]
        Bye,
        #[serde(rename = "org.debian.apt.hooks.install.pre-prompt")]
        InstallPrePrompt,
        #[serde(rename = "org.debian.apt.hooks.install.package-list")]
        InstallPackageList,
        #[serde(rename = "org.debian.apt.hooks.install.statistics")]
        InstallStatistics,
        #[serde(rename = "org.debian.apt.hooks.install.post")]
        InstallPost,
        #[serde(rename = "org.debian.apt.hooks.install.fail")]
        InstallFail,
        #[serde(rename = "org.debian.apt.hooks.search.pre")]
        SearchPre,
        #[serde(rename = "org.debian.apt.hooks.search.post")]
        SearchPost,
        #[serde(rename = "org.debian.apt.hooks.search.fail")]
        SearchFail,
        /// Catch-all variant to maintain forward compatibility.
        #[serde(other)]
        Unknown,
    }

    impl fmt::Display for HookMethod {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let s = match self {
                HookMethod::Hello => "org.debian.apt.hooks.hello",
                HookMethod::Bye => "org.debian.apt.hooks.bye",
                HookMethod::InstallPrePrompt => "org.debian.apt.hooks.install.pre-prompt",
                HookMethod::InstallPackageList => "org.debian.apt.hooks.install.package-list",
                HookMethod::InstallStatistics => "org.debian.apt.hooks.install.statistics",
                HookMethod::InstallPost => "org.debian.apt.hooks.install.post",
                HookMethod::InstallFail => "org.debian.apt.hooks.install.fail",
                HookMethod::SearchPre => "org.debian.apt.hooks.search.pre",
                HookMethod::SearchPost => "org.debian.apt.hooks.search.post",
                HookMethod::SearchFail => "org.debian.apt.hooks.search.fail",
                HookMethod::Unknown => "<unknown>",
            };
            write!(f, "{}", s)
        }
    }

    #[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
    pub struct PackageVersions {
        pub candidate: Option<PackageVersion>,
        pub install: Option<PackageVersion>,
        pub remove: Option<PackageVersion>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
    pub struct PackageVersion {
        pub id: u32,
        pub version: String,
        pub architecture: String,
        pub pin: Option<u32>,
        pub origin: Option<String>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
    pub struct Package {
        pub name: String,
        pub architecture: Option<String>,
        pub mode: String,
        pub automatic: Option<bool>,
        pub versions: Option<PackageVersions>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq, Default, Clone)]
    pub struct RpcParams {
        #[serde(rename = "command")]
        pub command: Option<String>,

        #[serde(rename = "search-terms", default)]
        pub search_terms: Vec<String>,

        #[serde(rename = "unknown-packages", default)]
        pub unknown_packages: Vec<String>,

        #[serde(default)]
        pub packages: Vec<Package>,

        // Hello handshake parameters
        #[serde(default)]
        pub versions: Vec<String>,

        #[serde(default)]
        pub options: Vec<serde_json::Value>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    pub struct RpcRequest {
        pub method: HookMethod,
        #[serde(default)]
        pub params: Option<RpcParams>,
        #[serde(default)]
        pub id: Option<serde_json::Value>,
    }

    #[derive(Debug, PartialEq, Eq)]
    pub enum HookMessage {
        InstallStatistics(RpcParams),
        InstallPost(RpcParams),
        InstallFail(RpcParams),
    }

    pub struct RpcStream<R: BufRead, W: Write> {
        reader: R,
        writer: W,
        saw_hello: bool,
        saw_bye: bool,
    }

    impl<R: BufRead, W: Write> RpcStream<R, W> {
        fn from(reader: R, writer: W) -> Self {
            RpcStream {
                reader,
                writer,
                saw_hello: false,
                saw_bye: false,
            }
        }

        fn read_request(&mut self) -> Result<RpcRequest, Error> {
            let mut line = String::new();
            self.reader.read_line(&mut line)?;

            if line.is_empty() {
                return Err(Error::UnexpectedEof);
            }

            let req: RpcRequest = serde_json::from_str(&line)?;

            // Read the trailing newline
            let mut empty_line = String::new();
            self.reader.read_line(&mut empty_line)?;
            if !empty_line.trim().is_empty() {
                return Err(Error::Protocol(format!(
                    "Expected empty line, got: {}",
                    empty_line
                )));
            }

            Ok(req)
        }

        fn send_hello_response(&mut self) -> Result<(), Error> {
            const HELLO_RESPONSE: &str = r#"{"jsonrpc":"2.0","id":0,"result":{"version":"0.2"}}"#;
            write!(self.writer, "{}\n\n", HELLO_RESPONSE)?;
            Ok(())
        }

        fn handle_hello_handshake(&mut self) -> Result<(), Error> {
            let req = self.read_request()?;
            if req.method != HookMethod::Hello {
                return Err(Error::UnexpectedMethod(req.method.to_string()));
            }

            self.send_hello_response()?;
            self.saw_hello = true;
            Ok(())
        }
    }

    impl<R: BufRead, W: Write> Iterator for RpcStream<R, W> {
        type Item = Result<HookMessage, Error>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.saw_bye {
                return None;
            }

            // Handle hello handshake on first call
            if !self.saw_hello {
                if let Err(e) = self.handle_hello_handshake() {
                    self.saw_bye = true;
                    return Some(Err(e));
                }
            }

            // Read the next request
            let request = match self.read_request() {
                Ok(req) => req,
                Err(e) => {
                    self.saw_bye = true;
                    return Some(Err(e));
                }
            };

            // Handle different message types
            match request.method {
                HookMethod::Bye => {
                    self.saw_bye = true;
                    None // Don't yield bye messages
                }
                HookMethod::InstallStatistics => {
                    let params = request.params.unwrap_or_default();
                    Some(Ok(HookMessage::InstallStatistics(params)))
                }
                HookMethod::InstallPost => {
                    let params = request.params.unwrap_or_default();
                    Some(Ok(HookMessage::InstallPost(params)))
                }
                HookMethod::InstallFail => {
                    let params = request.params.unwrap_or_default();
                    Some(Ok(HookMessage::InstallFail(params)))
                }
                HookMethod::Hello => {
                    // This shouldn't happen after handshake
                    self.saw_bye = true;
                    Some(Err(Error::Protocol(
                        "Unexpected hello message after handshake".to_string(),
                    )))
                }
                // TODO: Support these messages.
                HookMethod::InstallPrePrompt
                | HookMethod::InstallPackageList
                | HookMethod::SearchPre
                | HookMethod::SearchPost
                | HookMethod::SearchFail => self.next(),
                // Ignore methods added in future revisions.
                HookMethod::Unknown => self.next(),
            }
        }
    }

    /// Connect to the APT hook socket and return an iterator over RPC messages.
    pub fn socket() -> Result<RpcStream<BufReader<UnixStream>, UnixStream>, Error> {
        let socket_env = env::var("APT_HOOK_SOCKET").map_err(|_| Error::NoSocket)?;
        let fd: RawFd = socket_env.parse()?;

        // Safety: We're taking ownership of the file descriptor from APT
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        let reader = BufReader::new(stream.try_clone()?);

        Ok(RpcStream::from(reader, stream))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io;

        #[test]
        fn test_no_apt_socket() {
            // Test that the socket() function properly fails when
            // APT_HOOK_SOCKET is not set.
            assert!(matches!(socket(), Err(Error::NoSocket)));
        }

        #[test]
        fn test_unknown_method_deserialization() {
            let unknown = serde_json::from_str::<HookMethod>(r#""invalid.method""#);
            assert_eq!(unknown.unwrap(), HookMethod::Unknown);
        }

        #[test]
        fn test_read_hello_request() {
            // Example from the spec: https://salsa.debian.org/apt-team/apt/-/raw/main/doc/json-hooks-protocol.md
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","id":0,"params":{"versions":["0.1", "0.2"], "options": [{"name": "APT::Architecture", "value": "amd64"}]}}

"#;
            let mut conn = RpcStream::from(io::Cursor::new(input), Vec::new());
            let req = conn.read_request().unwrap();
            assert_eq!(
                req,
                RpcRequest {
                    method: HookMethod::Hello,
                    id: Some(serde_json::Value::Number(serde_json::Number::from(0))),
                    params: Some(RpcParams {
                        command: None,
                        search_terms: vec![],
                        unknown_packages: vec![],
                        packages: vec![],
                        versions: vec!["0.1".to_string(), "0.2".to_string()],
                        options: vec![
                            serde_json::json!({"name": "APT::Architecture", "value": "amd64"})
                        ],
                    })
                },
            );
        }

        #[test]
        fn test_rpc_request_parsing() {
            let json = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"install","search-terms":["vim"],"packages":[{"name":"vim","mode":"install"}]}}"#;
            let req: RpcRequest = serde_json::from_str(json).unwrap();
            assert_eq!(
                req,
                RpcRequest {
                    method: HookMethod::InstallStatistics,
                    id: None,
                    params: Some(RpcParams {
                        command: Some("install".to_string()),
                        search_terms: vec!["vim".to_string()],
                        unknown_packages: vec![],
                        packages: vec![Package {
                            name: "vim".to_string(),
                            architecture: None,
                            mode: "install".to_string(),
                            automatic: None,
                            versions: None,
                        }],
                        versions: vec![],
                        options: vec![],
                    }),
                }
            );
        }

        #[test]
        fn test_hook_message_creation() {
            let params = RpcParams::default();
            let msg = HookMessage::InstallStatistics(params.clone());

            match msg {
                HookMessage::InstallStatistics(p) => {
                    assert_eq!(p, params);
                }
                _ => panic!("Wrong message type"),
            }
        }

        #[test]
        fn test_rpc_connection_iterator_handles_hello_bye() {
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","id":0,"params":{"versions":["0.2"]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"install","packages":[{"name":"vim","mode":"install"}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.bye"}

"#;
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let conn = RpcStream::from(reader, writer);

            let messages: Result<Vec<_>, _> = conn.collect();
            let messages = messages.unwrap();

            // Should only get one message (InstallStatistics), hello and bye are handled internally
            assert_eq!(messages.len(), 1);

            match &messages[0] {
                HookMessage::InstallStatistics(params) => {
                    assert_eq!(params.command, Some("install".to_string()));
                    assert_eq!(params.packages.len(), 1);
                    assert_eq!(params.packages[0].name, "vim");
                }
                _ => panic!("Wrong message type"),
            }
        }

        #[test]
        fn test_rpc_connection_iterator_multiple_hooks() {
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","id":0,"params":{"versions":["0.2"]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"install","packages":[{"name":"vim","mode":"install"}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.post","params":{"command":"install","packages":[{"name":"vim","mode":"install"}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.bye"}

"#;
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let conn = RpcStream::from(reader, writer);

            let messages: Result<Vec<_>, _> = conn.collect();
            let messages = messages.unwrap();

            // Should get two messages (InstallStatistics and InstallPost)
            assert_eq!(messages.len(), 2);

            match &messages[0] {
                HookMessage::InstallStatistics(params) => {
                    assert_eq!(params.command, Some("install".to_string()));
                }
                _ => panic!("Wrong first message type"),
            }

            match &messages[1] {
                HookMessage::InstallPost(params) => {
                    assert_eq!(params.command, Some("install".to_string()));
                }
                _ => panic!("Wrong second message type"),
            }
        }

        #[test]
        fn test_rpc_connection_iterator_error_on_missing_hello() {
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"install"}}

"#;
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let conn = RpcStream::from(reader, writer);

            let result: Result<Vec<_>, _> = conn.collect();
            assert!(result.is_err());
        }

        #[test]
        fn test_rpc_connection_iterator_handles_install_fail() {
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","id":0,"params":{"versions":["0.2"]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.fail","params":{"command":"install","packages":[{"name":"broken","mode":"install"}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.bye"}

"#;
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let conn = RpcStream::from(reader, writer);

            let messages: Result<Vec<_>, _> = conn.collect();
            let messages = messages.unwrap();

            assert_eq!(messages.len(), 1);

            match &messages[0] {
                HookMessage::InstallFail(params) => {
                    assert_eq!(params.command, Some("install".to_string()));
                    assert_eq!(params.packages.len(), 1);
                    assert_eq!(params.packages[0].name, "broken");
                }
                _ => panic!("Wrong message type"),
            }
        }

        #[test]
        fn test_iterator_hello_message_bye_sequence() {
            // Test the complete APT hook sequence: hello -> message -> bye
            // This demonstrates that the iterator properly handles the protocol
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","id":0,"params":{"versions":["0.2"]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"upgrade","search-terms":["firefox"],"packages":[{"name":"firefox","mode":"upgrade","automatic":false}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.bye"}

"#;
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let mut stream = RpcStream::from(reader, writer);

            // First call should handle hello internally and return the first message
            let first = stream.next();
            assert!(first.is_some());

            let message = first.unwrap().unwrap();
            match message {
                HookMessage::InstallStatistics(params) => {
                    assert_eq!(params.command, Some("upgrade".to_string()));
                    assert_eq!(params.search_terms, vec!["firefox"]);
                    assert_eq!(params.packages.len(), 1);
                    assert_eq!(params.packages[0].name, "firefox");
                    assert_eq!(params.packages[0].mode, "upgrade");
                }
                _ => panic!("Expected InstallStatistics message"),
            }

            // Second call should handle bye internally and return None (end of iterator)
            let second = stream.next();
            assert!(second.is_none());

            // Subsequent calls should continue to return None
            let third = stream.next();
            assert!(third.is_none());

            // Verify the hello response was written to the writer
            let output = String::from_utf8(stream.writer).unwrap();
            assert!(output.contains(r#"{"jsonrpc":"2.0","id":0,"result":{"version":"0.2"}}"#));
        }

        #[test]
        fn test_socket_connection_send_hello_response() {
            let mut conn = RpcStream::from(io::Cursor::new(""), Vec::new());
            conn.send_hello_response().unwrap();

            let output = String::from_utf8(conn.writer).unwrap();
            assert!(output.contains(r#"{"jsonrpc":"2.0","id":0,"result":{"version":"0.2"}}"#));
            assert!(output.ends_with("\n\n"));
        }

        #[test]
        fn test_socket_connection_read_request_error_on_empty() {
            let conn = RpcStream::from(io::Cursor::new(""), Vec::new());
            let result: Result<Vec<_>, _> = conn.collect();
            assert!(matches!(result, Err(Error::UnexpectedEof)));
        }

        #[test]
        fn test_socket_connection_read_request_error_on_malformed_json() {
            let input = "not valid json\n\n";
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let conn = RpcStream::from(reader, writer);

            let result: Result<Vec<_>, _> = conn.collect();
            assert!(matches!(result, Err(Error::Json(_))));
        }

        #[test]
        fn test_hello_handshake_from_spec() {
            // Example from APT spec
            let json = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","params":{"versions":["0.1"]},"id":0}"#;

            let expected = RpcRequest {
                method: HookMethod::Hello,
                id: Some(serde_json::Value::Number(serde_json::Number::from(0))),
                params: Some(RpcParams {
                    command: None,
                    search_terms: vec![],
                    unknown_packages: vec![],
                    packages: vec![],
                    versions: vec!["0.1".to_string()],
                    options: vec![],
                }),
            };

            let req: RpcRequest = serde_json::from_str(json).unwrap();
            assert_eq!(req, expected);
        }

        #[test]
        fn test_install_statistics_with_package_details() {
            // Example based on APT spec with full package details
            let json = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"install","packages":[{"name":"hello","architecture":"amd64","mode":"install","automatic":false,"versions":{"candidate":{"id":1,"version":"2.10-2ubuntu2","architecture":"amd64","pin":500}}}]}}"#;

            let expected = RpcRequest {
                method: HookMethod::InstallStatistics,
                id: None,
                params: Some(RpcParams {
                    command: Some("install".to_string()),
                    search_terms: vec![],
                    unknown_packages: vec![],
                    packages: vec![Package {
                        name: "hello".to_string(),
                        architecture: Some("amd64".to_string()),
                        mode: "install".to_string(),
                        automatic: Some(false),
                        versions: Some(PackageVersions {
                            candidate: Some(PackageVersion {
                                id: 1,
                                version: "2.10-2ubuntu2".to_string(),
                                architecture: "amd64".to_string(),
                                pin: Some(500),
                                origin: None,
                            }),
                            install: None,
                            remove: None,
                        }),
                    }],
                    versions: vec![],
                    options: vec![],
                }),
            };

            let req: RpcRequest = serde_json::from_str(json).unwrap();
            assert_eq!(req, expected);
        }

        #[test]
        fn test_install_post_notification() {
            let json = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.post","params":{"command":"install","packages":[{"name":"hello","mode":"install"}]}}"#;

            let expected = RpcRequest {
                method: HookMethod::InstallPost,
                id: None,
                params: Some(RpcParams {
                    command: Some("install".to_string()),
                    search_terms: vec![],
                    unknown_packages: vec![],
                    packages: vec![Package {
                        name: "hello".to_string(),
                        architecture: None,
                        mode: "install".to_string(),
                        automatic: None,
                        versions: None,
                    }],
                    versions: vec![],
                    options: vec![],
                }),
            };

            let req: RpcRequest = serde_json::from_str(json).unwrap();
            assert_eq!(req, expected);
        }

        #[test]
        fn test_install_fail_notification() {
            let json = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.fail","params":{"command":"install","packages":[{"name":"broken-package","mode":"install"}]}}"#;

            let expected = RpcRequest {
                method: HookMethod::InstallFail,
                id: None,
                params: Some(RpcParams {
                    command: Some("install".to_string()),
                    search_terms: vec![],
                    unknown_packages: vec![],
                    packages: vec![Package {
                        name: "broken-package".to_string(),
                        architecture: None,
                        mode: "install".to_string(),
                        automatic: None,
                        versions: None,
                    }],
                    versions: vec![],
                    options: vec![],
                }),
            };

            let req: RpcRequest = serde_json::from_str(json).unwrap();
            assert_eq!(req, expected);
        }

        #[test]
        fn test_bye_notification() {
            let json = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.bye"}"#;
            let req: RpcRequest = serde_json::from_str(json).unwrap();
            assert_eq!(
                req,
                RpcRequest {
                    method: HookMethod::Bye,
                    id: None,
                    params: None,
                }
            );
        }

        #[test]
        fn test_new_hook_methods_parsing() {
            // Test that all new hook methods from the spec can be parsed correctly
            let test_cases = vec![
                (
                    "org.debian.apt.hooks.install.pre-prompt",
                    HookMethod::InstallPrePrompt,
                ),
                (
                    "org.debian.apt.hooks.install.package-list",
                    HookMethod::InstallPackageList,
                ),
                ("org.debian.apt.hooks.search.pre", HookMethod::SearchPre),
                ("org.debian.apt.hooks.search.post", HookMethod::SearchPost),
                ("org.debian.apt.hooks.search.fail", HookMethod::SearchFail),
            ];

            for (method_name, expected) in test_cases {
                let json = format!(r#"{{"jsonrpc":"2.0","method":"{}"}}"#, method_name);
                let req: RpcRequest = serde_json::from_str(&json).unwrap();
                assert_eq!(
                    req.method, expected,
                    "Failed to parse method: {}",
                    method_name
                );
            }
        }

        #[test]
        fn test_rpc_connection_skips_unprocessed_hooks() {
            // Test that the iterator properly skips hooks we don't process
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","id":0,"params":{"versions":["0.2"]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.pre-prompt","params":{"command":"upgrade"}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"upgrade","packages":[{"name":"vim","mode":"upgrade"}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.package-list","params":{"packages":[{"name":"vim","mode":"upgrade"}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.post","params":{"command":"upgrade"}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.bye"}

"#;
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let conn = RpcStream::from(reader, writer);

            let messages: Result<Vec<_>, _> = conn.collect();
            let messages = messages.unwrap();

            // Should only get InstallStatistics and InstallPost (skipping pre-prompt and package-list)
            assert_eq!(messages.len(), 2);

            match &messages[0] {
                HookMessage::InstallStatistics(params) => {
                    assert_eq!(params.command, Some("upgrade".to_string()));
                    assert_eq!(params.packages.len(), 1);
                    assert_eq!(params.packages[0].name, "vim");
                }
                _ => panic!("Expected first message to be InstallStatistics"),
            }

            match &messages[1] {
                HookMessage::InstallPost(params) => {
                    assert_eq!(params.command, Some("upgrade".to_string()));
                }
                _ => panic!("Expected second message to be InstallPost"),
            }
        }

        #[test]
        fn test_unknown_hook_methods_forward_compatibility() {
            // Test that unknown/future hook methods are parsed as Unknown variant
            let future_methods = vec![
                "org.debian.apt.hooks.install.future-method",
                "org.debian.apt.hooks.new.category",
                "org.debian.apt.hooks.unknown",
                "some.completely.different.method",
            ];

            for method_name in future_methods {
                let json = format!(r#"{{"jsonrpc":"2.0","method":"{}"}}"#, method_name);
                let req: RpcRequest = serde_json::from_str(&json).unwrap();
                assert_eq!(
                    req.method,
                    HookMethod::Unknown,
                    "Method {} should parse as Unknown",
                    method_name
                );
            }
        }

        #[test]
        fn test_rpc_connection_skips_unknown_future_methods() {
            // Test that the iterator properly skips unknown future hook methods
            let input = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.hello","id":0,"params":{"versions":["0.2"]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.future-method","params":{"command":"upgrade"}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"command":"upgrade","packages":[{"name":"vim","mode":"upgrade"}]}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.some.new.category","params":{"data":"unknown"}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.post","params":{"command":"upgrade"}}

{"jsonrpc":"2.0","method":"org.debian.apt.hooks.bye"}

"#;
            let reader = io::Cursor::new(input);
            let writer = Vec::new();
            let conn = RpcStream::from(reader, writer);

            let messages: Result<Vec<_>, _> = conn.collect();
            let messages = messages.unwrap();

            // Should only get InstallStatistics and InstallPost (skipping unknown future methods)
            assert_eq!(messages.len(), 2);

            match &messages[0] {
                HookMessage::InstallStatistics(params) => {
                    assert_eq!(params.command, Some("upgrade".to_string()));
                    assert_eq!(params.packages.len(), 1);
                    assert_eq!(params.packages[0].name, "vim");
                }
                _ => panic!("Expected first message to be InstallStatistics"),
            }

            match &messages[1] {
                HookMessage::InstallPost(params) => {
                    assert_eq!(params.command, Some("upgrade".to_string()));
                }
                _ => panic!("Expected second message to be InstallPost"),
            }
        }

        #[test]
        fn test_package_with_multiple_versions() {
            let json = r#"{"jsonrpc":"2.0","method":"org.debian.apt.hooks.install.statistics","params":{"packages":[{"name":"test-pkg","mode":"install","versions":{"candidate":{"id":1,"version":"1.0","architecture":"amd64"},"install":{"id":2,"version":"1.1","architecture":"amd64"},"remove":{"id":3,"version":"0.9","architecture":"amd64"}}}]}}"#;

            let expected = RpcRequest {
                method: HookMethod::InstallStatistics,
                id: None,
                params: Some(RpcParams {
                    command: None,
                    search_terms: vec![],
                    unknown_packages: vec![],
                    packages: vec![Package {
                        name: "test-pkg".to_string(),
                        architecture: None,
                        mode: "install".to_string(),
                        automatic: None,
                        versions: Some(PackageVersions {
                            candidate: Some(PackageVersion {
                                id: 1,
                                version: "1.0".to_string(),
                                architecture: "amd64".to_string(),
                                pin: None,
                                origin: None,
                            }),
                            install: Some(PackageVersion {
                                id: 2,
                                version: "1.1".to_string(),
                                architecture: "amd64".to_string(),
                                pin: None,
                                origin: None,
                            }),
                            remove: Some(PackageVersion {
                                id: 3,
                                version: "0.9".to_string(),
                                architecture: "amd64".to_string(),
                                pin: None,
                                origin: None,
                            }),
                        }),
                    }],
                    versions: vec![],
                    options: vec![],
                }),
            };

            let req: RpcRequest = serde_json::from_str(json).unwrap();
            assert_eq!(req, expected);
        }
    }
}
