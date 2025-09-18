// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::ValueEnum;
use std::path::{Path, PathBuf};
use thiserror::Error as ThisError;
#[cfg(feature = "dbus")]
use zvariant::{DeserializeDict, SerializeDict, Type};

pub mod mock;
pub mod validation;
pub mod zfs;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Boot environment '{name}' not found")]
    NotFound { name: String },

    #[error("Boot environment '{name}' already exists")]
    Conflict { name: String },

    #[error("Mount point '{path}' is already in use")]
    MountPointInUse { path: String },

    #[error("Cannot destroy active boot environment '{name}'")]
    CannotDestroyActive { name: String },

    #[error("Boot environment '{name}' has snapshots and cannot be destroyed")]
    HasSnapshots { name: String },

    #[error("Invalid boot environment name '{name}': {reason}")]
    InvalidName { name: String, reason: String },

    #[error("Invalid path: '{path}'")]
    InvalidPath { path: String },

    #[error("Boot environment name '{name}' is currently mounted at '{mountpoint}'")]
    Mounted { name: String, mountpoint: String },

    #[error("Boot environment '{name}' must be mounted to access its contents")]
    NotMounted { name: String },

    #[error("Invalid property '{name}={value}'")]
    InvalidProp { name: String, value: String },

    #[error("The root filesystem is not a ZFS boot environment")]
    NoActiveBootEnvironment,

    #[error("Invalid boot environment root: '{name}'")]
    InvalidBootEnvironmentRoot { name: String },

    #[error(transparent)]
    LibzfsError(#[from] zfs::LibzfsError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "dbus")]
    #[error("D-Bus error: {0}")]
    ZbusError(#[from] zbus::Error),
}

#[cfg(feature = "dbus")]
impl From<Error> for zbus::fdo::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::NotFound { .. } => zbus::fdo::Error::UnknownObject(err.to_string()),
            Error::InvalidName { .. } => zbus::fdo::Error::InvalidArgs(err.to_string()),
            Error::InvalidPath { .. } => zbus::fdo::Error::InvalidArgs(err.to_string()),
            Error::InvalidProp { .. } => zbus::fdo::Error::InvalidArgs(err.to_string()),
            Error::NoActiveBootEnvironment => zbus::fdo::Error::Failed(err.to_string()),
            Error::InvalidBootEnvironmentRoot { .. } => {
                zbus::fdo::Error::InvalidArgs(err.to_string())
            }
            Error::ZbusError(ref e) => match e {
                zbus::Error::FDO(fdo_err) => *fdo_err.clone(),
                _ => zbus::fdo::Error::Failed(err.to_string()),
            },
            _ => zbus::fdo::Error::Failed(err.to_string()),
        }
    }
}

#[cfg(feature = "dbus")]
impl From<Error> for zbus::Error {
    fn from(err: Error) -> Self {
        zbus::Error::Failure(err.to_string())
    }
}

impl Error {
    pub fn not_found(be_name: &str) -> Self {
        Error::NotFound {
            name: be_name.to_string(),
        }
    }

    pub fn conflict(be_name: &str) -> Self {
        Error::Conflict {
            name: be_name.to_string(),
        }
    }

    pub fn mounted(name: &str, mountpoint: &Path) -> Self {
        Error::Mounted {
            name: name.to_string(),
            mountpoint: mountpoint.display().to_string(),
        }
    }

    pub fn invalid_prop(name: &str, value: &str) -> Self {
        Error::InvalidProp {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    pub fn not_mounted(name: &str) -> Self {
        Error::NotMounted {
            name: name.to_string(),
        }
    }

    pub fn invalid_root(name: &str) -> Self {
        Error::InvalidBootEnvironmentRoot {
            name: name.to_string(),
        }
    }

    pub fn has_snapshots(name: &str) -> Self {
        Error::HasSnapshots {
            name: name.to_string(),
        }
    }
}

/// Whether a boot environment is mounted read-write (the default) or
/// read-only.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum MountMode {
    /// Mount read-write.
    #[value(name = "rw")]
    ReadWrite,
    /// Mount read-only.
    #[value(name = "ro")]
    ReadOnly,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "dbus", derive(SerializeDict, DeserializeDict, Type))]
#[cfg_attr(
    feature = "dbus",
    zvariant(signature = "a{sv}", rename_all = "PascalCase")
)]
pub struct BootEnvironment {
    /// The name of this boot environment.
    pub name: String,
    /// The ZFS dataset path (e.g., `zroot/ROOT/default`).
    #[allow(dead_code)]
    pub path: String,
    /// The ZFS dataset GUID.
    pub guid: u64,
    /// A description for this boot environment, if any.
    pub description: Option<String>,
    /// If the boot environment is currently mounted, this is its mountpoint.
    pub mountpoint: Option<PathBuf>,
    /// Whether the system is currently booted into this boot environment.
    pub active: bool,
    /// Whether the system will reboot into this environment.
    pub next_boot: bool,
    /// Whether the system will reboot into this environment temporarily.
    pub boot_once: bool,
    /// Bytes on the filesystem associated with this boot environment.
    pub space: u64,
    /// Unix timestamp for when this boot environment was created.
    pub created: i64,
}

#[derive(Clone)]
pub struct Snapshot {
    /// The name of this snapshot (e.g., `default@snapshot`).
    pub name: String,
    /// The ZFS snapshot path (e.g., `zroot/ROOT/default@snapshot`).
    #[allow(dead_code)]
    pub path: String,
    /// Optional description for this snapshot.
    pub description: Option<String>,
    /// Bytes used by this snapshot.
    pub space: u64,
    /// Unix timestamp for when this snapshot was created.
    pub created: i64,
}

/// Represents either a named boot environment or a snapshot of one. Used for
/// operations that are valid for either.
#[derive(Debug, Clone)]
pub enum Label {
    /// A named boot environment.
    Name(String),
    /// A snapshot of a named boot environment.
    Snapshot(String, String),
}

impl std::str::FromStr for Label {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((name, snapshot)) = s.split_once('@') {
            if name.is_empty() {
                return Err(Error::InvalidName {
                    name: s.to_string(),
                    reason: "boot environment name cannot be empty".to_string(),
                });
            }
            if snapshot.is_empty() {
                return Err(Error::InvalidName {
                    name: s.to_string(),
                    reason: "snapshot name cannot be empty".to_string(),
                });
            }
            if snapshot.contains("@") {
                return Err(Error::InvalidName {
                    name: s.to_string(),
                    reason: "too many '@' characters".to_string(),
                });
            }
            Ok(Label::Snapshot(name.to_string(), snapshot.to_string()))
        } else {
            if s.is_empty() {
                return Err(Error::InvalidName {
                    name: s.to_string(),
                    reason: "boot environment name cannot be empty".to_string(),
                });
            }
            Ok(Label::Name(s.to_string()))
        }
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Label::Name(name) => write!(f, "{}", name),
            Label::Snapshot(name, snapshot) => write!(f, "{}@{}", name, snapshot),
        }
    }
}

pub trait Client: Send + Sync {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&Label>,
        properties: &[String],
    ) -> Result<(), Error>;

    fn create_empty(
        &self,
        be_name: &str,
        description: Option<&str>,
        host_id: Option<&str>,
        properties: &[String],
    ) -> Result<(), Error>;

    fn destroy(&self, target: &Label, force_unmount: bool, snapshots: bool) -> Result<(), Error>;

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), Error>;

    fn unmount(&self, be_name: &str, force: bool) -> Result<Option<PathBuf>, Error>;

    fn hostid(&self, be_name: &str) -> Result<Option<u32>, Error>;

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error>;

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), Error>;

    /// Clear temporary boot environment activation.
    fn clear_boot_once(&self) -> Result<(), Error>;

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), Error>;

    /// Get a snapshot of the boot environments.
    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error>;

    /// Get snapshots for a specific boot environment.
    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, Error>;

    /// Create a snapshot of a source boot environment. When `source` is None,
    /// snapshot the active boot environment.
    ///
    /// Returns the final snapshot name (e.g. `be@snapshot`).
    fn snapshot(&self, source: Option<&Label>, description: Option<&str>) -> Result<String, Error>;

    /// Create the ZFS dataset layout for boot environments. It is not an error
    /// if the required datasets already exist.
    fn init(&self, pool: &str) -> Result<(), Error>;

    /// Set the description for an existing boot environment or snapshot.
    fn describe(&self, target: &Label, description: &str) -> Result<(), Error>;
}

/// Generate a snapshot name based on the current time.
///
/// This is similar to the behaviour of FreeBSD's `bectl create` command.
pub(crate) fn generate_snapshot_name() -> String {
    // Currently an RFC 3339-style timestamp.
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
