use clap::ValueEnum;
use std::path::PathBuf;
use thiserror::Error as ThisError;

pub mod mock;
pub mod validation;

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

    #[error("Cannot unmount boot environment '{name}': {reason}")]
    UnmountFailed { name: String, reason: String },

    #[error("Invalid boot environment name '{name}': {reason}")]
    InvalidName { name: String, reason: String },

    #[error("Boot environment name '{name}' is currently mounted at '{mountpoint}'")]
    BeMounted { name: String, mountpoint: String },

    #[error("ZFS operation failed: {message}")]
    ZfsError { message: String },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Operation not supported in no-op mode")]
    NoOpError,
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

#[derive(Clone)]
pub struct BootEnvironment {
    /// The name of this boot environment.
    pub name: String,
    /// The dataset backing this boot environment.
    #[allow(dead_code)]
    pub dataset: String,
    /// A description for this boot environment, if any.
    pub description: Option<String>,
    /// If the boot environment is currently mounted, this is its mountpoint.
    pub mountpoint: Option<PathBuf>,
    /// Whether the system is currently booted into this boot environment.
    pub active: bool,
    /// Whether the system will reboot into this environment.
    pub next_boot: bool,
    /// Bytes on the filesystem associated with this boot environment.
    pub space: u64,
    /// Unix timestamp for when this boot environment was created.
    pub created: i64,
}

pub trait Client {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&str>,
        properties: &[String],
    ) -> Result<(), Error>;

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        force_no_verify: bool,
        snapshots: bool,
    ) -> Result<(), Error>;

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), Error>;

    fn unmount(&self, target: &str, force: bool) -> Result<(), Error>;

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error>;

    fn activate(&self, be_name: &str, temporary: bool, remove_temp: bool) -> Result<(), Error>;

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), Error>;

    /// Get a snapshot of the boot environments.
    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error>;
}
