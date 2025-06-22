use clap::{Parser, Subcommand, ValueEnum};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BeError {
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
    InvalidBeName { name: String, reason: String },

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
enum MountMode {
    /// Mount read-write.
    #[value(name = "rw")]
    ReadWrite,
    /// Mount read-only.
    #[value(name = "ro")]
    ReadOnly,
}

trait BeRoot {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        clone_from: Option<&str>,
        properties: &[String],
    ) -> Result<(), BeError>;

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        force_no_verify: bool,
        snapshots: bool,
    ) -> Result<(), BeError>;

    fn list(
        &self,
        be_name: Option<&str>,
        all: bool,
        datasets: bool,
        snapshots: bool,
        parsable: bool,
        sort_date: bool,
        sort_name: bool,
    ) -> Result<(), BeError>;

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), BeError>;

    fn unmount(&self, target: &str, force: bool) -> Result<(), BeError>;

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), BeError>;

    fn activate(&self, be_name: &str, temporary: bool, remove_temp: bool) -> Result<(), BeError>;

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), BeError>;
}

struct NoopBeRoot {
    root: String,
}

impl NoopBeRoot {
    fn new(root_path: Option<String>) -> Self {
        match root_path {
            Some(p) => Self { root: p },
            None => Self {
                root: "zfake/ROOT".to_string(),
            },
        }
    }
}

impl BeRoot for NoopBeRoot {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        clone_from: Option<&str>,
        properties: &[String],
    ) -> Result<(), BeError> {
        // Demo error case: if BE name is "error", return an error
        if be_name == "error" {
            return Err(BeError::Conflict {
                name: be_name.to_string(),
            });
        }

        println!("Create boot environment: {}/{}", self.root, be_name);
        if let Some(desc) = description {
            println!("  - Description: {}", desc);
        }
        if let Some(clone) = clone_from {
            println!("  - Clone from: {}", clone);
        }
        if !properties.is_empty() {
            println!("  - Properties: {:?}", properties);
        }
        println!("  (This is a no-op implementation)");
        Ok(())
    }

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        force_no_verify: bool,
        snapshots: bool,
    ) -> Result<(), BeError> {
        println!("Destroy boot environment: {}/{}", self.root, target);
        if force_unmount {
            println!("  - Force unmount: true");
        }
        if force_no_verify {
            println!("  - Force no verify: true");
        }
        if snapshots {
            println!("  - Destroy snapshots: true");
        }
        println!("  (This is a no-op implementation)");
        Ok(())
    }

    fn list(
        &self,
        be_name: Option<&str>,
        all: bool,
        datasets: bool,
        snapshots: bool,
        parsable: bool,
        sort_date: bool,
        sort_name: bool,
    ) -> Result<(), BeError> {
        println!("List command called");
        if let Some(name) = be_name {
            println!("  - BE name: {}", name);
        }
        if all {
            println!("  - Show all: true");
        }
        if datasets {
            println!("  - Show datasets: true");
        }
        if snapshots {
            println!("  - Show snapshots: true");
        }
        if parsable {
            println!("  - Parsable output: true");
        }
        if sort_date {
            println!("  - Sort by date: true");
        }
        if sort_name {
            println!("  - Sort by name: true");
        }
        println!("  (This is a no-op implementation)");
        Ok(())
    }

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), BeError> {
        println!(
            "Mount command called with BE: {}/{} at {}:{}",
            self.root,
            be_name,
            mountpoint,
            if mode == MountMode::ReadWrite {
                "rw"
            } else {
                "ro"
            }
        );
        println!("  (This is a no-op implementation)");
        Ok(())
    }

    fn unmount(&self, target: &str, force: bool) -> Result<(), BeError> {
        println!("Unmount boot environment: {}/{}", self.root, target);
        if force {
            println!("  - Force: true");
        }
        println!("  (This is a no-op implementation)");
        Ok(())
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), BeError> {
        println!(
            "Rename boot environment: {}/{} -> {}/{}",
            self.root, be_name, self.root, new_name
        );
        println!("  (This is a no-op implementation)");
        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool, remove_temp: bool) -> Result<(), BeError> {
        println!("Activate boot environment: {}/{}", self.root, be_name);
        if temporary {
            println!("  - Temporary: true");
        }
        if remove_temp {
            println!("  - Remove temp: true");
        }
        println!("  (This is a no-op implementation)");
        Ok(())
    }

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), BeError> {
        println!(
            "Rollback boot environemtn: {}/{} to snapshot {}",
            self.root, be_name, snapshot
        );
        println!("  (This is a no-op implementation)");
        Ok(())
    }
}

#[derive(Parser)]
#[command(name = "beadm")]
#[command(about = "Boot Environment Administration")]
#[command(version)]
struct Cli {
    /// Set the boot environment root
    ///
    /// The boot environment root is a dataset whose children are all boot
    /// environments. Defaults to the parent dataset of the active boot
    /// environment.
    #[arg(short = 'r', long = "root", global = true, group = "Global options")]
    beroot: Option<String>,

    /// Verbose output
    #[arg(short = 'v', long = "verbose", global = true, group = "Global options")]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new boot environment
    Create {
        /// Boot environment name
        be_name: String,
        /// Activate the new boot environment
        #[arg(short = 'a')]
        activate: bool,
        /// Temporarily activate the new boot environment
        #[arg(short = 't')]
        temp_activate: bool,
        /// Description for the boot environment
        #[arg(short = 'd', long)]
        description: Option<String>,
        /// Clone from existing BE or snapshot
        #[arg(short = 'e', long)]
        clone_from: Option<String>,
        /// Set ZFS properties (property=value)
        #[arg(short = 'o', long)]
        property: Vec<String>,
    },
    /// Destroy a boot environment
    Destroy {
        /// Boot environment name or snapshot (beName@snapshot)
        target: String,
        /// Forcefully unmount if needed
        #[arg(short = 'f')]
        force_unmount: bool,
        /// Force without verification
        #[arg(short = 'F')]
        force_no_verify: bool,
        /// Destroy all snapshots
        #[arg(short = 's')]
        snapshots: bool,
    },
    /// List boot environments
    List {
        /// Boot environment name (optional)
        be_name: Option<String>,
        /// List all information
        #[arg(short = 'a')]
        all: bool,
        /// List subordinate filesystems
        #[arg(short = 'd')]
        datasets: bool,
        /// List snapshots
        #[arg(short = 's')]
        snapshots: bool,
        /// Machine-parsable output
        #[arg(short = 'H')]
        parsable: bool,
        /// Sort by date
        #[arg(short = 'k')]
        sort_date: bool,
        /// Sort by name
        #[arg(short = 'K')]
        sort_name: bool,
    },
    /// Mount a boot environment
    Mount {
        /// Boot environment name
        be_name: String,
        /// Mount point
        mountpoint: String,
        /// Set read/write mode (ro or rw)
        #[arg(short = 's', long, default_value = "rw")]
        mode: MountMode,
    },
    /// Unmount a boot environment
    Unmount {
        /// Boot environment name or mount point
        target: String,
        /// Force unmount
        #[arg(short = 'f')]
        force: bool,
    },
    /// Rename a boot environment
    Rename {
        /// Current boot environment name
        be_name: String,
        /// New boot environment name
        new_name: String,
    },
    /// Activate a boot environment
    Activate {
        /// Boot environment name
        be_name: String,
        /// Temporary activation
        #[arg(short = 't')]
        temporary: bool,
        /// Remove temporary activation
        #[arg(short = 'T')]
        remove_temp: bool,
    },
    /// Rollback to a snapshot
    Rollback {
        /// Boot environment name
        be_name: String,
        /// Snapshot name
        snapshot: String,
    },
}

fn main() {
    let cli = Cli::parse();
    let beroot = NoopBeRoot::new(cli.beroot.clone());

    if cli.verbose {
        println!("Verbose mode enabled");
    }

    let result = match &cli.command {
        Some(Commands::Create {
            be_name,
            activate,
            temp_activate,
            description,
            clone_from,
            property,
        }) => {
            let result = beroot.create(
                be_name,
                description.as_deref(),
                clone_from.as_deref(),
                property,
            );

            if result.is_ok() && (*activate || *temp_activate) {
                beroot.activate(be_name, *temp_activate, false)
            } else {
                result
            }
        }
        Some(Commands::Destroy {
            target,
            force_unmount,
            force_no_verify,
            snapshots,
        }) => beroot.destroy(target, *force_unmount, *force_no_verify, *snapshots),
        Some(Commands::List {
            be_name,
            all,
            datasets,
            snapshots,
            parsable,
            sort_date,
            sort_name,
        }) => beroot.list(
            be_name.as_deref(),
            *all,
            *datasets,
            *snapshots,
            *parsable,
            *sort_date,
            *sort_name,
        ),
        Some(Commands::Mount {
            be_name,
            mountpoint,
            mode,
        }) => beroot.mount(be_name, mountpoint, *mode),
        Some(Commands::Unmount { target, force }) => beroot.unmount(target, *force),
        Some(Commands::Rename { be_name, new_name }) => beroot.rename(be_name, new_name),
        Some(Commands::Activate {
            be_name,
            temporary,
            remove_temp,
        }) => beroot.activate(be_name, *temporary, *remove_temp),
        Some(Commands::Rollback { be_name, snapshot }) => beroot.rollback(be_name, snapshot),
        None => beroot.list(None, false, false, false, false, false, false),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert();
}
