use clap::{Parser, Subcommand, ValueEnum};

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
    );

    fn destroy(&self, target: &str, force_unmount: bool, force_no_verify: bool, snapshots: bool);

    fn list(
        &self,
        be_name: Option<&str>,
        all: bool,
        datasets: bool,
        snapshots: bool,
        parsable: bool,
        sort_date: bool,
        sort_name: bool,
    );

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode);

    fn unmount(&self, target: &str, force: bool);

    fn rename(&self, be_name: &str, new_name: &str);

    fn activate(&self, be_name: &str, temporary: bool, remove_temp: bool);

    fn rollback(&self, be_name: &str, snapshot: &str);
}

struct NoopBeRoot {
    root_path: Option<String>,
}

impl NoopBeRoot {
    fn new(root_path: Option<String>) -> Self {
        Self { root_path }
    }
}

impl BeRoot for NoopBeRoot {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        clone_from: Option<&str>,
        properties: &[String],
    ) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
        println!("Create command called with BE name: {}", be_name);
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
    }

    fn destroy(&self, target: &str, force_unmount: bool, force_no_verify: bool, snapshots: bool) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
        println!("Destroy command called with target: {}", target);
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
    ) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
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
    }

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
        println!(
            "Mount command called with BE: {} at {}:{}",
            be_name,
            mountpoint,
            if mode == MountMode::ReadWrite {
                "rw"
            } else {
                "ro"
            }
        );
        println!("  (This is a no-op implementation)");
    }

    fn unmount(&self, target: &str, force: bool) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
        println!("Unmount command called with target: {}", target);
        if force {
            println!("  - Force: true");
        }
        println!("  (This is a no-op implementation)");
    }

    fn rename(&self, be_name: &str, new_name: &str) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
        println!("Rename command called: {} -> {}", be_name, new_name);
        println!("  (This is a no-op implementation)");
    }

    fn activate(&self, be_name: &str, temporary: bool, remove_temp: bool) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
        println!("Activate command called with BE: {}", be_name);
        if temporary {
            println!("  - Temporary: true");
        }
        if remove_temp {
            println!("  - Remove temp: true");
        }
        println!("  (This is a no-op implementation)");
    }

    fn rollback(&self, be_name: &str, snapshot: &str) {
        if let Some(root) = &self.root_path {
            println!("Using boot environment root: {}", root);
        }
        println!(
            "Rollback command called: {} to snapshot {}",
            be_name, snapshot
        );
        println!("  (This is a no-op implementation)");
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

    match &cli.command {
        Some(Commands::Create {
            be_name,
            activate,
            temp_activate,
            description,
            clone_from,
            property,
        }) => {
            beroot.create(
                be_name,
                description.as_deref(),
                clone_from.as_deref(),
                property,
            );

            if *activate || *temp_activate {
                beroot.activate(be_name, *temp_activate, false);
            }
        }
        Some(Commands::Destroy {
            target,
            force_unmount,
            force_no_verify,
            snapshots,
        }) => {
            beroot.destroy(target, *force_unmount, *force_no_verify, *snapshots);
        }
        Some(Commands::List {
            be_name,
            all,
            datasets,
            snapshots,
            parsable,
            sort_date,
            sort_name,
        }) => {
            beroot.list(
                be_name.as_deref(),
                *all,
                *datasets,
                *snapshots,
                *parsable,
                *sort_date,
                *sort_name,
            );
        }
        Some(Commands::Mount {
            be_name,
            mountpoint,
            mode,
        }) => {
            beroot.mount(be_name, mountpoint, *mode);
        }
        Some(Commands::Unmount { target, force }) => {
            beroot.unmount(target, *force);
        }
        Some(Commands::Rename { be_name, new_name }) => {
            beroot.rename(be_name, new_name);
        }
        Some(Commands::Activate {
            be_name,
            temporary,
            remove_temp,
        }) => {
            beroot.activate(be_name, *temporary, *remove_temp);
        }
        Some(Commands::Rollback { be_name, snapshot }) => {
            beroot.rollback(be_name, snapshot);
        }
        None => {
            beroot.list(None, false, false, false, false, false, false);
        }
    }
}

#[test]
fn verify_cli() {
    use clap::CommandFactory;
    Cli::command().debug_assert();
}
