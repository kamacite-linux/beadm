use std::path::PathBuf;

use chrono::{Local, TimeZone};
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

struct BootEnvironment {
    /// The name of this boot environment.
    name: String,
    /// A description for this boot environment, if any.
    description: Option<String>,
    /// If the boot environment is currently mounted, this is its mountpoint.
    mountpoint: Option<PathBuf>,
    /// Whether the system is currently booted into this boot environment.
    active: bool,
    /// Whether the system will reboot into this environment.
    next_boot: bool,
    /// Bytes on the filesystem associated with this boot environment.
    space: u64,
    /// Unix timestamp for when this boot environment was created.
    created: i64,
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

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), BeError>;

    fn unmount(&self, target: &str, force: bool) -> Result<(), BeError>;

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), BeError>;

    fn activate(&self, be_name: &str, temporary: bool, remove_temp: bool) -> Result<(), BeError>;

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), BeError>;

    fn iter(&self) -> Box<dyn Iterator<Item = &BootEnvironment> + '_>;
}

struct NoopBeRoot {
    root: String,
    bes: Vec<BootEnvironment>,
}

impl NoopBeRoot {
    fn new(root_path: Option<String>, bes: Vec<BootEnvironment>) -> Self {
        let root = match root_path {
            Some(p) => p,
            None => "zfake/ROOT".to_string(),
        };
        Self { root, bes }
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

    fn iter(&self) -> Box<dyn Iterator<Item = &BootEnvironment> + '_> {
        // Return iterator over the stored sample boot environments
        Box::new(self.bes.iter())
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
    command: Commands,
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
        /// Omit headers and formatting, separate fields by a single tab
        #[arg(short = 'H')]
        parseable: bool,
        /// Sort by field, ascending
        #[arg(short = 'k', default_value = "date")]
        sort_asc: SortField,
        /// Sort by field, descending
        #[arg(short = 'K')]
        sort_des: Option<SortField>,
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

/// Field to sort boot environments by when listing them.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum SortField {
    /// Sort by name.
    Name,
    /// Sort by created date.
    Date,
    /// Sort by space.
    Space,
}

fn format_active_flags(be: &BootEnvironment) -> Option<String> {
    if !be.next_boot && !be.active {
        return None;
    }
    let mut flags = String::new();
    if be.next_boot {
        flags.push('N');
    }
    if be.active {
        flags.push('R');
    }
    Some(flags)
}

fn format_space(bytes: u64) -> String {
    // TODO: libzfs has a utility for this we should use, if possible.
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{}K", bytes / 1024)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{}M", bytes / (1024 * 1024))
    } else {
        format!("{}G", bytes / (1024 * 1024 * 1024))
    }
}

fn format_timestamp(timestamp: i64) -> String {
    match Local.timestamp_opt(timestamp, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M").to_string(),
        _ => format!("{}", timestamp), // Fallback to raw timestamp if conversion fails
    }
}

/// Options to control printing boot environments with `beadm list`.
struct PrintOptions<'a> {
    be_name: &'a Option<String>,
    sort_field: SortField,
    descending: bool,
    parseable: bool,
}

/// Prints a list of boot environments in the traditional `beadm list` format.
fn print_boot_environments<T: BeRoot>(
    root: &T,
    mut writer: impl std::io::Write,
    options: PrintOptions,
) -> Result<(), BeError> {
    let mut bes: Vec<&BootEnvironment> = root.iter().collect();

    // Allow narrowing the output to a single boot environment (if it exists).
    if let Some(filter_name) = options.be_name {
        bes.retain(|be| be.name == *filter_name);
    }

    // Sorting.
    match options.sort_field {
        SortField::Date => {
            bes.sort_by_key(|be| be.created);
        }
        SortField::Name => {
            bes.sort_by(|a, b| a.name.cmp(&b.name));
        }
        SortField::Space => {
            bes.sort_by_key(|be| be.space);
        }
    }
    if options.descending {
        bes.reverse();
    }

    // "Machine-parsable" output: no headers, tab-separated fields.
    //
    // beadm from illumos uses semicolons for -H, but bectl from FreeBSD
    // (sensibly) opts for tabs, which we follow. This also matches the
    // behaviour of zfs list -H.
    if options.parseable {
        for be in bes {
            writeln!(
                writer,
                "{}\t{}\t{}\t{}\t{}\t{}",
                be.name,
                match format_active_flags(be) {
                    Some(s) => s,
                    None => "".to_string(),
                },
                match &be.mountpoint {
                    Some(m) => m.clone().display().to_string(),
                    None => "".to_string(),
                },
                be.space,
                be.created,
                match &be.description {
                    Some(d) => d.clone(),
                    None => "".to_string(),
                }
            )?;
        }
        return Ok(());
    }

    // Calculate dynamic column widths for fields that can be longer than their
    // respective header.
    let mut name_width = 4;
    let mut mountpoint_width = 10;
    let mut space_width = 5;
    for be in &bes {
        name_width = name_width.max(be.name.len());
        if be.mountpoint.is_some() {
            mountpoint_width =
                mountpoint_width.max(be.mountpoint.clone().unwrap().display().to_string().len());
        }
        space_width = space_width.max(format_space(be.space).len());
    }

    // The traditional 'beadm list' format, with minor differences:
    //
    // - We support a "description" column.
    // - Headers are uppercase with no separator, similar to other zfs commands.
    writeln!(
        writer,
        "{:<name_width$}  {:<6}  {:<mountpoint_width$}  {:<space_width$}  {:<16}  {}",
        "NAME",
        "ACTIVE",
        "MOUNTPOINT",
        "SPACE",
        "CREATED",
        "DESCRIPTION",
        name_width = name_width,
        mountpoint_width = mountpoint_width,
        space_width = space_width
    )?;
    for be in bes {
        writeln!(
            writer,
            "{:<name_width$}  {:<6}  {:<mountpoint_width$}  {:<space_width$}  {:<16}  {}",
            be.name,
            match format_active_flags(be) {
                Some(s) => s,
                None => "-".to_string(),
            },
            match &be.mountpoint {
                Some(m) => m.clone().display().to_string(),
                None => "-".to_string(),
            },
            format_space(be.space),
            format_timestamp(be.created),
            match &be.description {
                Some(d) => d.clone(),
                None => "-".to_string(),
            },
            name_width = name_width,
            mountpoint_width = mountpoint_width,
            space_width = space_width
        )?;
    }

    Ok(())
}

fn sample_boot_environments() -> Vec<BootEnvironment> {
    vec![
        BootEnvironment {
            name: "default".to_string(),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/")),
            active: true,
            next_boot: true,
            space: 950_000_000,  // ~906M
            created: 1623301740, // 2021-06-10 01:09
        },
        BootEnvironment {
            name: "alt".to_string(),
            description: Some("Testing".to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            space: 8192,         // 8K
            created: 1623305460, // 2021-06-10 02:11
        },
    ]
}

fn main() {
    let cli = Cli::parse();
    let beroot = NoopBeRoot::new(cli.beroot.clone(), sample_boot_environments());

    if cli.verbose {
        println!("Verbose mode enabled");
    }

    let result = match &cli.command {
        Commands::Create {
            be_name,
            activate,
            temp_activate,
            description,
            clone_from,
            property,
        } => {
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
        Commands::Destroy {
            target,
            force_unmount,
            force_no_verify,
            snapshots,
        } => beroot.destroy(target, *force_unmount, *force_no_verify, *snapshots),
        Commands::List {
            be_name,
            all: _,
            datasets: _,
            snapshots: _,
            parseable,
            sort_asc,
            sort_des,
        } => {
            // TODO: Implement -a, -s, -d.

            // TODO: This is a bit lazy; there should probably be an error if
            // both -k and -K are specified.
            let sort_field = sort_des.unwrap_or(*sort_asc);
            let options = PrintOptions {
                be_name,
                sort_field,
                descending: sort_des.is_some(),
                parseable: *parseable,
            };

            print_boot_environments(&beroot, &mut std::io::stdout(), options)
        }
        Commands::Mount {
            be_name,
            mountpoint,
            mode,
        } => beroot.mount(be_name, mountpoint, *mode),
        Commands::Unmount { target, force } => beroot.unmount(target, *force),
        Commands::Rename { be_name, new_name } => beroot.rename(be_name, new_name),
        Commands::Activate {
            be_name,
            temporary,
            remove_temp,
        } => beroot.activate(be_name, *temporary, *remove_temp),
        Commands::Rollback { be_name, snapshot } => beroot.rollback(be_name, snapshot),
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

#[test]
fn test_print_boot_environments_output() {
    let beroot = NoopBeRoot::new(None, sample_boot_environments());
    let mut output = Vec::new();
    let options = PrintOptions {
        be_name: &None,
        sort_field: SortField::Date,
        descending: false,
        parseable: false,
    };
    print_boot_environments(&beroot, &mut output, options).unwrap();
    assert_eq!(
        String::from_utf8(output).unwrap(),
        r"NAME     ACTIVE  MOUNTPOINT  SPACE  CREATED           DESCRIPTION
default  NR      /           905M   2021-06-10 01:09  -
alt      -       -           8K     2021-06-10 02:11  Testing
"
    );
}

#[test]
fn test_print_boot_environments_parseable() {
    let beroot = NoopBeRoot::new(None, sample_boot_environments());
    let mut output = Vec::new();
    print_boot_environments(
        &beroot,
        &mut output,
        PrintOptions {
            be_name: &None,
            sort_field: SortField::Date,
            descending: false,
            parseable: true,
        },
    )
    .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], "default\tNR\t/\t950000000\t1623301740\t");
    assert_eq!(lines[1], "alt\t\t\t8192\t1623305460\tTesting");
}

#[test]
fn test_print_boot_environments_filtered() {
    let beroot = NoopBeRoot::new(None, sample_boot_environments());
    let mut output = Vec::new();
    print_boot_environments(
        &beroot,
        &mut output,
        PrintOptions {
            be_name: &Some("default".to_string()),
            sort_field: SortField::Date,
            descending: false,
            parseable: true,
        },
    )
    .unwrap();

    // Check that only default BE is shown.
    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.lines().collect();
    assert!(lines[0].starts_with("default"));
    assert_eq!(lines.len(), 1);
}

#[test]
fn test_print_boot_environments_sorting() {
    let beroot = NoopBeRoot::new(None, sample_boot_environments());
    let mut output = Vec::new();
    print_boot_environments(
        &beroot,
        &mut output,
        PrintOptions {
            be_name: &None,
            sort_field: SortField::Name,
            descending: true,
            parseable: true,
        },
    )
    .unwrap();

    // With name descending, "default" should come before "alt".
    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.lines().collect();
    assert!(lines[0].starts_with("default"));
    assert!(lines[1].starts_with("alt"));
}
