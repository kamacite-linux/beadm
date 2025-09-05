use anyhow::{Context, Result};
#[cfg(feature = "dbus")]
use async_io::block_on;
use chrono::{Local, TimeZone};
use clap::{Parser, Subcommand, ValueEnum};
use std::fs;
use std::path::PathBuf;

mod be;
#[cfg(feature = "dbus")]
mod dbus;
#[cfg(feature = "hooks")]
mod hooks;

use be::mock::EmulatorClient;
use be::zfs::{DatasetName, LibZfsClient, format_zfs_bytes, get_active_boot_environment_root};
use be::{BootEnvironment, Client, Error, Label, MountMode, Snapshot};
#[cfg(feature = "dbus")]
use dbus::{ClientProxy, serve};

#[derive(Parser)]
#[command(version, about = "Boot Environment Administration")]
struct Cli {
    /// Set the boot environment root
    ///
    /// The boot environment root is a dataset whose children are all boot
    /// environments. Defaults to the parent dataset of the active boot
    /// environment.
    #[arg(short = 'r', global = true, help_heading = "Global options")]
    beroot: Option<String>,

    /// Verbose output
    #[arg(short = 'v', global = true, help_heading = "Global options")]
    verbose: bool,

    /// Client implementation
    #[arg(
        long = "client",
        global = true,
        help_heading = "Global options",
        default_value = "libzfs"
    )]
    client: ClientType,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Mark a boot environment as the default root filesystem.
    Activate {
        /// The boot environment to activate.
        #[arg(required_unless_present = "deactivate")]
        be_name: Option<String>,

        /// Activate the boot environment only for the next boot.
        #[arg(short = 't', conflicts_with = "deactivate")]
        temporary: bool,

        /// Remove any temporary activations instead.
        #[arg(short = 'T', conflicts_with = "temporary")]
        deactivate: bool,
    },
    /// Create a new boot environment.
    Create {
        /// A name for the new boot environment.
        be_name: String,

        /// Activate the new boot environment after creating it.
        #[arg(short = 'a', conflicts_with = "temp_activate")]
        activate: bool,

        /// Temporarily activate the new boot environment after creating it.
        #[arg(short = 't', conflicts_with = "activate")]
        temp_activate: bool,

        /// An optional description for the new boot environment.
        #[arg(short = 'd')]
        description: Option<String>,

        /// Create the new boot environment from this boot environment or
        /// snapshot, rather than the active one.
        #[arg(short = 'e', conflicts_with = "empty")]
        source: Option<Label>,

        /// Set additional ZFS properties for the new boot environment (in
        /// 'property=value' format).
        #[arg(short = 'o')]
        property: Vec<String>,

        /// Create an empty boot environment instead of cloning another boot
        /// environment or snapshot.
        #[arg(long, conflicts_with_all = vec!["source", "activate", "temp_activate"])]
        empty: bool,

        /// Set the host ID for empty boot environments. Defaults to the value
        /// in /etc/hostid
        #[arg(long)]
        host_id: Option<String>,

        /// Set a description for an empty boot environment using PRETTY_NAME
        /// from an /etc/os-release file.
        #[arg(
            long,
            value_name = "FILE",
            value_hint = clap::ValueHint::FilePath,
            requires = "empty",
            conflicts_with = "description"
        )]
        use_os_release: Option<PathBuf>,
    },
    /// Create a snapshot of a boot environment.
    Snapshot {
        /// The boot environment and optional snapshot (in the form 'beName' or
        /// 'beName@snapshot').
        ///
        /// When the snapshot name is omitted, one will be generated
        /// automatically from the current time.
        source: Option<Label>,

        /// An optional description for the snapshot.
        #[arg(short = 'd')]
        description: Option<String>,
    },
    /// Destroy an existing boot environment or snapshot.
    Destroy {
        /// The boot environment or snapshot (in the form 'beName' or
        /// 'beName@snapshot').
        target: String,

        /// Forcefully unmount the boot environment if needed.
        #[arg(short = 'f')]
        force_unmount: bool,

        /// Destroy snapshots of the boot environment if needed.
        #[arg(short = 's')]
        destroy_snapshots: bool,
    },
    /// List boot environments.
    List {
        /// Include only this boot environment.
        be_name: Option<String>,

        /// Include subordinate filesystems and snapshots of each boot environment.
        #[arg(short = 'a')]
        all: bool,

        /// Include subordinate filesystems of each boot environment.
        #[arg(short = 'd', conflicts_with = "all")]
        datasets: bool,

        /// Include snapshots of each boot environment.
        #[arg(short = 's', conflicts_with = "all")]
        snapshots: bool,

        /// Omit headers and formatting, separate fields by a single tab.
        #[arg(short = 'H')]
        parseable: bool,

        /// Sort boot environments by this property, ascending.
        #[arg(
            short = 'k',
            value_name = "PROP",
            default_value = "date",
            conflicts_with = "sort_des"
        )]
        sort_asc: SortField,

        /// Sort boot environments by this property, descending.
        #[arg(short = 'K', value_name = "PROP", conflicts_with = "sort_asc")]
        sort_des: Option<SortField>,
    },
    /// Mount a boot environment.
    Mount {
        /// The boot environment to mount.
        ///
        /// The active boot environment (i.e. the current root filesystem)
        /// is already mounted and cannot have its mountpoint changed.
        be_name: String,

        /// A mount point (if omitted, creates a temporary mount in /tmp).
        #[arg(value_hint = clap::ValueHint::DirPath)]
        mountpoint: Option<String>,

        /// Mount as read/write or read-only.
        #[arg(short = 's', default_value = "rw")]
        mode: MountMode,
    },
    /// Unmount an inactive boot environment.
    ///
    /// Unmounting will not remove the mountpoint unless it is one we created.
    Unmount {
        /// The boot environment.
        ///
        /// The active boot environment (i.e. the current root filesystem)
        /// cannot be unmounted.
        be_name: String,

        /// Force unmounting.
        #[arg(short = 'f')]
        force: bool,
    },
    /// Rename a boot environment.
    Rename {
        /// The boot environment.
        be_name: String,

        /// A new name for the boot environment.
        new_name: String,
    },
    /// Set a description for an existing boot environment or snapshot.
    Describe {
        /// The boot environment or snapshot (in the form 'beName' or
        /// 'beName@snapshot').
        target: Label,

        /// The description to set.
        description: String,
    },
    /// Roll back a boot environment to an earlier snapshot.
    Rollback {
        /// The boot environment.
        be_name: String,

        /// The snapshot name.
        snapshot: String,
    },
    /// Get the host ID from a boot environment.
    Hostid {
        /// The boot environment.
        be_name: String,
    },
    /// Create the ZFS dataset layout for boot environments.
    Init {
        /// The ZFS pool to target.
        pool: String,
    },
    /// Start the boot environment D-Bus daemon.
    #[cfg(feature = "dbus")]
    Daemon {
        /// Run on the session bus instead of the system bus.
        #[arg(long)]
        user: bool,
    },
    /// APT hook integration.
    #[cfg(feature = "hooks")]
    #[command(hide = true)]
    AptHook,
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

/// Client selection.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ClientType {
    /// Use the D-Bus client.
    #[cfg(feature = "dbus")]
    #[value(name = "dbus")]
    DBus,
    /// Use LibZFS directly.
    #[value(name = "libzfs")]
    LibZfs,
    /// Use a mock/emulator client (for testing).
    #[value(name = "mock")]
    Mock,
}

/// A row in `beadm list` output, either a boot environment or a snapshot.
#[derive(Clone)]
enum ListRow {
    BootEnvironment(BootEnvironment),
    Snapshot(Snapshot),
}

impl ListRow {
    fn name(&self) -> &str {
        match self {
            ListRow::BootEnvironment(be) => &be.name,
            ListRow::Snapshot(snapshot) => &snapshot.name,
        }
    }

    fn space(&self) -> u64 {
        match self {
            ListRow::BootEnvironment(be) => be.space,
            ListRow::Snapshot(snapshot) => snapshot.space,
        }
    }

    fn created(&self) -> i64 {
        match self {
            ListRow::BootEnvironment(be) => be.created,
            ListRow::Snapshot(snapshot) => snapshot.created,
        }
    }

    fn active_flags(&self) -> Option<String> {
        match self {
            ListRow::BootEnvironment(be) => format_active_flags(be),
            ListRow::Snapshot(_) => None,
        }
    }

    fn mountpoint(&self) -> Option<String> {
        match self {
            ListRow::BootEnvironment(be) => match be.mountpoint.as_ref() {
                Some(m) => Some(m.display().to_string()),
                None => None,
            },
            ListRow::Snapshot(_) => None,
        }
    }

    fn description(&self) -> Option<&str> {
        match self {
            ListRow::BootEnvironment(be) => be.description.as_deref(),
            ListRow::Snapshot(snapshot) => snapshot.description.as_deref(),
        }
    }
}

fn format_active_flags(be: &BootEnvironment) -> Option<String> {
    if !be.next_boot && !be.active && !be.boot_once {
        return None;
    }
    let mut flags = String::new();
    if be.next_boot {
        flags.push('N');
    }
    if be.active {
        flags.push('R');
    }
    if be.boot_once {
        flags.push('T');
    }
    Some(flags)
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
    snapshots: bool,
}

/// Prints a list of boot environments in the traditional `beadm list` format.
fn print_boot_environments<T: Client>(
    root: &T,
    mut writer: impl std::io::Write,
    options: PrintOptions,
) -> Result<(), Error> {
    let mut bes = root.get_boot_environments()?;

    // Allow narrowing the output to a single boot environment (if it exists).
    if let Some(filter_name) = options.be_name {
        bes.retain(|be| be.name == *filter_name);
    }

    // Sort boot environments first.
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

    // Convert boot environments (and optionally their snapshots) to rows.
    let mut rows: Vec<ListRow> = Vec::new();
    for be in bes.into_iter() {
        let name = be.name.clone();
        rows.push(ListRow::BootEnvironment(be));

        // Group snapshots under their respective boot environment.
        if options.snapshots {
            let mut snapshots = root.get_snapshots(&name)?;
            // Sort snapshots by the same field as boot environments
            match options.sort_field {
                SortField::Date => {
                    snapshots.sort_by_key(|snap| snap.created);
                }
                SortField::Name => {
                    snapshots.sort_by(|a, b| a.name.cmp(&b.name));
                }
                SortField::Space => {
                    snapshots.sort_by_key(|snap| snap.space);
                }
            }
            rows.extend(snapshots.into_iter().map(ListRow::Snapshot));
        }
    }

    // "Machine-parsable" output: no headers, tab-separated fields.
    //
    // beadm from illumos uses semicolons for -H, but bectl from FreeBSD
    // (sensibly) opts for tabs, which we follow. This also matches the
    // behaviour of zfs list -H.
    if options.parseable {
        for row in rows {
            writeln!(
                writer,
                "{}\t{}\t{}\t{}\t{}\t{}",
                row.name(),
                row.active_flags().unwrap_or("".to_string()),
                row.mountpoint().unwrap_or("".to_string()),
                row.space(),
                row.created(),
                row.description().unwrap_or("")
            )?;
        }
        return Ok(());
    }

    // Calculate dynamic column widths for fields that can be longer than their
    // respective header.
    let mut name_width = 4;
    let mut mountpoint_width = 10;
    for row in &rows {
        name_width = name_width.max(row.name().len());
        if let Some(mountpoint) = row.mountpoint() {
            mountpoint_width = mountpoint_width.max(mountpoint.len());
        }
    }

    // The traditional 'beadm list' format, with minor differences:
    //
    // - We support a "description" column.
    // - Headers are uppercase with no separator, similar to other zfs commands.
    writeln!(
        writer,
        "{:<name_width$}  {:<6}  {:<mountpoint_width$}  {}  {:<16}  {}",
        "NAME",
        "ACTIVE",
        "MOUNTPOINT",
        "SPACE",
        "CREATED",
        "DESCRIPTION",
        name_width = name_width,
        mountpoint_width = mountpoint_width
    )?;
    for row in rows {
        writeln!(
            writer,
            "{:<name_width$}  {:<6}  {:<mountpoint_width$}  {:<5}  {:<16}  {}",
            row.name(),
            row.active_flags().unwrap_or("-".to_string()),
            row.mountpoint().unwrap_or("-".to_string()),
            format_zfs_bytes(row.space()),
            format_timestamp(row.created()),
            row.description().unwrap_or("-"),
            name_width = name_width,
            mountpoint_width = mountpoint_width
        )?;
    }

    Ok(())
}

/// Check if a mountpoint path looks like one of our temporary ones.
fn is_temp_mountpoint(path: &PathBuf) -> bool {
    let prefix = std::env::temp_dir().join("be_mount.");
    // Safe to unwrap because we know the prefix is valid UTF-8.
    path.to_string_lossy().starts_with(prefix.to_str().unwrap())
}

/// Parse the `PRETTY_NAME` field from an `/etc/os-release`-style file.
fn parse_os_release_pretty_name(path: &PathBuf) -> Result<String> {
    let content = fs::read_to_string(path)?;

    // We could use a regular expression for this instead.
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with("PRETTY_NAME=") {
            let value = &line[12..];

            // Handle quoted values.
            if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
                return Ok(value[1..value.len() - 1].to_string());
            } else if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
                return Ok(value[1..value.len() - 1].to_string());
            } else {
                return Ok(value.to_string());
            }
        }
    }

    anyhow::bail!(
        "PRETTY_NAME field not found in os-release file: '{}'",
        path.to_string_lossy()
    )
}

fn execute_command<T: Client + 'static>(command: &Commands, client: T) -> Result<()> {
    match command {
        Commands::Create {
            be_name,
            activate,
            temp_activate,
            description,
            source,
            property,
            empty,
            host_id,
            use_os_release,
        } => {
            if *empty {
                let final_description = if let Some(os_release_path) = use_os_release {
                    Some(parse_os_release_pretty_name(os_release_path)?)
                } else {
                    description.clone()
                };

                client
                    .create_empty(
                        be_name,
                        final_description.as_deref(),
                        host_id.as_deref(),
                        property,
                    )
                    .context("Failed to create empty boot environment")?;
                println!("Created empty boot environment '{}'.", be_name);
                return Ok(());
            }

            client
                .create(be_name, description.as_deref(), source.as_ref(), property)
                .context("Failed to create boot environment")?;
            if *activate || *temp_activate {
                client
                    .activate(be_name, *temp_activate)
                    .context("Failed to activate newly-created boot environment")?;
            }
            println!(
                "Created {} boot environment '{}'.",
                if *activate {
                    "active"
                } else if *temp_activate {
                    "temporarily active"
                } else {
                    "inactive"
                },
                be_name
            );
            Ok(())
        }
        Commands::Destroy {
            target,
            force_unmount,
            destroy_snapshots,
        } => {
            client
                .destroy(target, *force_unmount, *destroy_snapshots)
                .context("Failed to destroy boot environment")?;
            println!("Destroyed '{}'.", target);
            Ok(())
        }
        Commands::List {
            be_name,
            all: _,
            datasets: _,
            snapshots,
            parseable,
            sort_asc,
            sort_des,
        } => {
            // TODO: Implement -a, -d.

            let sort_field = sort_des.unwrap_or(*sort_asc);
            let options = PrintOptions {
                be_name,
                sort_field,
                descending: sort_des.is_some(),
                parseable: *parseable,
                snapshots: *snapshots,
            };

            print_boot_environments(&client, &mut std::io::stdout(), options)
                .context("Failed to list boot environments")?;
            Ok(())
        }
        Commands::Mount {
            be_name,
            mountpoint,
            mode,
        } => {
            if let Some(mountpoint) = mountpoint {
                client
                    .mount(be_name, mountpoint, *mode)
                    .context("Failed to mount boot environment")?;
                return Ok(());
            }

            // If no mountpoint is specified, create a temporary one and write
            // it to standard output for downstream consumption.
            let mut temp_dir = tempfile::TempDir::with_prefix("be_mount.")
                .context("Failed to create temporary mountpoint directory")?;
            let temp_path = temp_dir.path().to_string_lossy().to_string();
            client
                .mount(be_name, &temp_path, *mode)
                .context("Failed to mount boot environment at temporary path")?;
            temp_dir.disable_cleanup(true);
            println!("{}", temp_path);
            Ok(())
        }
        Commands::Unmount { be_name, force } => {
            let mountpoint = client
                .unmount(be_name, *force)
                .context("Failed to unmount boot environment")?;

            // Check for temporary mountpoints we need to clean up.
            if let Some(mp) = mountpoint {
                if is_temp_mountpoint(&mp) {
                    std::fs::remove_dir_all(&mp)
                        .context("Failed to clean up temporary mountpoint")?;
                }
            }

            Ok(())
        }
        Commands::Rename { be_name, new_name } => {
            client
                .rename(be_name, new_name)
                .context("Failed to rename boot environment")?;
            println!("Renamed boot environment '{}' to '{}'.", be_name, new_name);
            Ok(())
        }
        Commands::Activate {
            be_name,
            temporary,
            deactivate,
        } => {
            if *deactivate {
                client
                    .clear_boot_once()
                    .context("Failed to remove temporary boot environment activation")?;
                println!("Removed temporary boot environment activation.");
            } else {
                // SAFETY: Safe due to required_unless_present.
                let be_name = be_name.as_ref().unwrap();
                client
                    .activate(be_name, *temporary)
                    .context("Failed to activate boot environment")?;
                println!(
                    "Activated '{}'{}.",
                    be_name,
                    if *temporary { " temporarily" } else { "" }
                );
            }
            Ok(())
        }
        Commands::Rollback { be_name, snapshot } => {
            client
                .rollback(be_name, snapshot)
                .context("Failed to rollback to snapshot")?;
            println!("Rolled back to '{}'.", snapshot);
            Ok(())
        }
        Commands::Hostid { be_name } => {
            match client
                .hostid(be_name)
                .context("Failed to retrieve host ID")?
            {
                Some(id) => println!("0x{:08x}", id),
                None => {
                    // TODO: Should probably be using anyhow here.
                    eprintln!("No host ID found for '{}'.", be_name);
                    std::process::exit(1);
                }
            }
            Ok(())
        }
        Commands::Snapshot {
            source,
            description,
        } => {
            let snapshot_name = client
                .snapshot(source.as_ref(), description.as_deref())
                .context("Failed to create snapshot")?;
            println!("Created '{}'.", snapshot_name);
            Ok(())
        }
        Commands::Describe {
            target,
            description,
        } => {
            client
                .describe(&target, description)
                .context("Failed to set description")?;
            println!("Set description for '{}'.", target);
            Ok(())
        }
        Commands::Init { pool } => {
            client
                .init(pool)
                .context("Failed to initialize a boot environment dataset layout")?;
            println!("Boot environment dataset layout initialized.");
            Ok(())
        }
        #[cfg(feature = "dbus")]
        Commands::Daemon { user } => {
            block_on(serve(client, *user)).context("Failed to start D-Bus service")?;
            Ok(())
        }
        #[cfg(feature = "hooks")]
        Commands::AptHook => {
            hooks::execute_apt_hook(&client).context("Failed to run APT hook")?;
            Ok(())
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.client {
        ClientType::Mock => {
            let client = EmulatorClient::sampled();
            execute_command(&cli.command, client)?;
        }
        #[cfg(feature = "dbus")]
        ClientType::DBus => {
            // Use the system bus by default.
            let connection = block_on(zbus::Connection::system())?;
            let client = ClientProxy::new(connection)?;
            execute_command(&cli.command, client)?;
        }
        ClientType::LibZfs => {
            let root = match cli.beroot {
                Some(value) => DatasetName::new(&value)?,
                None => get_active_boot_environment_root().context(
                    "Failed to determine the default boot environment root. Consider using the --beroot option.",
                )?,
            };
            execute_command(&cli.command, LibZfsClient::new(root))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn test_print_boot_environments_output() {
        let client = EmulatorClient::sampled();
        let mut output = Vec::new();
        let options = PrintOptions {
            be_name: &None,
            sort_field: SortField::Date,
            descending: false,
            parseable: false,
            snapshots: false,
        };
        print_boot_environments(&client, &mut output, options).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            r"NAME     ACTIVE  MOUNTPOINT  SPACE  CREATED           DESCRIPTION
default  NR      /           906M   2021-06-10 01:09  -
alt      -       -           8K     2021-06-10 02:11  Testing
"
        );
    }

    #[test]
    fn test_print_boot_environments_parseable() {
        let client = EmulatorClient::sampled();
        let mut output = Vec::new();
        print_boot_environments(
            &client,
            &mut output,
            PrintOptions {
                be_name: &None,
                sort_field: SortField::Date,
                descending: false,
                parseable: true,
                snapshots: false,
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
        let client = EmulatorClient::sampled();
        let mut output = Vec::new();
        print_boot_environments(
            &client,
            &mut output,
            PrintOptions {
                be_name: &Some("default".to_string()),
                sort_field: SortField::Date,
                descending: false,
                parseable: true,
                snapshots: false,
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
        let client = EmulatorClient::sampled();
        let mut output = Vec::new();
        print_boot_environments(
            &client,
            &mut output,
            PrintOptions {
                be_name: &None,
                sort_field: SortField::Name,
                descending: true,
                parseable: true,
                snapshots: false,
            },
        )
        .unwrap();

        // With name descending, "default" should come before "alt".
        let output_str = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = output_str.lines().collect();
        assert!(lines[0].starts_with("default"));
        assert!(lines[1].starts_with("alt"));
    }

    #[test]
    fn test_print_boot_environments_with_boot_once_flag() {
        let client = EmulatorClient::new(vec![BootEnvironment {
            name: "temp-boot".to_string(),
            path: "zfake/ROOT/temp-boot".to_string(),
            guid: EmulatorClient::generate_guid("temp-boot"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: true, // This should yield the 'T' flag.
            space: 8192,
            created: 1623301740,
        }]);
        let mut output = Vec::new();
        print_boot_environments(
            &client,
            &mut output,
            PrintOptions {
                be_name: &None,
                sort_field: SortField::Date,
                descending: false,
                parseable: true,
                snapshots: false,
            },
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            "temp-boot\tT\t\t8192\t1623301740\t\n"
        );
    }

    #[test]
    fn test_is_temp_mountpoint() {
        assert!(is_temp_mountpoint(
            &std::env::temp_dir().join("be_mount.abc123")
        ));
        assert!(!is_temp_mountpoint(&PathBuf::from("/mnt/custom")));
        assert!(!is_temp_mountpoint(&PathBuf::from("/")));
    }

    #[test]
    fn test_hostid_command() {
        let client = EmulatorClient::sampled();

        // Test hostid command execution with existing BE
        let result = execute_command(
            &Commands::Hostid {
                be_name: "default".to_string(),
            },
            client,
        );
        assert!(result.is_ok());

        // Test with non-existent BE
        let client2 = EmulatorClient::sampled();
        let result = execute_command(
            &Commands::Hostid {
                be_name: "non-existent".to_string(),
            },
            client2,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        let be_err = err.downcast_ref::<Error>().expect("Should be a be::Error");
        assert!(matches!(be_err, Error::NotFound { name } if name == "non-existent"));

        // Note: Testing the "no hostid found" case (exit code 1) requires
        // integration tests since it calls std::process::exit(1)
    }

    #[test]
    fn test_mount_command_with_mountpoint() {
        let client = EmulatorClient::sampled();

        // Test mount with specified mountpoint
        let result = execute_command(
            &Commands::Mount {
                be_name: "alt".to_string(),
                mountpoint: Some("/mnt/test".to_string()),
                mode: MountMode::ReadWrite,
            },
            client,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_mount_command_temp_directory() {
        let client = EmulatorClient::sampled();

        // Test mount without mountpoint (temp directory)
        let result = execute_command(
            &Commands::Mount {
                be_name: "alt".to_string(),
                mountpoint: None,
                mode: MountMode::ReadOnly,
            },
            client,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_mount_command_not_found() {
        let client = EmulatorClient::sampled();

        // Test mount non-existent BE
        let result = execute_command(
            &Commands::Mount {
                be_name: "non-existent".to_string(),
                mountpoint: Some("/mnt/test".to_string()),
                mode: MountMode::ReadWrite,
            },
            client,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        let be_err = err.downcast_ref::<Error>().expect("Should be a be::Error");
        assert!(matches!(be_err, Error::NotFound { name } if name == "non-existent"));
    }

    #[test]
    fn test_unmount_command() {
        let client = EmulatorClient::sampled();

        // First mount a BE
        let mount_result = client.mount("alt", "/mnt/test", MountMode::ReadWrite);
        assert!(mount_result.is_ok());

        // Then unmount it
        let result = execute_command(
            &Commands::Unmount {
                be_name: "alt".to_string(),
                force: false,
            },
            client,
        );
        assert!(result.is_ok());

        // Test unmount non-existent BE
        let client2 = EmulatorClient::sampled();
        let result = execute_command(
            &Commands::Unmount {
                be_name: "non-existent".to_string(),
                force: false,
            },
            client2,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        let be_err = err.downcast_ref::<Error>().expect("Should be a be::Error");
        assert!(matches!(be_err, Error::NotFound { name } if name == "non-existent"));
    }

    #[test]
    fn test_print_boot_environments_with_snapshots() {
        let client = EmulatorClient::sampled();
        let mut output = Vec::new();
        print_boot_environments(
            &client,
            &mut output,
            PrintOptions {
                be_name: &None,
                sort_field: SortField::Date,
                descending: false,
                parseable: false,
                snapshots: true,
            },
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(output).unwrap(),
            r"NAME                      ACTIVE  MOUNTPOINT  SPACE  CREATED           DESCRIPTION
default                   NR      /           906M   2021-06-10 01:09  -
default@2021-06-10-04:30  -       -           395K   2021-06-10 01:30  -
default@2021-06-10-05:10  -       -           395K   2021-06-10 02:10  -
alt                       -       -           8K     2021-06-10 02:11  Testing
alt@backup                -       -           1K     2021-06-10 02:20  -
"
        );

        output = Vec::new();
        print_boot_environments(
            &client,
            &mut output,
            PrintOptions {
                be_name: &None,
                sort_field: SortField::Date,
                descending: false,
                parseable: true,
                snapshots: true,
            },
        )
        .unwrap();

        let output_str = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = output_str.lines().collect();
        assert_eq!(lines.len(), 5);
        assert_eq!(lines[0], "default\tNR\t/\t950000000\t1623301740\t");
        assert_eq!(
            lines[1],
            "default@2021-06-10-04:30\t\t\t404000\t1623303000\t"
        );
        assert_eq!(
            lines[2],
            "default@2021-06-10-05:10\t\t\t404000\t1623305400\t"
        );
        assert_eq!(lines[3], "alt\t\t\t8192\t1623305460\tTesting");
        assert_eq!(lines[4], "alt@backup\t\t\t1024\t1623306000\t");
    }
}
