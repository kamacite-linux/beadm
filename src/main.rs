use chrono::{Local, TimeZone};
use clap::{Parser, Subcommand, ValueEnum};

mod be;

use be::mock::EmulatorClient;
use be::{BootEnvironment, Client, Error, MountMode};

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
                match format_active_flags(&be) {
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
            match format_active_flags(&be) {
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

fn main() {
    let cli = Cli::parse();
    let client = EmulatorClient::sampled();

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
            let result = client.create(
                be_name,
                description.as_deref(),
                clone_from.as_deref(),
                property,
            );

            if result.is_ok() && (*activate || *temp_activate) {
                client.activate(be_name, *temp_activate, false)
            } else {
                result
            }
        }
        Commands::Destroy {
            target,
            force_unmount,
            force_no_verify,
            snapshots,
        } => client.destroy(target, *force_unmount, *force_no_verify, *snapshots),
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

            print_boot_environments(&client, &mut std::io::stdout(), options)
        }
        Commands::Mount {
            be_name,
            mountpoint,
            mode,
        } => client.mount(be_name, mountpoint, *mode),
        Commands::Unmount { target, force } => client.unmount(target, *force),
        Commands::Rename { be_name, new_name } => client.rename(be_name, new_name),
        Commands::Activate {
            be_name,
            temporary,
            remove_temp,
        } => client.activate(be_name, *temporary, *remove_temp),
        Commands::Rollback { be_name, snapshot } => client.rollback(be_name, snapshot),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
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
        };
        print_boot_environments(&client, &mut output, options).unwrap();
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
            },
        )
        .unwrap();

        // With name descending, "default" should come before "alt".
        let output_str = String::from_utf8(output).unwrap();
        let lines: Vec<&str> = output_str.lines().collect();
        assert!(lines[0].starts_with("default"));
        assert!(lines[1].starts_with("alt"));
    }
}
