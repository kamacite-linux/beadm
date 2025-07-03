# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Project Overview

beadm is a Boot Environment Administration utility implemented in Rust. It
provides both a command-line interface and a D-Bus service for managing ZFS boot
environments, similar to the FreeBSD `bectl` or illumos `beadm` tools.

## Core Architecture

The codebase is structured around a flexible client abstraction with multiple
implementations and interfaces:

### Client Abstraction (`src/be/mod.rs`)

- **Client Trait**: Defines the core operations for boot environment management
  (create, new, destroy, mount, unmount, rename, activate, deactivate, rollback)
- **BootEnvironment Struct**: Represents a boot environment with properties:
  - `name`: Human-readable name
  - `path`: ZFS dataset path
  - `guid`: ZFS dataset GUID (unique identifier)
  - `description`: Optional description
  - `mountpoint`: Current mount location (if mounted)
  - `active`, `next_boot`, `boot_once`: Boot status flags
  - `space`: Storage usage in bytes
  - `created`: Unix timestamp of creation

### Client Implementations

- **Mock Implementation (`src/be/mock.rs`)**: An in-memory emulator that
  simulates ZFS operations without side effects, used for testing and
  development. Generates deterministic GUIDs based on boot environment names.

- **LibZFS Implementation (`src/be/zfs.rs`)**: Production implementation that
  interfaces with the actual ZFS library through FFI bindings to libzfs.
  Includes safe Rust wrappers around raw libzfs handles and operations.
  Retrieves actual ZFS dataset GUIDs via `ZFS_PROP_GUID`.

- **Thread-Safe Wrapper (`src/be/threadsafe.rs`)**: Provides thread-safe access
  to any client implementation using `Mutex`.

- **D-Bus Remote Client (`src/dbus.rs`)**: Client that connects to the D-Bus
  service to perform operations remotely.

### Supporting Components

- **Validation (`src/be/validation.rs`)**: Validates boot environment names
  and dataset names according to ZFS naming rules

- **CLI Interface (`src/main.rs`)**: Uses `clap` for argument parsing and
  delegates operations to the client implementation. Switches between mock
  and real ZFS implementations based on the `--client` argument.

## D-Bus Service Architecture

The D-Bus service (`src/dbus.rs`) provides a comprehensive system interface:

### Service Components

- **BeadmManager**: Implements the `org.beadm.Manager` interface for boot
  environment operations (create, new operations)

- **BeadmObjectManager**: Implements `org.freedesktop.DBus.ObjectManager` for
  object discovery and enumeration

- **BootEnvironmentObject**: Individual boot environment objects implementing
  `org.beadm.BootEnvironment` interface with properties and methods

### Object Path Design

Boot environments are exposed as D-Bus objects using **GUID-based paths**:

- Path format: `/org/beadm/BootEnvironments/{guid:016x}`
- Example: `/org/beadm/BootEnvironments/1234567890abcdef`

This design eliminates issues with:

- Special characters in boot environment names
- Path conflicts when renaming boot environments
- Sanitization problems with diverse naming schemes

### D-Bus Interfaces

1. **org.beadm.Manager** (`/org/beadm/Manager`)
   - `create()`: Clone existing boot environment
   - `create_new()`: Create new empty boot environment

2. **org.freedesktop.DBus.ObjectManager** (`/org/beadm/Manager`)
   - `GetManagedObjects()`: Discover all boot environments

3. **org.beadm.BootEnvironment** (`/org/beadm/BootEnvironments/{guid}`)
   - Properties: `Name`, `Path`, `Guid`, `Description`, `Mountpoint`, `Active`, etc.
   - Methods: `destroy()`, `mount()`, `unmount()`, `rename()`, `activate()`, etc.

## Development Commands

```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Run tests in a specific module
cargo test mock::tests

# Check code without building
cargo check

# Format code
cargo fmt

# Build and run the CLI (uses mock by default)
cargo run -- list

# Run with real ZFS implementation
cargo run -- --client libzfs list

# Run with D-Bus remote client
cargo run -- --client dbus list
```

## Key Implementation Details

- **Error Handling**: Uses `thiserror` for structured error types with
  user-friendly messages

- **Validation**: Boot environment names must follow ZFS dataset naming rules
  (alphanumeric start, limited special characters)

- **FFI Safety**: The ZFS client wraps unsafe libzfs calls in safe Rust APIs,
  managing memory and handle lifetimes automatically

- **Resource Management**: Dataset handles are automatically closed when dropped,
  and the libzfs handle is properly finalized on client destruction

- **GUID Usage**: Uses actual ZFS dataset GUIDs for stable object identification
  across renames and to avoid naming conflicts

- **Testing Strategy**: Extensive unit tests cover both happy path and error
  conditions for all operations across multiple client implementations

## D-Bus Service Usage

### Service Information

- **Service Name**: `org.beadm.Manager`
- **Manager Object**: `/org/beadm/Manager`
- **Boot Environment Objects**: `/org/beadm/BootEnvironments/{guid}`

### Usage Examples

```bash
# List all boot environments (system bus)
busctl --system call org.beadm.Manager /org/beadm/Manager org.freedesktop.DBus.ObjectManager GetManagedObjects

# List all boot environments (session bus)
busctl --user call org.beadm.Manager /org/beadm/Manager org.freedesktop.DBus.ObjectManager GetManagedObjects

# Destroy a boot environment (using GUID-based path)
busctl --system call org.beadm.Manager /org/beadm/BootEnvironments/1234567890abcdef org.beadm.BootEnvironment destroy false false false

# Mount a boot environment
busctl --system call org.beadm.Manager /org/beadm/BootEnvironments/abcdef1234567890 org.beadm.BootEnvironment mount "/mnt/test" false

# Get boot environment properties
busctl --system get-property org.beadm.Manager /org/beadm/BootEnvironments/1234567890abcdef org.beadm.BootEnvironment Active
busctl --system get-property org.beadm.Manager /org/beadm/BootEnvironments/1234567890abcdef org.beadm.BootEnvironment Guid
```

### Starting the Service

```bash
# Start D-Bus service on system bus
cargo run -- serve

# Start D-Bus service on session bus
cargo run -- serve --user
```

## Output Formatting

The CLI supports both human-readable and machine-parseable output formats:

- Human format includes headers and formatted columns
- Parseable format (`-H` flag) uses tab-separated values without headers
- Space values are formatted with human-readable units (K, M, G)
- Timestamps are displayed in local time format

## Development Workflow

- Always run `cargo fmt` after making changes
- Use `cargo check` for quick compilation verification
- Run full test suite with `cargo test` before committing
- Test D-Bus functionality with both system and session buses
- Verify GUID-based object paths work correctly after boot environment operations

## Architecture Benefits

1. **Flexibility**: Multiple client implementations support different use cases
2. **Testability**: Mock client enables comprehensive testing without ZFS
3. **Safety**: Safe Rust wrappers around unsafe FFI operations
4. **Stability**: GUID-based object paths survive renames and avoid conflicts
5. **Interoperability**: D-Bus service enables integration with other tools
6. **Maintainability**: Clear separation of concerns and comprehensive error handling
