# beadm (development version)

# beadm v0.1.0

`beadm` is a tool for managing ZFS boot environments on Linux. It is largely
compatible with `beadm` from illumos and `bectl` from FreeBSD, but can operate
over D-Bus and outsource authorisation to Polkit. `beadm` is made available
under the terms of the Mozilla Public License, version 2.0.

## Features

* `beadm` implements the traditional `activate`, `create`, `snapshot`,
  `destroy`, `list`, `mount`, `unmount`, `rename`, and `rollback` subcommands.

* A `beadm describe` subcommand sets a description for a boot environment or
  snapshot, which is then visible in `beadm list`.

* A `beadm daemon` subcommand starts the D-Bus service.

* A `beadm init` subcommand creates the ZFS dataset layout for boot
  environments.

* A `beadm(8)` man page.

* systemd and Dinit services.

* APT and APK hooks that take a snapshot prior to changes.
