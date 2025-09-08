# beadm - ZFS Boot Environment Administration

`beadm` is a tool for managing ZFS boot environments on Linux. These boot
environments provide a way to swap between multiple Linux installations (or
versions of the same installation) on a single system. It is also commonly used
to facilitate safe system updates and easy rollback if problems occur.

`beadm` traces its history back to the original `beadm` on Solaris (and later
illumos), as well as `bectl` on FreeBSD. It is not the first attempt to bring
these tools to Linux, but it does have some notable differences in approach:

- It is designed for Linux desktop users, and so makes use of D-Bus, Polkit, and
  other Linux-specific platform features (e.g. systemd) by default.

- It benefits from the speed and correctness guarantees of Rust. Anecdotally, it
  feels much more responsive than other Linux implementations (which are largely
  written in Shell). It is also much easier to modify safely than the illumos or
  FreeBSD implementations, which are written in C.

- It completely sidesteps the complexity of interacting with the bootloader.
  Instead, `beadm` assumes you are using a bootloader that can natively
  understand boot environments (e.g. [ZFSBootMenu](https://zfsbootmenu.org/)).

## Features

- The traditional `beadm` commands to list, create, activate, destroy, rename,
  and mount boot environments are all available.

- First-class support for attaching a description to boot environments and
  snapshots.

- A D-Bus service exposes boot environments to graphical interfaces without the
  need to parse CLI output or use `pkexec`. This also enables writing
  applications that can be distributed via Flakpak.

- Polkit integration for fine-grained permissions and an authorization UI,
  rather than requiring `sudo` for evey operation.

- Linux package manager integration.

## Installation

The `beadm` binary can be built with Cargo, but the project requires Meson and
`scdoc` for a real installation. The usual Meson workflow applies:

```bash
meson setup build
meson compile -C build
meson install -C build
```

Where `meson install` usually requires escalated privileges.

Several features (including D-Bus and systemd integration) are enabled by
default if the host system supports these tools.

Note: for distros with an older version of Polkit, policy files installed to
`/usr/local/share` may not be picked up correctly. Creating a symlink can
alleviate this:

```bash
# ln -s /usr/local/share/polkit-1/actions/ca.kamacite.BootEnvironments1.policy \
  /usr/share/polkit-1/actions/ca.kamacite.BootEnvironments1.policy
```

