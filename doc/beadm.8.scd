beadm(8)

# NAME

beadm - Boot Environment Administration

# SYNOPSIS

*beadm* *activate* _name_ | *-t* _name_ | *-T*

*beadm* *create* [*-a* | *-t*] [*-d* _desc_] [*-o* _property=value_]
	\[*-e* _source_] _name_

*beadm* *create* [*-a* | *-t*] [*-d* _desc_] [*-o* _property=value_]
	\[*--use-os-release* _file_] *--empty* _name_

*beadm* *snapshot* [*-d* _desc_] [_name_ | _name@snapshot_]

*beadm* *destroy* [*-fs*] _name_ | _name@snapshot_

*beadm* *list* [*-adsH*] [*-k* _property_ | *-K* _property_] [_name_]

*beadm* *mount* [*-s* _mode_] _name_ [_mountpoint_]

*beadm* *unmount* [*-f*] _name_

*beadm* *rename* _name_ _new-name_

*beadm* *rollback* _name_ _snapshot_

*beadm* *init* _pool_

*beadm* *daemon*

# DESCRIPTION

*beadm* is a utility for managing ZFS boot environments. Boot environments
provide a way to swap between multiple Linux installations (or versions of the
same installation) on a single system.

*beadm* also faciliates safe system updates and easy rollback if problems occur.

# COMMANDS

*activate* _name_ | *-t* _name_ | *-T*

	Mark a boot environment as the default root filesystem.

	_name_
		The boot environment to activate.

	*-t* _name_
		Activate this boot environment only for the next boot.

	*-T*
		Remove any temporary activations instead.

*create* [*-a* | *-t*] [*-d* _desc_] [*-o* _property=value_]
	\[*-e* _source_ | *--empty*] [*--use-os-release* _file_] _name_

	Create a new boot environment.

	*-a*
		Activate the new boot environment after creating it.

	*-t*
		Temporarily activate the new boot environment after creating it.

	*-d* _desc_
		Set a description for the new boot environment.

	*-e* _source_
		Create the new boot environment from this boot environment or snapshot,
		rather than the active one.

	*-o* _property=value_
		Set additional ZFS properties for the new boot environment. May be specified
		multiple times.

	*--empty*
		Create an empty boot environment instead of cloning another boot environment
		or snapshot.

	*--use-os-release* _file_
		Set a description for an empty boot environment using *PRETTY_NAME* from the
		specified _/etc/os-release_ file.

*snapshot* [*-d* _desc_] [_name_ | _name@snapshot_]

	Create a snapshot of a boot environment.

	_name_ | _name@snapshot_
		The boot environment and optional snapshot name (in the form _name_ or
		_name@snapshot_). When the snapshot name is omitted, one will be generated
		automatically from the current time.

	*-d* _desc_
		Set a description for the snapshot.

*destroy* [*-fs*] _name_ | _name@snapshot_

	Destroy an existing boot environment or snapshot.

	_name_ | _name@snapshot_
		The boot environment or snapshot to destroy (in the form _name_ or
		_name@snapshot_).

	*-f*
		Forcefully unmount the boot environment if needed.

	*-s*
		Destroy snapshots of the boot environment if needed.

*list* [*-adsH*] [*-k* _property_ | *-K* _property_] [_name_]

	List boot environments.

	_name_
		Include only this boot environment.

	*-a*
		Include subordinate filesystems and snapshots of each boot environment.

	*-d*
		Include subordinate filesystems of each boot environment.

	*-s*
		Include snapshots of each boot environment.

	*-H*
		Omit headers and formatting, separate fields by a single tab.

	*-k* _property_
		Sort boot environments by this property, ascending. Valid properties are:
		_name_, _date_, _space_. The default is _date_.

	*-K* _property_
		Sort boot environments by this property, descending.

*mount* [*-s* _mode_] _name_ [_mountpoint_]

	Mount a boot environment.

	_name_
		The boot environment to mount. The active boot environment cannot have its
		mountpoint changed.

	_mountpoint_
		A mount point. If omitted, creates a temporary mount in _/tmp_ and prints the
		path to stdout.

	*-s* _mode_
		Mount as read/write (_rw_) or read-only (_ro_). Default is _rw_.

*unmount* [*-f*] _name_

	Unmount an inactive boot environment.

	_name_
		The boot environment to unmount. The active boot environment cannot be
		unmounted.

	*-f*
		Force unmounting.

*rename* _name_ _new-name_

	Rename a boot environment.

	_name_
		The boot environment to rename.

	_new-name_
		The new name for the boot environment.

*rollback* _name_ _snapshot_

	Roll back a boot environment to an earlier snapshot.

	_name_
		The boot environment to roll back.

	_snapshot_
		The snapshot name to roll back to.

*hostid* _name_

	Get the host ID from a boot environment.

	_name_
		The boot environment to query.

*init* _pool_

	Create the ZFS dataset layout for boot environments.

	_pool_
		The ZFS pool to initialize.

*daemon*

	Start the boot environment D-Bus daemon.

# GLOBAL OPTIONS

*-r*, *--beroot* _dataset_
	Set the boot environment root dataset. This is the parent dataset of all boot
	environments. If not specified, *beadm* will attempt to auto-detect the root
	from the currently active boot environment.

*-v*, *--verbose*
	Enable verbose output.

*--client* _type_
	Select the client implementation to use. Valid values are:
	- _libzfs_: Use LibZFS directly (default)
	- _mock_: Use a mock/emulator client for testing
	- _dbus_: Use the D-Bus client (if compiled with D-Bus support)

# OUTPUT FORMAT

*beadm list* displays boot environments in a tabular format with the following
columns:

*NAME*
	The name of the boot environment or snapshot.

*ACTIVE*
	One or more flags for the activation status of the boot environment:

	- _R_: Active now (i.e. it is the current filesystem for _/_).
	- _N_: Active on reboot.
	- _T_: Active on reboot temporarily.

*MOUNTPOINT*
	The current mount point of the boot environment, if any.

*SPACE*
	The amount of space used by the boot environment.

*CREATED*
	The date and time when the boot environment was created.

*DESCRIPTION*
	The description of the boot environment, if any.

# EXAMPLES

Create a new boot environment named "backup":
	*beadm create backup*

Create and activate a new boot environment:
	*beadm create -a new-system*

List all boot environments:
	*beadm list*

List boot environments with snapshots:
	*beadm list -s*

Mount a boot environment to /mnt:
	*beadm mount backup /mnt*

Activate a boot environment for the next boot only:
	*beadm activate -t backup*

Destroy a boot environment:
	*beadm destroy old-system*

Create a snapshot of the current boot environment:
	*beadm snapshot*

Roll back to a previous snapshot:
	*beadm rollback current-be@yesterday*

# EXIT STATUS

*0*
	Success

*1*
	Failure

# SEE ALSO

*zfs*(8), *zpool*(8)

# HISTORY

The original *beadm* (and the accompanying *libbe*) first appeared in Solaris
10, with later improvements under the illumos project.

Slawomir Wojciech Wojtczak (vermaden) wrote a well-known *beadm* clone (in
Shell) for FreeBSD, where it was available as a port. FreeBSD 13 introduced its
successor *bectl* (rewritten in C by Kyle Kneitingerone and once again
accompanied by a *libbe*) as part of the base system.

There have been numerous cosmetic ports of vermaden's *beadm* to Linux, as well
as the *zectl* utility used by TrueNAS SCALE.

This version of *beadm* is an independent reimplementation targeting Linux.
Its commands and flags are largely compatible with the illumos variant of
*beadm* and FreeBSD's *bectl*, but it has a totally different architecture.

# AUTHORS

*beadm* was written by Aaron Jacobs <atheriel@gmail.com>.
