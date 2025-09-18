// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::ffi::{CStr, CString, OsStr, c_char, c_int, c_void};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{LazyLock, Mutex, MutexGuard};

use super::validation::{validate_component, validate_dataset_name};
use super::{BootEnvironment, Client, Error, Label, MountMode, Snapshot, generate_snapshot_name};

const DESCRIPTION_PROP: &str = "ca.kamacite:description";
const PREVIOUS_BOOTFS_PROP: &str = "ca.kamacite:previous-bootfs";

/// A ZFS boot environment client backed by libzfs.
pub struct LibZfsClient {
    root: DatasetName,
}

impl LibZfsClient {
    /// Create a new client with the specified boot environment root.
    pub fn new(root: DatasetName) -> Self {
        Self { root }
    }

    /// Get the filesystem (if any) that will be active on next boot for the
    /// pool backing the boot environment root.
    fn get_next_boot(&self, lzh: &LibHandle) -> Result<Option<DatasetName>, Error> {
        let zpool = Zpool::open(lzh, &self.root.pool())?;
        Ok(zpool.get_bootfs())
    }

    /// Get the filesystem (if any) that was previously active on next boot for
    /// the pool backing the boot environment root.
    fn get_previous_boot(&self, lzh: &LibHandle) -> Result<Option<DatasetName>, Error> {
        let zpool = Zpool::open(lzh, &self.root.pool())?;
        Ok(zpool.get_previous_bootfs())
    }
}

impl Client for LibZfsClient {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&Label>,
        _properties: &[String],
    ) -> Result<(), Error> {
        let be_path = self.root.append(be_name)?;
        let lzh = LibHandle::get();

        // Create properties nvlist with description if provided
        let props = if let Some(desc) = description {
            Some(NvList::from(&[(DESCRIPTION_PROP, desc)])?)
        } else {
            None
        };

        let snapshot = match source {
            Some(Label::Snapshot(name, snapshot)) => {
                // Case #1: beadm create -e EXISTING@SNAPSHOT NAME, which
                // creates the clone from an existing snapshot of a boot
                // environment.

                // Build the full snapshot path (which handles validation).
                let snapshot_path = self.root.append(name)?.snapshot(snapshot)?;

                // Open the snapshot (which also verifies it exists).
                Dataset::snapshot(&lzh, &snapshot_path).map_err(|err| {
                    // Special casing for EZFS_NOENT.
                    if let Error::LibzfsError(LibzfsError {
                        errno: ffi::EZFS_NOENT,
                        ..
                    }) = err
                    {
                        return Error::not_found(&format!("{}@{}", name, snapshot));
                    }
                    err
                })
            }
            Some(Label::Name(name)) => {
                // Case #2: beadm create -e EXISTING NAME, which creates the
                // clone from a new snapshot of a source boot environment.
                let snapshot_path = self.root.append(name)?.generate_snapshot()?;

                Dataset::create_snapshot(&lzh, &snapshot_path, props.as_ref()).map_err(|err| {
                    // Special casing for EZFS_NOENT.
                    if let Error::LibzfsError(LibzfsError {
                        errno: ffi::EZFS_NOENT,
                        ..
                    }) = err
                    {
                        return Error::not_found(name);
                    }
                    err
                })
            }
            None => {
                // Case #3: beadm create NAME, which creates the clone from a
                // snapshot of the active boot environment.
                let snapshot_path = get_rootfs()?
                    .ok_or_else(|| Error::NoActiveBootEnvironment)?
                    .generate_snapshot()?;

                Dataset::create_snapshot(&lzh, &snapshot_path, props.as_ref())
            }
        }?;

        let mut clone_props = NvList::from(&[("canmount", "noauto"), ("mountpoint", "/")])?;
        if let Some(desc) = description {
            clone_props.add_string(DESCRIPTION_PROP, desc)?;
        }

        // Clone the source snapshot to create the new boot environment.
        //
        // TODO: Investigate 'beadm' for whether we need to handle recursion.
        // In 'bectl' it is manually specified.
        snapshot
            .clone(&lzh, &be_path, Some(&clone_props))
            .map_err(|err| {
                // Special casing for EZFS_EEXIST.
                if let Error::LibzfsError(LibzfsError {
                    errno: ffi::EZFS_EEXIST,
                    ..
                }) = err
                {
                    return Error::conflict(be_name);
                }
                err
            })
    }

    fn create_empty(
        &self,
        be_name: &str,
        description: Option<&str>,
        _host_id: Option<&str>,
        _properties: &[String],
    ) -> Result<(), Error> {
        let mut props = NvList::from(&[("canmount", "noauto"), ("mountpoint", "/")])?;
        if let Some(desc) = description {
            props.add_string(DESCRIPTION_PROP, desc)?;
        }

        let be_path = self.root.append(be_name)?;
        let lzh = LibHandle::get();
        Dataset::create(&lzh, &be_path, &props).map_err(|err| {
            // Special casing for EZFS_EEXIST.
            if let Error::LibzfsError(LibzfsError {
                errno: ffi::EZFS_EEXIST,
                ..
            }) = err
            {
                return Error::conflict(be_name);
            }
            err
        })
    }

    fn destroy(&self, target: &Label, force_unmount: bool, _snapshots: bool) -> Result<(), Error> {
        let lzh = LibHandle::get();

        let dataset = match target {
            Label::Name(name) => {
                let path = self.root.append(name)?;
                let dataset = Dataset::boot_environment(&lzh, name, &path)?;

                // Cannot destroy the active, next, or boot once boot environment.
                if let Some(rootfs) = get_rootfs()? {
                    if path == rootfs {
                        return Err(Error::CannotDestroyActive {
                            name: name.to_string(),
                        });
                    }
                }
                if let Some(bootfs) = self.get_next_boot(&lzh)? {
                    if path == bootfs {
                        return Err(Error::CannotDestroyActive {
                            name: name.to_string(),
                        });
                    }
                }
                if let Some(bootfs) = self.get_previous_boot(&lzh)? {
                    if path == bootfs {
                        return Err(Error::CannotDestroyActive {
                            name: name.to_string(),
                        });
                    }
                }

                let mountpoint = dataset.get_mountpoint();
                if mountpoint.is_some() {
                    if !force_unmount {
                        return Err(Error::Mounted {
                            name: name.to_string(),
                            mountpoint: mountpoint.unwrap().display().to_string(),
                        });
                    } else {
                        // Best-effort attempt to unmount the dataset.
                        _ = dataset.unmount(&lzh, true);
                    }
                }

                dataset
            }
            Label::Snapshot(name, snapshot) => {
                let path = self.root.append(name)?.snapshot(snapshot)?;
                Dataset::snapshot(&lzh, &path)?
            }
        };

        dataset.destroy(&lzh)
    }

    fn mount(&self, be_name: &str, mountpoint: &str, _mode: MountMode) -> Result<(), Error> {
        let be_path = self.root.append(be_name)?;
        let lzh = LibHandle::get();
        let dataset = Dataset::boot_environment(&lzh, be_name, &be_path)?;

        // Check if it's already mounted. Otherwise zfs_mount_at() seems to
        // create a second mountpoint, which is not ideal.
        if let Some(existing) = dataset.get_mountpoint() {
            return Err(Error::mounted(be_name, &existing));
        }

        // TODO: Support recursively mounting child datasets.
        dataset.mount_at(&lzh, mountpoint)
    }

    fn unmount(&self, be_name: &str, force: bool) -> Result<Option<PathBuf>, Error> {
        let be_path = self.root.append(be_name)?;
        let lzh = LibHandle::get();
        let dataset = Dataset::boot_environment(&lzh, be_name, &be_path)?;

        // Get the mountpoint before unmounting
        let mountpoint = dataset.get_mountpoint();
        if mountpoint.is_none() {
            return Ok(None);
        }

        // TODO: Support recursively unmounting child datasets.
        dataset.unmount(&lzh, force)?;
        Ok(mountpoint)
    }

    fn hostid(&self, be_name: &str) -> Result<Option<u32>, Error> {
        let be_path = self.root.append(be_name)?;
        let lzh = LibHandle::get();
        let dataset = Dataset::boot_environment(&lzh, be_name, &be_path)?;
        if let Some(mountpoint) = dataset.get_mountpoint() {
            Ok(read_hostid(&mountpoint.join("etc/hostid")))
        } else {
            Err(Error::not_mounted(be_name))
        }
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error> {
        let be_path = self.root.append(be_name)?;
        let new_path = self.root.append(new_name)?;
        let lzh = LibHandle::get();
        let dataset = Dataset::boot_environment(&lzh, be_name, &be_path)?;
        dataset
            .rename(
                &lzh,
                &new_path,
                ffi::RenameFlags {
                    recursive: 0,
                    nounmount: 1, // Leave boot environment mounts in place.
                    forceunmount: 0,
                },
            )
            .map_err(|err| {
                // Special casing for EZFS_EEXIST.
                if let Error::LibzfsError(LibzfsError {
                    errno: ffi::EZFS_EEXIST,
                    ..
                }) = err
                {
                    return Error::conflict(new_name);
                }
                err
            })
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), Error> {
        let dataset = self.root.append(be_name)?;
        let lzh = LibHandle::get();
        Dataset::boot_environment(&lzh, be_name, &dataset)?; // Check existence.
        let zpool = Zpool::open(&lzh, &self.root.pool())?;

        if !temporary {
            // Unset any temporary activations *before* setting the new `bootfs`
            // value. That way we don't end up in an inconsistent state if
            // either operation fails.
            zpool.clear_previous_bootfs(&lzh)?;
        } else if zpool.get_previous_bootfs().is_none() {
            // For temporary activation, copy the current `bootfs` into the
            // `previous-bootfs` property before write the new `bootfs` value,
            // but *only* if there isn't a value already.
            let current_bootfs = zpool
                .get_bootfs()
                // TODO: We could potentially have a more useful error here.
                .ok_or_else(|| Error::NoActiveBootEnvironment)?;
            zpool.set_previous_bootfs(&lzh, &current_bootfs)?;
        }

        zpool.set_bootfs(&lzh, &dataset)
    }

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), Error> {
        let lzh = LibHandle::get();
        let be_path = self.root.append(be_name)?;
        let be_dataset = Dataset::filesystem(&lzh, &be_path)?;
        let snap_path = self.root.snapshot(snapshot)?;
        let snap_dataset = Dataset::snapshot(&lzh, &snap_path)?;
        be_dataset.rollback_to(&lzh, &snap_dataset)
    }

    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error> {
        let lzh = LibHandle::get();
        let root_dataset = Dataset::filesystem(&lzh, &self.root)?;
        let rootfs = get_rootfs()?;
        let bootfs = self.get_next_boot(&lzh)?;
        let previous_bootfs = self.get_previous_boot(&lzh)?;
        let mut bes = Vec::new();
        root_dataset.iter_children(&lzh, |dataset| {
            let path = match dataset.get_name() {
                Some(name) => name,
                None => return Ok(()), // Skip this iteration
            };
            let active = rootfs.as_ref().map_or(false, |fs| *fs == path);
            let next_boot = if let Some(prev) = previous_bootfs.as_ref() {
                // There is a temporary activation.
                *prev == path
            } else {
                // There is no temporary activation.
                bootfs.as_ref().map_or(false, |fs| *fs == path)
            };
            let boot_once = if previous_bootfs.is_some() {
                bootfs.as_ref().map_or(false, |fs| *fs == path)
            } else {
                false
            };

            bes.push(BootEnvironment {
                name: path.basename(),
                path: path.to_string(),
                guid: dataset.get_guid(),
                description: dataset.get_user_property(DESCRIPTION_PROP),
                mountpoint: dataset.get_mountpoint(),
                active,
                next_boot,
                boot_once,
                space: dataset.get_used_space(),
                created: dataset.get_creation_time(),
            });
            Ok(())
        })?;
        Ok(bes)
    }

    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, Error> {
        let be_path = self.root.append(be_name)?;
        let lzh = LibHandle::get();
        let dataset = Dataset::filesystem(&lzh, &be_path)?;
        let mut snapshots = Vec::new();
        dataset.iter_snapshots(&lzh, |snapshot| {
            if let Some(path) = snapshot.get_name() {
                snapshots.push(Snapshot {
                    name: path.basename(),
                    path: path.to_string(),
                    description: snapshot.get_user_property(DESCRIPTION_PROP),
                    space: snapshot.get_used_space(),
                    created: snapshot.get_creation_time(),
                });
            }
            Ok(())
        })?;
        Ok(snapshots)
    }

    fn snapshot(&self, source: Option<&Label>, description: Option<&str>) -> Result<String, Error> {
        let snapshot_path = match source {
            Some(label) => match label {
                Label::Name(name) => self.root.append(name)?.generate_snapshot(),
                Label::Snapshot(name, snapshot) => self.root.append(name)?.snapshot(snapshot),
            },
            None => {
                // Snapshot the active boot environment with auto-generated name
                get_rootfs()?
                    .ok_or_else(|| Error::NoActiveBootEnvironment)?
                    .generate_snapshot()
            }
        }?;

        // Pass the description (if provided) as a snapshot property.
        let props = if let Some(desc) = description {
            Some(NvList::from(&[(DESCRIPTION_PROP, desc)])?)
        } else {
            None
        };

        let lzh = LibHandle::get();
        Dataset::create_snapshot(&lzh, &snapshot_path, props.as_ref()).map_err(|err| {
            // Special casing for EZFS_NOENT.
            if let Error::LibzfsError(LibzfsError {
                errno: ffi::EZFS_NOENT,
                ..
            }) = err
            {
                return Error::not_found(&snapshot_path.basename());
            }
            err
        })?;

        Ok(snapshot_path.basename())
    }

    fn clear_boot_once(&self) -> Result<(), Error> {
        let lzh = LibHandle::get();
        let zpool = Zpool::open(&lzh, &self.root.pool())?;

        // Get the previous bootfs value
        let previous_bootfs = match zpool.get_previous_bootfs() {
            Some(value) => value,
            None => return Ok(()), // Nothing to clear.
        };

        // Set the bootfs back to the previous value.
        zpool.set_bootfs(&lzh, &previous_bootfs)?;

        // Clear the temporary activation.
        zpool.clear_previous_bootfs(&lzh)
    }

    fn init(&self, pool: &str) -> Result<(), Error> {
        let lzh = LibHandle::get();
        let pool_dataset = DatasetName::new(pool)?;
        let _ = Zpool::open(&lzh, &pool_dataset)?; // Ensure the pool exists.

        // Initialize the ROOT dataset to hold boot environments.
        let root_dataset = pool_dataset.append("ROOT").unwrap();
        match Dataset::filesystem(&lzh, &root_dataset) {
            Ok(dataset) => {
                // Verify that mountpoint=none.
                if let Some(mountpoint) = dataset.get_mountpoint_property() {
                    if mountpoint != "none" {
                        return Err(Error::InvalidBootEnvironmentRoot {
                            name: root_dataset.to_string(),
                        });
                    }
                }
            }
            Err(Error::LibzfsError(LibzfsError {
                errno: ffi::EZFS_NOENT,
                ..
            })) => {
                // Create it.
                let props = NvList::from(&[("mountpoint", "none")])?;
                Dataset::create(&lzh, &root_dataset, &props)?;
            }
            Err(e) => return Err(e),
        }

        // Initialize the home dataset to hold home directories.
        let home_dataset = pool_dataset.append("home").unwrap();
        match Dataset::filesystem(&lzh, &home_dataset) {
            Ok(dataset) => {
                // Verify that mountpoint=/home.
                if let Some(mountpoint) = dataset.get_mountpoint_property() {
                    if mountpoint != "/home" {
                        return Err(Error::invalid_prop("mountpoint", &mountpoint));
                    }
                }
            }
            Err(Error::LibzfsError(LibzfsError {
                errno: ffi::EZFS_NOENT,
                ..
            })) => {
                // Create it.
                let props = NvList::from(&[("mountpoint", "/home")])?;
                Dataset::create(&lzh, &home_dataset, &props)?;
            }
            Err(e) => return Err(e),
        }

        Ok(())
    }

    fn describe(&self, target: &Label, description: &str) -> Result<(), Error> {
        let lzh = LibHandle::get();
        let dataset = match target {
            Label::Snapshot(name, snapshot) => {
                let dataset_path = self.root.append(name)?.snapshot(snapshot)?;
                Dataset::snapshot(&lzh, &dataset_path).map_err(|err| {
                    if let Error::LibzfsError(LibzfsError {
                        errno: ffi::EZFS_NOENT,
                        ..
                    }) = err
                    {
                        return Error::not_found(&format!("{}", target));
                    }
                    err
                })?
            }
            Label::Name(name) => {
                let dataset_path = self.root.append(name)?;
                Dataset::boot_environment(&lzh, name, &dataset_path)?
            }
        };
        dataset.set_property(&lzh, DESCRIPTION_PROP, description)
    }
}

/// Safe wrapper for various operations on a ZFS dataset handle.
struct Dataset {
    handle: *mut ffi::ZfsHandle,
    owns_handle: bool,
}

impl Dataset {
    /// Open a ZFS dataset with the given name and type.
    pub fn open(lzh: &LibHandle, name: &DatasetName, zfs_type: c_int) -> Result<Self, Error> {
        let handle = unsafe { ffi::zfs_open(lzh.as_ptr(), name.as_ptr(), zfs_type) };
        if handle.is_null() {
            return Err(lzh.libzfs_error().into());
        }
        Ok(Dataset {
            handle,
            owns_handle: true,
        })
    }

    // Open a filesystem dataset.
    pub fn filesystem(lzh: &LibHandle, name: &DatasetName) -> Result<Self, Error> {
        Dataset::open(lzh, name, ffi::ZFS_TYPE_FILESYSTEM)
    }

    // Open a snapshot dataset.
    pub fn snapshot(lzh: &LibHandle, name: &DatasetName) -> Result<Self, Error> {
        Dataset::open(lzh, name, ffi::ZFS_TYPE_SNAPSHOT)
    }

    // Open a filesystem dataset corresponding to a boot environment of the
    // given name.
    pub fn boot_environment(
        lzh: &LibHandle,
        be_name: &str,
        path: &DatasetName,
    ) -> Result<Self, Error> {
        Dataset::filesystem(lzh, path).map_err(|err| {
            // Special casing for EZFS_NOENT.
            if let Error::LibzfsError(LibzfsError {
                errno: ffi::EZFS_NOENT,
                ..
            }) = err
            {
                return Error::not_found(be_name);
            }
            err
        })
    }

    /// Create a Dataset from an existing handle. Closing the handle is the
    /// responsibility of the caller.
    pub fn borrowed(handle: *mut ffi::ZfsHandle) -> Self {
        Dataset {
            handle,
            owns_handle: false,
        }
    }

    /// Create a new ZFS filesystem.
    pub fn create(lzh: &LibHandle, name: &DatasetName, properties: &NvList) -> Result<(), Error> {
        let result = unsafe {
            ffi::zfs_create(
                lzh.as_ptr(),
                name.as_ptr(),
                ffi::ZFS_TYPE_FILESYSTEM,
                properties.as_ptr(),
            )
        };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }

    /// Create a snapshot of a dataset.
    pub fn create_snapshot(
        lzh: &LibHandle,
        snapshot_path: &DatasetName,
        properties: Option<&NvList>,
    ) -> Result<Dataset, Error> {
        let props_ptr = properties.map_or(ptr::null_mut(), |p| p.as_nvlist_ptr());
        let result = unsafe {
            ffi::zfs_snapshot(
                lzh.as_ptr(),
                snapshot_path.as_ptr(),
                0, // recursive = false (boolean_t)
                props_ptr,
            )
        };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Dataset::snapshot(lzh, snapshot_path)
    }

    /// Get the dataset name.
    pub fn get_name(&self) -> Option<DatasetName> {
        let name_ptr = unsafe { ffi::zfs_get_name(self.handle) };
        if name_ptr.is_null() {
            // The libzfs API claims this is not possible.
            return None;
        }
        let cstr = unsafe { CStr::from_ptr(name_ptr) };
        DatasetName::new(&cstr.to_string_lossy()).ok()
    }

    /// Get the dataset's current mountpoint if it is mounted.
    pub fn get_mountpoint(&self) -> Option<PathBuf> {
        let mut mountpoint_ptr: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe {
            ffi::zfs_is_mounted(
                self.handle,
                &mut mountpoint_ptr as *mut *mut std::os::raw::c_char,
            )
        };
        if result != 0 && !mountpoint_ptr.is_null() {
            let cstr = unsafe { CStr::from_ptr(mountpoint_ptr) };
            let path = Path::new(OsStr::from_bytes(cstr.to_bytes()));
            Some(path.to_path_buf())
        } else {
            None
        }
    }

    /// Get the space used by this dataset.
    pub fn get_used_space(&self) -> u64 {
        self.get_numeric_property(ffi::ZFS_PROP_USED).unwrap_or(0)
    }

    /// Get the creation timestamp for this dataset.
    pub fn get_creation_time(&self) -> i64 {
        self.get_numeric_property(ffi::ZFS_PROP_CREATION)
            .unwrap_or(0) as i64
    }

    /// Get the GUID of this dataset.
    pub fn get_guid(&self) -> u64 {
        self.get_numeric_property(ffi::ZFS_PROP_GUID).unwrap_or(0)
    }

    // Rename this dataset.
    pub fn rename(
        &self,
        lzh: &LibHandle,
        new_name: &DatasetName,
        flags: ffi::RenameFlags,
    ) -> Result<(), Error> {
        let result = unsafe { ffi::zfs_rename(self.handle, new_name.as_ptr(), flags) };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }

    /// Destroy this dataset.
    pub fn destroy(&self, lzh: &LibHandle) -> Result<(), Error> {
        let result = unsafe { ffi::zfs_destroy(self.handle, 0) };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }

    /// Unmount this dataset with optional force flag.
    pub fn unmount(&self, lzh: &LibHandle, force: bool) -> Result<(), Error> {
        let flags = if force { 1 } else { 0 };
        let result = unsafe { ffi::zfs_unmount(self.handle, ptr::null(), flags) };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }

    /// Mount this dataset at the specified path.
    pub fn mount_at(&self, lzh: &LibHandle, mountpoint: &str) -> Result<(), Error> {
        let c_mountpoint = CString::new(mountpoint).map_err(|_| Error::InvalidPath {
            path: mountpoint.to_string(),
        })?;
        let result =
            unsafe { ffi::zfs_mount_at(self.handle, ptr::null(), 0, c_mountpoint.as_ptr()) };
        if result != 0 {
            // TODO: zfs_mount_at() sets regular ELOOP, ENOENT, ENOTDIR, EPERM,
            // EBUSY via errno. We should convert these to the relevant errors
            // rather than this generic one.
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }

    /// Rollback this dataset to the specified snapshot.
    pub fn rollback_to(&self, lzh: &LibHandle, snapshot: &Dataset) -> Result<(), Error> {
        let result = unsafe { ffi::zfs_rollback(self.handle, snapshot.handle, 0) };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }

    /// Iterate over the snapshots of this dataset.
    pub fn iter_snapshots<F>(&self, lzh: &LibHandle, callback: F) -> Result<(), Error>
    where
        F: FnMut(&Dataset) -> Result<(), Error>,
    {
        let mut data = IterData::from(callback);
        let result = unsafe {
            ffi::zfs_iter_snapshots(
                self.handle,
                0, // simple = false for recursive iteration
                iter_callback::<F>,
                data.as_mut_ptr(),
                0,        // min_txg = 0 (no minimum)
                u64::MAX, // max_txg = max (no maximum)
            )
        };

        // Check if the callback set an error.
        if let Some(error) = data.error {
            return Err(error);
        }

        // Check for iteration failures.
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }

        Ok(())
    }

    /// Iterate over child datasets.
    pub fn iter_children<F>(&self, lzh: &LibHandle, callback: F) -> Result<(), Error>
    where
        F: FnMut(&Dataset) -> Result<(), Error>,
    {
        let mut data = IterData::from(callback);
        let result =
            unsafe { ffi::zfs_iter_children(self.handle, iter_callback::<F>, data.as_mut_ptr()) };

        // Check if the callback set an error.
        if let Some(error) = data.error {
            return Err(error);
        }

        // Check for iteration failures.
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }

        Ok(())
    }

    /// Iterate over the clones of this dataset.
    pub fn iter_clones<F>(
        &self,
        lzh: &LibHandle,
        allow_recursion: bool,
        callback: F,
    ) -> Result<(), Error>
    where
        F: FnMut(&Dataset) -> Result<(), Error>,
    {
        let mut data = IterData::from(callback);
        let result = unsafe {
            ffi::zfs_iter_dependents(
                self.handle,
                if allow_recursion { 1 } else { 0 },
                iter_callback::<F>,
                data.as_mut_ptr(),
            )
        };

        // Check if the callback set an error.
        if let Some(error) = data.error {
            return Err(error);
        }

        // Check for iteration failures.
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }

        Ok(())
    }

    /// Get the parent dataset.
    pub fn parent(&self, lzh: &LibHandle) -> Option<Dataset> {
        if let Some(Some(name)) = self.get_name().map(|name| name.parent()) {
            Dataset::filesystem(lzh, &name).ok()
        } else {
            None
        }
    }

    /// Get the canmount property of this dataset.
    pub fn get_canmount(&self) -> Option<String> {
        self.get_property(ffi::ZFS_PROP_CANMOUNT)
    }

    /// Get the mountpoint property of this dataset.
    pub fn get_mountpoint_property(&self) -> Option<String> {
        self.get_property(ffi::ZFS_PROP_MOUNTPOINT)
    }

    /// Get a ZFS property for this dataset.
    fn get_property(&self, prop: c_int) -> Option<String> {
        const PROP_BUF_SIZE: usize = 1024;
        let mut buf = vec![0u8; PROP_BUF_SIZE];
        let result = unsafe {
            ffi::zfs_prop_get(
                self.handle,
                prop,
                buf.as_mut_ptr() as *mut std::os::raw::c_char,
                PROP_BUF_SIZE,
                ptr::null_mut(),
                0,
            )
        };
        if result == 0 {
            if let Some(null_pos) = buf.iter().position(|&x| x == 0) {
                buf.truncate(null_pos);
            }
            String::from_utf8(buf).ok()
        } else {
            None
        }
    }

    /// Get a numeric ZFS property for this dataset.
    fn get_numeric_property(&self, prop: c_int) -> Option<u64> {
        let mut value: u64 = 0;
        let result = unsafe {
            ffi::zfs_prop_get_numeric(
                self.handle,
                prop,
                &mut value as *mut u64,
                ptr::null_mut(),
                ptr::null_mut(),
                0,
            )
        };
        if result == 0 { Some(value) } else { None }
    }

    /// Get a specific ZFS user property for this dataset.
    fn get_user_property(&self, prop_name: &str) -> Option<String> {
        let prop_cstr = match CString::new(prop_name) {
            Ok(cstr) => cstr,
            Err(_) => return None,
        };

        let user_props = unsafe { ffi::zfs_get_user_props(self.handle) };
        if user_props.is_null() {
            // This should never happen.
            return None;
        }

        // User properties are stored as an nvlist of nvlists.
        let mut prop_nvlist_ptr: *mut ffi::NvList = ptr::null_mut();
        let result = unsafe {
            ffi::nvlist_lookup_nvlist(
                user_props,
                prop_cstr.as_ptr(),
                &mut prop_nvlist_ptr as *mut *mut ffi::NvList,
            )
        };

        if result != 0 || prop_nvlist_ptr.is_null() {
            // No entry for this user property.
            return None;
        }

        // The property is stored under the aptly-named "value" name.
        let value_cstr = CString::new("value").unwrap();
        let mut value_ptr: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe {
            ffi::nvlist_lookup_string(
                prop_nvlist_ptr,
                value_cstr.as_ptr(),
                &mut value_ptr as *mut *mut std::os::raw::c_char,
            )
        };

        if result != 0 || value_ptr.is_null() {
            // This should never happen.
            return None;
        }

        let cstr = unsafe { CStr::from_ptr(value_ptr) };
        let value_str = cstr.to_string_lossy().to_string();
        // TODO: Can this '-' ever be a legitimate value?
        if !value_str.is_empty() && value_str != "-" {
            Some(value_str)
        } else {
            None
        }
    }

    /// Set a ZFS property for this dataset.
    fn set_property(&self, lzh: &LibHandle, prop_name: &str, value: &str) -> Result<(), Error> {
        let prop_cstr =
            CString::new(prop_name).map_err(|_| Error::invalid_prop(prop_name, value))?;
        let value_cstr = CString::new(value).map_err(|_| Error::invalid_prop(prop_name, value))?;
        let result =
            unsafe { ffi::zfs_prop_set(self.handle, prop_cstr.as_ptr(), value_cstr.as_ptr()) };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }

    /// Clone a dataset from an existing snapshot.
    pub fn clone(
        &self,
        lzh: &LibHandle,
        name: &DatasetName,
        properties: Option<&NvList>,
    ) -> Result<(), Error> {
        let props_ptr = properties.map_or(ptr::null_mut(), |p| p.as_nvlist_ptr());
        let result = unsafe { ffi::zfs_clone(self.handle, name.as_ptr(), props_ptr) };
        if result != 0 {
            return Err(lzh.libzfs_error().into());
        }
        Ok(())
    }
}

impl Drop for Dataset {
    fn drop(&mut self) {
        if !self.owns_handle || self.handle.is_null() {
            return;
        }
        unsafe {
            ffi::zfs_close(self.handle);
        }
    }
}

/// Helper struct to pass both a closure and error state to libzfs iterator
/// callbacks.
struct IterData<F> {
    callback: F,
    error: Option<Error>,
}

impl<F> IterData<F>
where
    F: FnMut(&Dataset) -> Result<(), Error>,
{
    pub fn from(callback: F) -> Self {
        IterData {
            callback,
            error: None,
        }
    }

    pub fn as_mut_ptr(&mut self) -> *mut c_void {
        self as *mut IterData<F> as *mut c_void
    }
}

/// C-style callback that can be passed to libzfs iterator functions.
///
/// SAFETY: This function assumes that the data is valid IterData.
extern "C" fn iter_callback<F>(
    zhp: *mut ffi::ZfsHandle,
    data: *mut std::os::raw::c_void,
) -> std::os::raw::c_int
where
    F: FnMut(&Dataset) -> Result<(), Error>,
{
    let iter_data = unsafe { &mut *(data as *mut IterData<F>) };
    let dataset = Dataset::borrowed(zhp);

    match (iter_data.callback)(&dataset) {
        Ok(()) => 0, // Continue iteration
        Err(e) => {
            iter_data.error = Some(e);
            1 // Stop iteration
        }
    }
}

/// Safe wrapper for zpool operations.
struct Zpool {
    handle: *mut ffi::ZpoolHandle,
}

impl Zpool {
    /// Open a zpool by name.
    pub fn open(lzh: &LibHandle, name: &DatasetName) -> Result<Self, Error> {
        let handle = unsafe { ffi::zpool_open(lzh.as_ptr(), name.as_ptr()) };
        if handle.is_null() {
            return Err(lzh.libzfs_error().into());
        }
        Ok(Zpool { handle })
    }

    /// Get a zpool property.
    pub fn get_property(&self, prop: c_int) -> Option<String> {
        const PROP_BUF_SIZE: usize = 1024;
        let mut buf = vec![0u8; PROP_BUF_SIZE];
        let result = unsafe {
            ffi::zpool_get_prop(
                self.handle,
                prop,
                buf.as_mut_ptr() as *mut std::os::raw::c_char,
                PROP_BUF_SIZE,
                ptr::null_mut(),
                0,
            )
        };
        if result == 0 {
            if let Some(null_pos) = buf.iter().position(|&x| x == 0) {
                buf.truncate(null_pos);
            }
            String::from_utf8(buf).ok()
        } else {
            None
        }
    }

    /// Get the bootfs property (which dataset boots by default).
    pub fn get_bootfs(&self) -> Option<DatasetName> {
        match self.get_property(ffi::ZPOOL_PROP_BOOTFS) {
            Some(fs) => DatasetName::new(&fs).map_or(None, |ds| Some(ds)),
            None => None,
        }
    }

    /// Set the bootfs property (which dataset boots by default).
    pub fn set_bootfs(&self, lzh: &LibHandle, dataset: &DatasetName) -> Result<(), Error> {
        let prop = CString::new("bootfs").unwrap();
        let result = unsafe { ffi::zpool_set_prop(self.handle, prop.as_ptr(), dataset.as_ptr()) };
        if result != 0 {
            Err(lzh.libzfs_error().into())
        } else {
            Ok(())
        }
    }

    /// Get the "previous bootfs" property (used for temporary activation).
    pub fn get_previous_bootfs(&self) -> Option<DatasetName> {
        let prop = CString::new(PREVIOUS_BOOTFS_PROP).unwrap();
        const PROP_BUF_SIZE: usize = 1024;
        let mut buf = vec![0u8; PROP_BUF_SIZE];
        let result = unsafe {
            ffi::zpool_get_userprop(
                self.handle,
                prop.as_ptr(),
                buf.as_mut_ptr() as *mut std::os::raw::c_char,
                PROP_BUF_SIZE,
                ptr::null_mut(), // Don't need source info
            )
        };
        if result == 0 {
            if let Some(null_pos) = buf.iter().position(|&x| x == 0) {
                buf.truncate(null_pos);
            }
            if let Ok(value) = String::from_utf8(buf) {
                if !value.is_empty() && value != "-" {
                    DatasetName::new(&value).ok()
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Set the "previous bootfs" property (used for temporary activation).
    pub fn set_previous_bootfs(&self, lzh: &LibHandle, dataset: &DatasetName) -> Result<(), Error> {
        let prop = CString::new(PREVIOUS_BOOTFS_PROP).unwrap();
        let result = unsafe { ffi::zpool_set_prop(self.handle, prop.as_ptr(), dataset.as_ptr()) };
        if result != 0 {
            Err(lzh.libzfs_error().into())
        } else {
            Ok(())
        }
    }

    /// Clear the "previous bootfs" property (used for temporary activation).
    pub fn clear_previous_bootfs(&self, lzh: &LibHandle) -> Result<(), Error> {
        let prop = CString::new(PREVIOUS_BOOTFS_PROP).unwrap();
        let empty_value = CString::new("").unwrap();
        let result =
            unsafe { ffi::zpool_set_prop(self.handle, prop.as_ptr(), empty_value.as_ptr()) };
        if result != 0 {
            Err(lzh.libzfs_error().into())
        } else {
            Ok(())
        }
    }
}

impl Drop for Zpool {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                ffi::zpool_close(self.handle);
            }
        }
    }
}

// Convenience type for already-validated ZFS dataset names that can be passed
// directly to the FFI layer.
#[derive(Debug, PartialEq, Eq)]
pub struct DatasetName {
    inner: CString,
}

impl DatasetName {
    pub fn new(name: &str) -> Result<Self, Error> {
        validate_dataset_name(name)?;
        Ok(Self {
            inner: CString::new(name).unwrap(),
        })
    }

    pub fn append(&self, child: &str) -> Result<Self, Error> {
        validate_component(child, true)?;
        let mut v = Vec::from(self.inner.to_bytes());
        v.push('/' as u8);
        v.append(&mut Vec::from(child));
        // We know all components are valid at this point, it is safe to skip
        // UTF-8 and nul-byte checks.
        unsafe { Self::from_vec_unchecked(v) }
    }

    pub fn snapshot(&self, name: &str) -> Result<Self, Error> {
        validate_component(name, false)?;
        let mut v = Vec::from(self.inner.to_bytes());
        v.push('@' as u8);
        v.append(&mut Vec::from(name));
        // We know all components are valid at this point, it is safe to skip
        // UTF-8 and nul-byte checks.
        unsafe { Self::from_vec_unchecked(v) }
    }

    /// Generate a snapshot with an auto-generated timestamp-based name.
    pub fn generate_snapshot(&self) -> Result<Self, Error> {
        let snapshot_name = generate_snapshot_name();
        self.snapshot(&snapshot_name)
    }

    // Create a dataset name from a byte vector that is known to be (1) UTF-8;
    // and (2) contain no nul bytes.
    unsafe fn from_vec_unchecked(v: Vec<u8>) -> Result<Self, Error> {
        unsafe {
            // Don't trust the caller to have done end-to-end validation.
            validate_dataset_name(str::from_utf8_unchecked(&v))?;
            Ok(Self {
                inner: CString::from_vec_unchecked(v),
            })
        }
    }

    pub fn as_ptr(&self) -> *const c_char {
        self.inner.as_ptr()
    }

    pub fn to_string(&self) -> String {
        // We can safely unwrap here because dataset names are valid UTF-8.
        self.inner.to_str().unwrap().to_string()
    }

    /// Get the pool name (the first component) for the dataset.
    pub fn pool(&self) -> Self {
        let mut v = Vec::from(self.inner.to_bytes());
        for (i, b) in v.iter().enumerate() {
            if *b == ('/' as u8) {
                v.truncate(i);
                break;
            }
        }
        Self {
            // We know there are no nul bytes in either component at this
            // point, so this is safe.
            inner: unsafe { CString::from_vec_unchecked(v) },
        }
    }

    /// Get the "basename" for the dataset, e.g. for `zfs/ROOT/be@snapshot`
    /// this is `be@snapshot`.
    pub fn basename(&self) -> String {
        let name = self.to_string();
        if let Some(slash_pos) = name.rfind('/') {
            name[slash_pos + 1..].to_string()
        } else {
            name
        }
    }

    /// Get the parent of this dataset.
    pub fn parent(&self) -> Option<DatasetName> {
        let name = self.to_string();
        if let Some(index) = name.rfind('/') {
            // This is safe to unwrap() because we've already validated it.
            Some(DatasetName::new(&name[..index]).unwrap())
        } else {
            // No parent (this is a pool root dataset).
            None
        }
    }
}

// Wraps the libzfs handle to manage its lifetime.
struct LibHandle {
    handle: ptr::NonNull<ffi::LibzfsHandle>,
}

impl LibHandle {
    /// Get a guarded reference to the underlying `libzfs` handle.
    ///
    /// ## Deadlocks
    ///
    /// This function may cause a deadlock if called from a thread that is
    /// already holding the lock. Use with caution.
    ///
    /// ## Panics
    ///
    /// Panics if `libzfs` cannot be initialized, if the lock is poisoned, or
    /// (maybe) if the handle is already locked by the current thread.
    pub fn get() -> MutexGuard<'static, Self> {
        // TODO: This would be a hell of a lot safer if ReentrantLock was stable.
        static LZH: LazyLock<Mutex<LibHandle>> = LazyLock::new(|| {
            let handle = unsafe { ffi::libzfs_init() };
            if handle.is_null() {
                panic!("Failed to initialize libzfs");
            }
            Mutex::new(LibHandle {
                handle: unsafe { ptr::NonNull::new_unchecked(handle) },
            })
        });
        LZH.lock().expect("Failed to acquire libzfs handle")
    }

    /// Get the current libzfs error.
    pub fn libzfs_error(&self) -> LibzfsError {
        let errno = unsafe { ffi::libzfs_errno(self.handle.as_ptr()) };
        let desc_ptr = unsafe { ffi::libzfs_error_description(self.handle.as_ptr()) };
        let description = if desc_ptr.is_null() {
            // This should never happen (tm).
            "unknown".to_string()
        } else {
            let cstr = unsafe { CStr::from_ptr(desc_ptr) };
            cstr.to_string_lossy().to_string()
        };
        LibzfsError { errno, description }
    }

    // Get the underlying libzfs handle as a raw pointer.
    pub fn as_ptr(&self) -> *mut ffi::LibzfsHandle {
        self.handle.as_ptr()
    }
}

impl Drop for LibHandle {
    fn drop(&mut self) {
        // This is here for completeness; it's never called when accessing the
        // handle through the LazyLock.
        unsafe {
            ffi::libzfs_fini(self.handle.as_ptr());
        }
    }
}

// SAFETY: Since we have complete control over the lifetime of the underlying
// libzfs handle and serialize access to it, it is safe to send LibHandle
// across threads.
unsafe impl Send for LibHandle {}

/// Surfaces errors from the underlying libzfs library.
#[derive(Debug)]
pub struct LibzfsError {
    pub errno: i32,
    pub description: String,
}

impl std::fmt::Display for LibzfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl std::error::Error for LibzfsError {}

/// Wraps an nvlist to manage its lifetime.
struct NvList {
    nvl: *mut ffi::NvList,
}

impl NvList {
    pub fn new() -> Result<Self, Error> {
        let mut nvl: *mut ffi::NvList = ptr::null_mut();
        let result =
            unsafe { ffi::nvlist_alloc(&mut nvl as *mut *mut ffi::NvList, ffi::NV_UNIQUE_NAME, 0) };
        if result != 0 {
            return Err(std::io::Error::from_raw_os_error(result).into());
        }
        if nvl.is_null() {
            let err: std::io::Error = std::io::ErrorKind::OutOfMemory.into();
            return Err(err.into());
        }
        Ok(NvList { nvl })
    }

    pub fn from(pairs: &[(&str, &str)]) -> Result<Self, Error> {
        let mut nvl = Self::new()?;
        for (key, value) in pairs {
            nvl.add_string(key, value)?;
        }
        Ok(nvl)
    }

    pub fn add_string(&mut self, name: &str, value: &str) -> Result<(), Error> {
        let name_cstr = CString::new(name).map_err(|_| Error::invalid_prop(name, value))?;
        let value_cstr = CString::new(value).map_err(|_| Error::invalid_prop(name, value))?;
        let result =
            unsafe { ffi::nvlist_add_string(self.nvl, name_cstr.as_ptr(), value_cstr.as_ptr()) };
        if result != 0 {
            return Err(std::io::Error::from_raw_os_error(result).into());
        }
        Ok(())
    }

    fn as_ptr(&self) -> *mut c_void {
        self.nvl as *mut c_void
    }

    pub fn as_nvlist_ptr(&self) -> *mut ffi::NvList {
        self.nvl
    }
}

impl Drop for NvList {
    fn drop(&mut self) {
        unsafe { ffi::nvlist_free(self.nvl) };
    }
}

/// Format a byte count using the same method as the standard ZFS CLI tools.
pub fn format_zfs_bytes(bytes: u64) -> String {
    // zfs_nicebytes is guaranteed to return something that fits in five bytes.
    const BUF_SIZE: usize = 6;
    let mut buf = vec![0u8; BUF_SIZE];
    unsafe {
        ffi::zfs_nicebytes(
            bytes,
            buf.as_mut_ptr() as *mut std::os::raw::c_char,
            BUF_SIZE,
        );
    }
    // Truncate at the null terminator.
    if let Some(null_pos) = buf.iter().position(|&x| x == 0) {
        buf.truncate(null_pos);
    }
    String::from_utf8(buf).unwrap_or("-".to_string())
}

/// Read a host ID, usually from `/etc/hostid`.
pub fn read_hostid(path: &Path) -> Option<u32> {
    let hostid_bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(_) => return None,
    };
    if hostid_bytes.len() != 4 {
        return None;
    }
    let hostid = u32::from_le_bytes([
        hostid_bytes[0],
        hostid_bytes[1],
        hostid_bytes[2],
        hostid_bytes[3],
    ]);
    Some(hostid)
}

/// Get the root ZFS filesystem, if any, from `/proc/mounts`.
fn get_rootfs() -> Result<Option<DatasetName>, Error> {
    let file = File::open("/proc/mounts")?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }
        let device = parts[0];
        let mountpoint = parts[1];
        let fstype = parts[2];

        if mountpoint == "/" && fstype != "zfs" {
            // Not running ZFS-on-root at all.
            break;
        }

        if mountpoint == "/" && fstype == "zfs" {
            return DatasetName::new(device).map(|ds| Some(ds));
        }
    }
    Ok(None)
}

// Gets the parent dataset of the active boot environment, provided it exists
// and looks valid.
pub fn get_active_boot_environment_root() -> Result<DatasetName, Error> {
    // We're looking for a ZFS filesystem layout that looks like the
    // following:
    //
    // 1. A dataset like zroot/ROOT/default mounted at '/' with the
    //    canmount=noauto property set.
    // 2. The parent dataset with mountpoint=none.
    //
    // This parent dataset is the boot environment root.
    let rootfs = match get_rootfs()? {
        Some(fs) => fs,
        None => return Err(Error::NoActiveBootEnvironment),
    };
    let parent = match rootfs.parent() {
        Some(ds) => ds,
        None => return Err(Error::NoActiveBootEnvironment),
    };

    // Check if we have the expected canmount/mountpoint setup.
    let lzh = LibHandle::get();
    let rootfs_ds = Dataset::filesystem(&lzh, &rootfs)?;
    if rootfs_ds.get_canmount() != Some("noauto".to_string()) {
        return Err(Error::invalid_root(&parent.to_string()));
    }
    let parent_ds = Dataset::filesystem(&lzh, &parent)?;
    if parent_ds.get_mountpoint_property() != Some("none".to_string()) {
        return Err(Error::invalid_root(&parent.to_string()));
    }

    Ok(parent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_name_pool() {
        let name = DatasetName::new("rpool/ROOT/default").unwrap();
        assert_eq!(name.pool().to_string(), "rpool");

        let name = DatasetName::new("tank/data/projects").unwrap();
        assert_eq!(name.pool().to_string(), "tank");

        let name = DatasetName::new("simple").unwrap();
        assert_eq!(name.pool().to_string(), "simple");
    }

    #[test]
    fn test_dataset_name_basename() {
        assert_eq!(DatasetName::new("pool").unwrap().basename(), "pool");
        assert_eq!(
            DatasetName::new("rpool/ROOT/default").unwrap().basename(),
            "default"
        );
        assert_eq!(
            DatasetName::new("rpool/ROOT/default@backup")
                .unwrap()
                .basename(),
            "default@backup"
        );
        assert_eq!(
            DatasetName::new("simple@snap").unwrap().basename(),
            "simple@snap"
        );
    }

    #[test]
    fn test_dataset_name_append_and_snapshot() {
        // Test append() method
        let base = DatasetName::new("pool").unwrap();
        let child = base.append("data").unwrap();
        assert_eq!(child.to_string(), "pool/data");

        let grandchild = child.append("projects").unwrap();
        assert_eq!(grandchild.to_string(), "pool/data/projects");

        // Test snapshot() method
        let snap = base.snapshot("backup").unwrap();
        assert_eq!(snap.to_string(), "pool@backup");

        let child_snap = grandchild.snapshot("2023-12-01").unwrap();
        assert_eq!(child_snap.to_string(), "pool/data/projects@2023-12-01");

        // Test error cases - cannot append to a snapshot
        let snapshot = DatasetName::new("pool/dataset@snap").unwrap();
        assert!(snapshot.append("child").is_err());

        // Test error cases - cannot create snapshot of a snapshot
        assert!(snapshot.snapshot("another").is_err());

        // Test validation still works
        assert!(base.append("").is_err()); // empty component
        assert!(base.append("invalid name").is_err()); // space in name
        assert!(base.snapshot("").is_err()); // empty snapshot name
        assert!(base.snapshot("invalid name").is_err()); // space in snapshot name
    }

    #[test]
    fn test_dataset_parent() {
        assert_eq!(DatasetName::new("rpool").unwrap().parent(), None);
        assert_eq!(
            DatasetName::new("rpool/ROOT/default")
                .unwrap()
                .parent()
                .unwrap()
                .to_string(),
            "rpool/ROOT"
        );
        assert_eq!(
            DatasetName::new("rpool/ROOT/default@backup")
                .unwrap()
                .parent()
                .unwrap()
                .to_string(),
            "rpool/ROOT"
        );
    }

    #[test]
    fn test_libzfs_error() {
        let libzfs_err = LibzfsError {
            errno: ffi::EZFS_NOENT,
            description: "no such pool or dataset".to_string(),
        };
        let err: Error = libzfs_err.into();
        assert_eq!(format!("{}", err), "no such pool or dataset");
    }

    #[test]
    fn test_read_hostid() {
        use std::io::Write;

        let cases = [
            (vec![0x0c, 0xb1, 0xba, 0x00], Some("0x00bab10c".to_string())), // ZFSBootMenu
            (vec![0x00, 0x00, 0x00, 0x00], Some("0x00000000".to_string())),
            (vec![0xff, 0xff, 0xff, 0xff], Some("0xffffffff".to_string())),
            (vec![0xef, 0xbe, 0xad, 0xde], Some("0xdeadbeef".to_string())), // zgenhostid(8)
            (vec![0x01, 0x02, 0x03], None),
            (vec![0x01, 0x02, 0x03, 0x04, 0x05], None),
            (vec![], None),
        ];

        for (bytes, expected) in cases {
            let mut temp_file = tempfile::NamedTempFile::new().unwrap();
            temp_file.write_all(&bytes).unwrap();
            temp_file.flush().unwrap();
            let hostid = read_hostid(temp_file.path()).map(|hostid| format!("0x{:08x}", hostid));
            assert_eq!(hostid, expected);
        }

        // Test with non-existent file
        assert_eq!(read_hostid(Path::new("/path/that/does/not/exist")), None);
    }

    #[test]
    fn test_nvlist() {
        assert!(
            NvList::from(&[
                ("canmount", "noauto"),
                ("mountpoint", "none"),
                ("compression", "lz4"),
            ])
            .is_ok()
        );
        assert!(NvList::from(&[("invalid\0key", "value")]).is_err());
        assert!(NvList::from(&[("key", "invalid\0value")]).is_err());
    }

    #[test]
    fn test_format_zfs_bytes() {
        // We're technically just testing a ZFS library function here, but
        // these are useful to have in case we ever need to write an
        // independent implementation.

        // Boundary values.
        assert_eq!(format_zfs_bytes(0), "0B");
        assert_eq!(format_zfs_bytes(u64::MAX), "16.0E");

        // Verify no decimal places for exact order of magnitude values.
        assert_eq!(format_zfs_bytes(1024), "1K");
        assert_eq!(format_zfs_bytes(1_048_576), "1M");
        assert_eq!(format_zfs_bytes(1_073_741_824), "1G");
        assert_eq!(format_zfs_bytes(1_099_511_627_776), "1T");
        assert_eq!(format_zfs_bytes(1_125_899_906_842_624), "1P");
        assert_eq!(format_zfs_bytes(1_152_921_504_606_846_976), "1E");

        // Verify for values that need rounding to truncate precision.
        assert_eq!(format_zfs_bytes(10239), "10.0K"); // 9.999K
        assert_eq!(format_zfs_bytes(10289), "10.0K"); // 10.04K
        assert_eq!(format_zfs_bytes(1023), "1023B"); // 0.999K
        assert_eq!(format_zfs_bytes(1025), "1.00K"); // 1.001K
        assert_eq!(format_zfs_bytes(1028), "1.00K"); // 1.004K
        assert_eq!(format_zfs_bytes(1610612736), "1.50G"); // 1.5G exactly

        // Test 5-character formatting limit edge cases.
        assert_eq!(format_zfs_bytes(10_234_880), "9.76M");
        assert_eq!(format_zfs_bytes(102_348_800), "97.6M");
        assert_eq!(format_zfs_bytes(1_023_488_000), "976M");
    }
}

// libzfs FFI bindings
mod ffi {
    use std::os::raw::{c_char, c_int, c_uint, c_void};

    // Opaque handle types matching libzfs
    #[repr(C)]
    pub struct LibzfsHandle {
        _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct ZfsHandle {
        _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct ZpoolHandle {
        _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct NvList {
        _opaque: [u8; 0],
    }

    // ZFS type constants from sys/fs/zfs.h
    pub const ZFS_TYPE_FILESYSTEM: c_int = 1 << 0;
    pub const ZFS_TYPE_SNAPSHOT: c_int = 1 << 1;

    // ZFS property constants from sys/fs/zfs.h
    pub const ZFS_PROP_CREATION: c_int = 1;
    pub const ZFS_PROP_USED: c_int = 2;
    pub const ZFS_PROP_MOUNTPOINT: c_int = 13;
    pub const ZFS_PROP_CANMOUNT: c_int = 28;
    pub const ZFS_PROP_GUID: c_int = 42;

    // ZPool property constants from sys/fs/zfs.h
    pub const ZPOOL_PROP_BOOTFS: c_int = 7;

    // NvList constants
    pub const NV_UNIQUE_NAME: c_uint = 0x1;

    // ZFS property type (placeholder - we'd need to define proper enum)
    pub type ZfsProp = c_int;
    pub type ZpoolProp = c_int;

    // Rename flags structure matching libzfs.h
    #[repr(C)]
    pub struct RenameFlags {
        pub recursive: c_uint,    // : 1 bit field
        pub nounmount: c_uint,    // : 1 bit field
        pub forceunmount: c_uint, // : 1 bit field
    }

    // The subset of error codes in libzfs.h we pay special attention to.
    pub const EZFS_EEXIST: c_int = 2008;
    pub const EZFS_NOENT: c_int = 2009;

    unsafe extern "C" {
        // Library initialization
        pub fn libzfs_init() -> *mut LibzfsHandle;
        pub fn libzfs_fini(hdl: *mut LibzfsHandle);

        // Error handling
        pub fn libzfs_errno(hdl: *mut LibzfsHandle) -> c_int;
        pub fn libzfs_error_description(hdl: *mut LibzfsHandle) -> *const c_char;

        // Dataset handle management
        pub fn zfs_open(
            hdl: *mut LibzfsHandle,
            name: *const c_char,
            types: c_int,
        ) -> *mut ZfsHandle;
        pub fn zfs_close(zhp: *mut ZfsHandle);

        // Dataset operations
        pub fn zfs_create(
            hdl: *mut LibzfsHandle,
            path: *const c_char,
            typ: c_int,
            props: *mut c_void,
        ) -> c_int;

        pub fn zfs_destroy(zhp: *mut ZfsHandle, defer: c_int) -> c_int;

        pub fn zfs_snapshot(
            hdl: *mut LibzfsHandle,
            path: *const c_char,
            recursive: c_int, // boolean_t
            props: *mut NvList,
        ) -> c_int;

        pub fn zfs_clone(zhp: *mut ZfsHandle, target: *const c_char, props: *mut NvList) -> c_int;

        // Mount operations
        pub fn zfs_mount_at(
            zhp: *mut ZfsHandle,
            path: *const c_char,
            flags: c_int,
            fstype: *const c_char,
        ) -> c_int;
        pub fn zfs_unmount(zhp: *mut ZfsHandle, mountpoint: *const c_char, flags: c_int) -> c_int;
        pub fn zfs_is_mounted(zhp: *mut ZfsHandle, where_: *mut *mut c_char) -> c_int;

        // Rename operation
        pub fn zfs_rename(zhp: *mut ZfsHandle, target: *const c_char, flags: RenameFlags) -> c_int;

        // Rollback operation
        pub fn zfs_rollback(zhp: *mut ZfsHandle, snap: *mut ZfsHandle, force: c_int) -> c_int;

        // Iterator functions
        pub fn zfs_iter_children(
            zhp: *mut ZfsHandle,
            func: extern "C" fn(*mut ZfsHandle, *mut c_void) -> c_int,
            data: *mut c_void,
        ) -> c_int;

        pub fn zfs_iter_snapshots(
            zhp: *mut ZfsHandle,
            simple: c_int,
            func: extern "C" fn(*mut ZfsHandle, *mut c_void) -> c_int,
            data: *mut c_void,
            min_txg: u64,
            max_txg: u64,
        ) -> c_int;

        pub fn zfs_iter_dependents(
            zhp: *mut ZfsHandle,
            allowrecursion: c_int, // boolean_t
            func: extern "C" fn(*mut ZfsHandle, *mut c_void) -> c_int,
            data: *mut c_void,
        ) -> c_int;

        // Property functions
        pub fn zfs_get_name(zhp: *mut ZfsHandle) -> *const c_char;
        pub fn zfs_prop_get(
            zhp: *mut ZfsHandle,
            prop: ZfsProp,
            buf: *mut c_char,
            len: usize,
            source: *mut c_int,
            literal: c_int,
        ) -> c_int;
        pub fn zfs_prop_get_numeric(
            zhp: *mut ZfsHandle,
            prop: ZfsProp,
            value: *mut u64,
            source: *mut c_int,
            buf: *mut c_char,
            len: usize,
        ) -> c_int;
        pub fn zfs_get_user_props(zhp: *mut ZfsHandle) -> *mut NvList;
        pub fn zfs_prop_set(
            zhp: *mut ZfsHandle,
            propname: *const c_char,
            propval: *const c_char,
        ) -> c_int;

        // Utility functions
        pub fn zfs_nicebytes(bytes: u64, buf: *mut c_char, len: usize);

        // NvList functions for property management
        pub fn nvlist_alloc(nvlp: *mut *mut NvList, nvflag: c_uint, kmflag: c_int) -> c_int;
        pub fn nvlist_add_string(
            nvl: *mut NvList,
            name: *const c_char,
            val: *const c_char,
        ) -> c_int;
        pub fn nvlist_lookup_string(
            nvl: *mut NvList,
            name: *const c_char,
            val: *mut *mut c_char,
        ) -> c_int;
        pub fn nvlist_lookup_nvlist(
            nvl: *mut NvList,
            name: *const c_char,
            val: *mut *mut NvList,
        ) -> c_int;
        pub fn nvlist_free(nvl: *mut NvList);

        // ZPool functions
        pub fn zpool_open(hdl: *mut LibzfsHandle, name: *const c_char) -> *mut ZpoolHandle;
        pub fn zpool_close(zhp: *mut ZpoolHandle);
        pub fn zpool_get_prop(
            zhp: *mut ZpoolHandle,
            prop: ZpoolProp,
            buf: *mut c_char,
            len: usize,
            source: *mut c_int,
            literal: c_int,
        ) -> c_int;
        pub fn zpool_set_prop(
            zhp: *mut ZpoolHandle,
            prop: *const c_char,
            value: *const c_char,
        ) -> c_int;
        pub fn zpool_get_userprop(
            zhp: *mut ZpoolHandle,
            prop: *const c_char,
            buf: *mut c_char,
            len: usize,
            source: *mut c_int,
        ) -> c_int;
    }
}
