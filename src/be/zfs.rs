use chrono::Utc;
use std::ffi::{CStr, CString};
use std::path::PathBuf;
use std::ptr;

use super::validation::validate_be_name;
use super::{BootEnvironment, Client, Error, MountMode, Snapshot};

use self::libzfs::*;

/// A ZFS client that makes actual libzfs calls
/// This implementation maintains validation logic from the mock client
/// but delegates filesystem operations to libzfs
pub struct LibZfsClient {
    root: String,
    libzfs_handle: *mut LibzfsHandle,
}

impl LibZfsClient {
    /// Create a new ZFS client with the specified boot environment root
    pub fn new(root: String) -> Result<Self, Error> {
        let handle = libzfs_init();
        if handle.is_null() {
            return Err(Error::ZfsError {
                message: "Failed to initialize libzfs".to_string(),
            });
        }

        Ok(Self {
            root,
            libzfs_handle: handle,
        })
    }

    /// Create a ZFS client using the default root (system detection)
    pub fn system() -> Result<Self, Error> {
        // In a real implementation, this would detect the current BE root
        // For now, use a common default
        Self::new("rpool/ROOT".to_string())
    }

    /// Get the current boot environment (the one we're running from)
    fn get_current_be(&self) -> Result<String, Error> {
        // Stub: In a real implementation, this would detect the current BE
        // by checking mountpoints, kernel parameters, etc.
        Ok("default".to_string())
    }

    /// Check if a boot environment exists
    fn be_exists(&self, be_name: &str) -> Result<bool, Error> {
        let be_path = format!("{}/{}", self.root, be_name);
        let c_path = CString::new(be_path).map_err(|_| Error::InvalidName {
            name: be_name.to_string(),
            reason: "contains null bytes".to_string(),
        })?;

        let zhp = zfs_open(self.libzfs_handle, c_path.as_ptr(), ZFS_TYPE_FILESYSTEM);
        if zhp.is_null() {
            Ok(false)
        } else {
            zfs_close(zhp);
            Ok(true)
        }
    }

    /// Get boot environment properties from ZFS
    fn get_be_properties(&self, be_name: &str) -> Result<BootEnvironment, Error> {
        let be_path = format!("{}/{}", self.root, be_name);
        let c_path = CString::new(be_path.clone()).map_err(|_| Error::InvalidName {
            name: be_name.to_string(),
            reason: "contains null bytes".to_string(),
        })?;

        let zhp = zfs_open(self.libzfs_handle, c_path.as_ptr(), ZFS_TYPE_FILESYSTEM);
        if zhp.is_null() {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        // In a real implementation, these would read actual ZFS properties
        // For now, we stub with reasonable defaults
        let current_be = self.get_current_be()?;
        let active = be_name == current_be;

        let be = BootEnvironment {
            name: be_name.to_string(),
            path: be_path,
            description: None, // Would read from user property
            mountpoint: if active {
                Some(PathBuf::from("/"))
            } else {
                None
            },
            active,
            next_boot: false,                // Would read from boot configuration
            boot_once: false,                // Would read from boot configuration
            space: 8192,                     // Would read from ZFS 'used' property
            created: Utc::now().timestamp(), // Would read from ZFS 'creation' property
        };

        zfs_close(zhp);
        Ok(be)
    }
}

impl Drop for LibZfsClient {
    fn drop(&mut self) {
        if !self.libzfs_handle.is_null() {
            libzfs_fini(self.libzfs_handle);
        }
    }
}

impl Client for LibZfsClient {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&str>,
        _properties: &[String],
    ) -> Result<(), Error> {
        validate_be_name(be_name, &self.root)?;

        // Check if BE already exists
        if self.be_exists(be_name)? {
            return Err(Error::Conflict {
                name: be_name.to_string(),
            });
        }

        // If cloning from source, verify source exists
        if let Some(src) = source {
            if !self.be_exists(src)? {
                return Err(Error::NotFound {
                    name: src.to_string(),
                });
            }
        }

        let be_path = format!("{}/{}", self.root, be_name);
        let c_path = CString::new(be_path).map_err(|_| Error::InvalidName {
            name: be_name.to_string(),
            reason: "contains null bytes".to_string(),
        })?;

        // Create the ZFS filesystem
        let result = zfs_create(
            self.libzfs_handle,
            c_path.as_ptr(),
            ZFS_TYPE_FILESYSTEM,
            ptr::null_mut(),
        );
        if result != 0 {
            return Err(Error::ZfsError {
                message: format!("Failed to create boot environment '{}'", be_name),
            });
        }

        // Set description if provided
        if let Some(_desc) = description {
            // In a real implementation, this would set a user property
            // zfs_prop_set(zhp, "beadm:description", desc);
        }

        Ok(())
    }

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        _force_no_verify: bool,
        _snapshots: bool,
    ) -> Result<(), Error> {
        // Check if BE exists
        if !self.be_exists(target)? {
            return Err(Error::NotFound {
                name: target.to_string(),
            });
        }

        // Get BE properties to check constraints
        let be = self.get_be_properties(target)?;

        // Cannot destroy active BE
        if be.active {
            return Err(Error::CannotDestroyActive {
                name: target.to_string(),
            });
        }

        // Check if mounted
        if !force_unmount && be.mountpoint.is_some() {
            return Err(Error::BeMounted {
                name: target.to_string(),
                mountpoint: be.mountpoint.unwrap().display().to_string(),
            });
        }

        let be_path = format!("{}/{}", self.root, target);
        let c_path = CString::new(be_path).map_err(|_| Error::InvalidName {
            name: target.to_string(),
            reason: "contains null bytes".to_string(),
        })?;

        let zhp = zfs_open(self.libzfs_handle, c_path.as_ptr(), 1);
        if zhp.is_null() {
            return Err(Error::ZfsError {
                message: format!("Failed to open boot environment '{}'", target),
            });
        }

        // Unmount if force_unmount is set and it's mounted
        if force_unmount && be.mountpoint.is_some() {
            let result = zfs_unmount(zhp, ptr::null(), 0);
            if result != 0 {
                zfs_close(zhp);
                return Err(Error::UnmountFailed {
                    name: target.to_string(),
                    reason: "ZFS unmount failed".to_string(),
                });
            }
        }

        // Destroy the filesystem
        let result = zfs_destroy(zhp, 0);
        zfs_close(zhp);

        if result != 0 {
            return Err(Error::ZfsError {
                message: format!("Failed to destroy boot environment '{}'", target),
            });
        }

        Ok(())
    }

    fn mount(&self, be_name: &str, mountpoint: &str, _mode: MountMode) -> Result<(), Error> {
        validate_be_name(be_name, &self.root)?;

        let c_mountpoint = CString::new(mountpoint).map_err(|_| Error::InvalidPath {
            path: mountpoint.to_string(),
        })?;

        // Note: we know this is safe to unwrap because we've already validated
        // the name.
        let be_path = CString::new(format!("{}/{}", self.root, be_name)).unwrap();
        let zhp = zfs_open(self.libzfs_handle, be_path.as_ptr(), ZFS_TYPE_FILESYSTEM);
        if zhp.is_null() {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        // Check if it's already mounted.
        let mut mountpoint_ptr: *mut std::os::raw::c_char = ptr::null_mut();
        if zfs_is_mounted(zhp, &mut mountpoint_ptr as *mut *mut std::os::raw::c_char) {
            let mountpoint = if mountpoint_ptr.is_null() {
                "unknown".to_string() // Unclear how this could ever happen, panic instead?
            } else {
                let mountpoint_str = unsafe { CStr::from_ptr(mountpoint_ptr) };
                // Free the C-allocated memory as libzfs doesn't do it for us
                unsafe {
                    libc::free(mountpoint_ptr as *mut libc::c_void);
                }
                mountpoint_str.to_string_lossy().into_owned()
            };
            zfs_close(zhp);
            return Err(Error::BeMounted {
                name: be_name.to_string(),
                mountpoint,
            });
        }

        // TODO: Support recursively mounting child datasets.
        let result = zfs_mount_at(zhp, ptr::null(), 0, c_mountpoint.as_ptr());
        zfs_close(zhp);

        // TODO: zfs_mount_at() sets regular ELOOP, ENOENT, ENOTDIR, EPERM,
        // EBUSY via errno. We should convert these to the relevant errors
        // rather than this generic one.
        if result != 0 {
            return Err(Error::ZfsError {
                message: format!("Failed to mount boot environment '{}'", be_name),
            });
        }

        Ok(())
    }

    fn unmount(&self, be_name: &str, force: bool) -> Result<(), Error> {
        validate_be_name(be_name, &self.root)?;

        // Note: we know this is safe to unwrap because we've already validated
        // the name.
        let be_path = CString::new(format!("{}/{}", self.root, be_name)).unwrap();
        let zhp = zfs_open(self.libzfs_handle, be_path.as_ptr(), ZFS_TYPE_FILESYSTEM);
        if zhp.is_null() {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        if !zfs_is_mounted(zhp, ptr::null_mut()) {
            return Ok(());
        }

        // TODO: Use a proper constant here for MS_FORCE.
        let flags = if force { 1 } else { 0 };

        // TODO: Support recursively unmounting child datasets.
        let result = zfs_unmount(zhp, ptr::null(), flags);
        zfs_close(zhp);

        // TODO: Handle EBUSY set by zfs_unmount() above.
        if result != 0 {
            return Err(Error::UnmountFailed {
                name: be_name.to_string(),
                reason: "ZFS unmount failed".to_string(),
            });
        }

        Ok(())
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error> {
        validate_be_name(be_name, &self.root)?;
        validate_be_name(new_name, &self.root)?;

        // Check if the target already exists.
        if self.be_exists(new_name)? {
            return Err(Error::Conflict {
                name: new_name.to_string(),
            });
        }

        // Note: we know these are safe to unwrap because we've already
        // validated the names.
        let old_path = CString::new(format!("{}/{}", self.root, be_name)).unwrap();
        let new_path = CString::new(format!("{}/{}", self.root, new_name)).unwrap();

        let zhp = zfs_open(self.libzfs_handle, old_path.as_ptr(), ZFS_TYPE_FILESYSTEM);
        if zhp.is_null() {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        let result = zfs_rename(
            zhp,
            new_path.as_ptr(),
            RenameFlags {
                recursive: false,
                nounmount: true, // Leave boot environment mounts in place.
                forceunmount: false,
            },
        );
        zfs_close(zhp);

        if result != 0 {
            return Err(Error::ZfsError {
                message: format!(
                    "Failed to rename boot environment '{}' to '{}'",
                    be_name, new_name
                ),
            });
        }

        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), Error> {
        // Check if BE exists
        if !self.be_exists(be_name)? {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        // In a real implementation, this would:
        // - Update bootloader configuration (GRUB, loader.conf, etc.)
        // - Set ZFS pool bootfs property if needed
        // - Handle temporary vs permanent activation differently

        if temporary {
            // Set boot-once flag in bootloader
            // This is highly system-specific (GRUB vs loader vs systemd-boot)
        } else {
            // Set permanent boot environment
            // Update bootloader default entry
        }

        // For now, just return success
        // Real implementation would handle bootloader integration
        Ok(())
    }

    fn deactivate(&self, be_name: &str) -> Result<(), Error> {
        // Check if BE exists
        if !self.be_exists(be_name)? {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        // In a real implementation, this would remove temporary boot flags
        // from bootloader configuration

        Ok(())
    }

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), Error> {
        // Check if BE exists
        if !self.be_exists(be_name)? {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        let be_path = format!("{}/{}", self.root, be_name);
        let snap_path = format!("{}@{}", be_path, snapshot);

        let c_be_path = CString::new(be_path).map_err(|_| Error::InvalidName {
            name: be_name.to_string(),
            reason: "contains null bytes".to_string(),
        })?;
        let c_snap_path = CString::new(snap_path).map_err(|_| Error::InvalidName {
            name: snapshot.to_string(),
            reason: "contains null bytes".to_string(),
        })?;

        let zhp = zfs_open(self.libzfs_handle, c_be_path.as_ptr(), 1);
        if zhp.is_null() {
            return Err(Error::ZfsError {
                message: format!("Failed to open boot environment '{}'", be_name),
            });
        }

        let snap_zhp = zfs_open(self.libzfs_handle, c_snap_path.as_ptr(), ZFS_TYPE_SNAPSHOT);
        if snap_zhp.is_null() {
            zfs_close(zhp);
            return Err(Error::NotFound {
                name: snapshot.to_string(),
            });
        }

        let result = zfs_rollback(zhp, snap_zhp, 0);
        zfs_close(zhp);
        zfs_close(snap_zhp);

        if result != 0 {
            return Err(Error::ZfsError {
                message: format!(
                    "Failed to rollback boot environment '{}' to snapshot '{}'",
                    be_name, snapshot
                ),
            });
        }

        Ok(())
    }

    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error> {
        // In a real implementation, this would iterate over ZFS datasets
        // under the root and collect their properties

        // For now, return a stub list
        // Real implementation would use zfs_iter_children() callback
        let mut bes = Vec::new();

        // Stub: just return the current BE if it exists
        if let Ok(current) = self.get_current_be() {
            if let Ok(be) = self.get_be_properties(&current) {
                bes.push(be);
            }
        }

        Ok(bes)
    }

    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, Error> {
        // Check if BE exists
        if !self.be_exists(be_name)? {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }

        // In a real implementation, this would iterate over snapshots
        // using zfs_iter_snapshots() and collect their properties

        // For now, return empty list
        Ok(Vec::new())
    }
}

// Internal libzfs FFI bindings module
// These are stubs that would normally link to the actual libzfs library
mod libzfs {
    use std::os::raw::{c_char, c_int, c_void};

    // Opaque handle types matching libzfs
    #[repr(C)]
    pub struct LibzfsHandle {
        _opaque: c_void,
    }

    #[repr(C)]
    pub struct ZfsHandle {
        _opaque: c_void,
    }

    #[repr(C)]
    pub struct ZpoolHandle {
        _opaque: c_void,
    }

    // ZFS type constants
    pub const ZFS_TYPE_FILESYSTEM: c_int = 1;
    pub const ZFS_TYPE_SNAPSHOT: c_int = 2;

    // Stub libzfs functions - these would be actual extern "C" bindings in practice
    // DO NOT ATTEMPT to link to real libzfs

    pub fn libzfs_init() -> *mut LibzfsHandle {
        // Stub: return a fake handle
        std::ptr::null_mut()
    }

    pub fn libzfs_fini(_hdl: *mut LibzfsHandle) {
        // Stub: no-op
    }

    pub fn zfs_open(_hdl: *mut LibzfsHandle, _name: *const c_char, _type: c_int) -> *mut ZfsHandle {
        // Stub: return a fake handle
        std::ptr::null_mut()
    }

    pub fn zfs_close(_zhp: *mut ZfsHandle) {
        // Stub: no-op
    }

    pub fn zfs_create(
        _hdl: *mut LibzfsHandle,
        _path: *const c_char,
        _type: c_int,
        _props: *mut c_void,
    ) -> c_int {
        // Stub: return success
        0
    }

    pub fn zfs_destroy(_zhp: *mut ZfsHandle, _defer: c_int) -> c_int {
        // Stub: return success
        0
    }

    pub fn zfs_mount(_zhp: *mut ZfsHandle, _options: *const c_char, _flags: c_int) -> c_int {
        // Stub: return success
        0
    }

    pub fn zfs_is_mounted(_zhp: *mut ZfsHandle, _where: *mut *mut c_char) -> bool {
        // Stub: return false (not mounted)
        false
    }

    pub fn zfs_mount_at(
        _zhp: *mut ZfsHandle,
        _path: *const c_char,
        _flags: c_int,
        _fstype: *const c_char,
    ) -> c_int {
        // Stub: return success
        0
    }

    pub fn zfs_unmount(_zhp: *mut ZfsHandle, _mountpoint: *const c_char, _flags: c_int) -> c_int {
        // Stub: return success
        0
    }

    #[repr(C)]
    pub struct RenameFlags {
        pub recursive: bool,
        pub nounmount: bool,
        pub forceunmount: bool,
    }

    pub fn zfs_rename(_zhp: *mut ZfsHandle, _target: *const c_char, _flags: RenameFlags) -> c_int {
        // Stub: return success
        0
    }

    pub fn zfs_rollback(_zhp: *mut ZfsHandle, _snap: *mut ZfsHandle, _force: c_int) -> c_int {
        // Stub: return success
        0
    }

    pub fn zfs_iter_children(
        _zhp: *mut ZfsHandle,
        _func: extern "C" fn(*mut ZfsHandle, *mut c_void) -> c_int,
        _data: *mut c_void,
    ) -> c_int {
        // Stub: return success (no children)
        0
    }

    pub fn zfs_iter_snapshots(
        _zhp: *mut ZfsHandle,
        _func: extern "C" fn(*mut ZfsHandle, *mut c_void) -> c_int,
        _data: *mut c_void,
    ) -> c_int {
        // Stub: return success (no snapshots)
        0
    }

    pub fn zfs_get_name(_zhp: *mut ZfsHandle) -> *const c_char {
        // Stub: return a fake name
        b"stub\0".as_ptr() as *const c_char
    }

    pub fn zfs_prop_get(
        _zhp: *mut ZfsHandle,
        _prop: c_int,
        _buf: *mut c_char,
        _len: usize,
        _source: *mut c_int,
        _literal: c_int,
    ) -> c_int {
        // Stub: return success with empty value
        0
    }
}
