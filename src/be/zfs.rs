use std::ffi::{CStr, CString, OsStr, c_char, c_int};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr;

use super::validation::{validate_component, validate_dataset_name};
use super::{BootEnvironment, Client, Error, MountMode, Snapshot};

use self::libzfs::*;

/// A ZFS boot environment client backed by libzfs.
pub struct LibZfsClient {
    root: DatasetName,
    lzh: *mut LibzfsHandle,
}

impl LibZfsClient {
    /// Create a new client with the specified boot environment root.
    pub fn new(root: String) -> Result<Self, Error> {
        let root = DatasetName::new(root.as_str())?;
        let lzh = unsafe { libzfs_init() };
        if lzh.is_null() {
            return Err(Error::ZfsError {
                message: "Failed to initialize libzfs".to_string(),
            });
        }
        Ok(Self { root, lzh })
    }

    /// Create a new client using the default boot environment root.
    pub fn system() -> Result<Self, Error> {
        // In a real implementation, this would detect the current BE root
        // For now, use a common default
        Self::new("rpool/ROOT".to_string())
    }

    /// Get the active boot environment (the one we're running from).
    fn get_active_be(&self) -> Result<String, Error> {
        // Stub: In a real implementation, this would detect the current BE
        // by checking mountpoints, kernel parameters, etc.
        Ok("default".to_string())
    }

    /// Check if a boot environment exists.
    fn be_exists(&self, be_name: &str) -> Result<bool, Error> {
        let be_ds = self.root.append(be_name)?;
        match Dataset::filesystem(self.lzh, &be_ds) {
            Ok(_) => Ok(true),
            Err(Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

impl Drop for LibZfsClient {
    fn drop(&mut self) {
        if !self.lzh.is_null() {
            unsafe {
                libzfs_fini(self.lzh);
            }
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
        // Check if a boot environment with this name already exists.
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

        let be_path = self.root.append(be_name)?;

        // Create the ZFS filesystem
        let result = unsafe {
            zfs_create(
                self.lzh,
                be_path.as_ptr(),
                ZFS_TYPE_FILESYSTEM,
                ptr::null_mut(),
            )
        };
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
        be_name: &str,
        force_unmount: bool,
        _force_no_verify: bool,
        _snapshots: bool,
    ) -> Result<(), Error> {
        // Cannot destroy active boot environment.
        if self
            .get_active_be()
            .map(|active| active == be_name)
            .unwrap_or(false)
        {
            return Err(Error::CannotDestroyActive {
                name: be_name.to_string(),
            });
        }

        let be_path = self.root.append(be_name)?;
        let dataset = Dataset::filesystem(self.lzh, &be_path)?;

        let mountpoint = dataset.get_mountpoint();
        if mountpoint.is_some() {
            if !force_unmount {
                return Err(Error::BeMounted {
                    name: be_name.to_string(),
                    mountpoint: mountpoint.unwrap().display().to_string(),
                });
            } else {
                // Best-effort attempt to unmount the dataset.
                _ = dataset.unmount(true);
            }
        }

        dataset.destroy()
    }

    fn mount(&self, be_name: &str, mountpoint: &str, _mode: MountMode) -> Result<(), Error> {
        let be_path = self.root.append(be_name)?;
        let dataset = Dataset::filesystem(self.lzh, &be_path)?;

        // Check if it's already mounted.
        if let Some(existing) = dataset.get_mountpoint() {
            return Err(Error::BeMounted {
                name: be_name.to_string(),
                mountpoint: existing.display().to_string(),
            });
        }

        // TODO: Support recursively mounting child datasets.
        // TODO: Better error mapping.
        dataset.mount_at(mountpoint).map_err(|_| Error::ZfsError {
            message: format!("Failed to mount boot environment '{}'", be_name),
        })
    }

    fn unmount(&self, be_name: &str, force: bool) -> Result<(), Error> {
        let be_path = self.root.append(be_name)?;
        let dataset = Dataset::filesystem(self.lzh, &be_path)?;

        // Don't unmount what isn't mounted.
        if dataset.get_mountpoint().is_none() {
            return Ok(());
        }

        // TODO: Support recursively unmounting child datasets.
        // TODO: Better error mapping.
        dataset.unmount(force).map_err(|_| Error::UnmountFailed {
            name: be_name.to_string(),
            reason: "ZFS unmount failed".to_string(),
        })
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error> {
        let be_path = self.root.append(be_name)?;
        let new_path = self.root.append(new_name)?;

        // Check if the target already exists.
        if Dataset::filesystem(self.lzh, &new_path).is_ok() {
            return Err(Error::Conflict {
                name: new_name.to_string(),
            });
        }

        let dataset = Dataset::filesystem(self.lzh, &be_path)?;
        dataset.rename(
            &new_path,
            RenameFlags {
                recursive: 0,
                nounmount: 1, // Leave boot environment mounts in place.
                forceunmount: 0,
            },
        )
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
        let be_path = self.root.append(be_name)?;
        let be_dataset = Dataset::filesystem(self.lzh, &be_path)?;
        let snap_path = self.root.snapshot(snapshot)?;
        let snap_dataset = Dataset::snapshot(self.lzh, &snap_path)?;

        // TODO: Better error mapping.
        be_dataset
            .rollback_to(&snap_dataset)
            .map_err(|_| Error::ZfsError {
                message: format!(
                    "Failed to rollback boot environment '{}' to snapshot '{}'",
                    be_name, snapshot
                ),
            })
    }

    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error> {
        let root_dataset = Dataset::filesystem(self.lzh, &self.root)?;

        // Helper struct to pass multiple pieces of data to the callback.
        struct CollectData {
            boot_environments: Vec<BootEnvironment>,
            current_be: String,
        }
        let mut collect_data = CollectData {
            boot_environments: Vec::new(),
            current_be: self.get_active_be().unwrap_or_default(),
        };
        let data_ptr = &mut collect_data as *mut CollectData;

        extern "C" fn collect_be_callback(
            zhp: *mut ZfsHandle,
            data: *mut std::os::raw::c_void,
        ) -> std::os::raw::c_int {
            let collect_data = unsafe { &mut *(data as *mut CollectData) };
            let dataset = Dataset::borrowed(zhp);

            let path = match dataset.get_name() {
                Some(name) => name,
                None => return 0, // Skip this iteration.
            };
            let be_name = if let Some(index) = path.rfind('/') {
                (&path[index + 1..]).to_string()
            } else {
                path.clone()
            };

            collect_data.boot_environments.push(BootEnvironment {
                name: be_name.to_string(),
                path: path,
                description: None, // TODO: Read from user property
                mountpoint: dataset.get_mountpoint(),
                active: be_name == collect_data.current_be,
                next_boot: false, // TODO: Read from boot configuration
                boot_once: false, // TODO: Read from boot configuration
                space: dataset.get_used_space(),
                created: dataset.get_creation_time(),
            });

            0 // Continue.
        }

        let result = unsafe {
            zfs_iter_children(
                root_dataset.handle(),
                collect_be_callback,
                data_ptr as *mut std::os::raw::c_void,
            )
        };
        if result != 0 {
            return Err(Error::ZfsError {
                message: "Failed to iterate over boot environments".to_string(),
            });
        }

        Ok(collect_data.boot_environments)
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

/// Safe wrapper for various operations on a ZFS dataset handle.
struct Dataset {
    handle: *mut ZfsHandle,
    owns_handle: bool,
}

impl Dataset {
    /// Open a ZFS dataset with the given name and type.
    pub fn open(
        handle: *mut LibzfsHandle,
        name: &DatasetName,
        zfs_type: c_int,
    ) -> Result<Self, Error> {
        let handle = unsafe { zfs_open(handle, name.as_ptr(), zfs_type) };
        if handle.is_null() {
            return Err(Error::NotFound {
                name: name.to_string(),
            });
        }
        Ok(Dataset {
            handle,
            owns_handle: true,
        })
    }

    // Open a filesystem dataset.
    pub fn filesystem(handle: *mut LibzfsHandle, name: &DatasetName) -> Result<Self, Error> {
        Dataset::open(handle, name, ZFS_TYPE_FILESYSTEM)
    }

    // Open a snapshot dataset.
    pub fn snapshot(handle: *mut LibzfsHandle, name: &DatasetName) -> Result<Self, Error> {
        Dataset::open(handle, name, ZFS_TYPE_SNAPSHOT)
    }

    /// Create a Dataset from an existing handle. Closing the handle is the
    /// responsibility of the caller.
    pub fn borrowed(handle: *mut ZfsHandle) -> Self {
        Dataset {
            handle,
            owns_handle: false,
        }
    }

    /// Get the raw handle (for use with libzfs functions)
    pub fn handle(&self) -> *mut ZfsHandle {
        self.handle
    }

    /// Get the dataset name.
    pub fn get_name(&self) -> Option<String> {
        let name_ptr = unsafe { zfs_get_name(self.handle) };
        if name_ptr.is_null() {
            // The libzfs API claims this is not possible.
            return None;
        }
        let cstr = unsafe { CStr::from_ptr(name_ptr) };
        Some(cstr.to_string_lossy().into_owned())
    }

    /// Get the dataset's current mountpoint if it is mounted.
    pub fn get_mountpoint(&self) -> Option<PathBuf> {
        let mut mountpoint_ptr: *mut std::os::raw::c_char = ptr::null_mut();
        let result = unsafe {
            zfs_is_mounted(
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
        self.get_property(ZFS_PROP_USED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    /// Get the creation timestamp for this dataset.
    pub fn get_creation_time(&self) -> i64 {
        self.get_property(ZFS_PROP_CREATION)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    // Rename this dataset.
    pub fn rename(&self, new_name: &DatasetName, flags: RenameFlags) -> Result<(), Error> {
        let result = unsafe { zfs_rename(self.handle, new_name.as_ptr(), flags) };
        if result != 0 {
            return Err(Error::ZfsError {
                message: "Failed to rename dataset".to_string(),
            });
        }
        Ok(())
    }

    /// Destroy this dataset.
    pub fn destroy(&self) -> Result<(), Error> {
        let result = unsafe { zfs_destroy(self.handle, 0) };
        if result != 0 {
            return Err(Error::ZfsError {
                message: "Failed to destroy dataset".to_string(),
            });
        }
        Ok(())
    }

    /// Unmount this dataset with optional force flag.
    pub fn unmount(&self, force: bool) -> Result<(), Error> {
        let flags = if force { 1 } else { 0 };
        let result = unsafe { zfs_unmount(self.handle, ptr::null(), flags) };
        if result != 0 {
            return Err(Error::ZfsError {
                message: "Failed to unmount dataset".to_string(),
            });
        }
        Ok(())
    }

    /// Mount this dataset at the specified path.
    pub fn mount_at(&self, mountpoint: &str) -> Result<(), Error> {
        let c_mountpoint = CString::new(mountpoint).map_err(|_| Error::InvalidPath {
            path: mountpoint.to_string(),
        })?;
        let result = unsafe { zfs_mount_at(self.handle, ptr::null(), 0, c_mountpoint.as_ptr()) };
        if result != 0 {
            // TODO: zfs_mount_at() sets regular ELOOP, ENOENT, ENOTDIR, EPERM,
            // EBUSY via errno. We should convert these to the relevant errors
            // rather than this generic one.
            return Err(Error::ZfsError {
                message: "Failed to mount dataset".to_string(),
            });
        }
        Ok(())
    }

    /// Rollback this dataset to the specified snapshot.
    pub fn rollback_to(&self, snapshot: &Dataset) -> Result<(), Error> {
        let result = unsafe { zfs_rollback(self.handle, snapshot.handle, 0) };
        if result != 0 {
            return Err(Error::ZfsError {
                message: "Failed to rollback dataset".to_string(),
            });
        }
        Ok(())
    }

    /// Get a ZFS property for this dataset.
    fn get_property(&self, prop: c_int) -> Option<String> {
        const PROP_BUF_SIZE: usize = 1024;
        let mut buf = vec![0u8; PROP_BUF_SIZE];
        let result = unsafe {
            zfs_prop_get(
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
}

impl Drop for Dataset {
    fn drop(&mut self) {
        if !self.owns_handle || self.handle.is_null() {
            return;
        }
        unsafe {
            zfs_close(self.handle);
        }
    }
}

// Convenience type for already-validated ZFS dataset names that can be passed
// directly to the FFI layer.
struct DatasetName {
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
        Ok(Self {
            // We know there are no nul bytes in either component at this
            // point, so this is safe.
            inner: unsafe { CString::from_vec_unchecked(v) },
        })
    }

    pub fn snapshot(&self, name: &str) -> Result<Self, Error> {
        validate_component(name, false)?;
        let mut v = Vec::from(self.inner.to_bytes());
        v.push('@' as u8);
        v.append(&mut Vec::from(name));
        Ok(Self {
            // We know there are no nul bytes in either component at this
            // point, so this is safe.
            inner: unsafe { CString::from_vec_unchecked(v) },
        })
    }

    pub fn as_ptr(&self) -> *const c_char {
        self.inner.as_ptr()
    }

    pub fn to_string(&self) -> String {
        // We can safely unwrap here because dataset names are valid UTF-8.
        self.inner.to_str().unwrap().to_string()
    }
}

// libzfs FFI bindings
mod libzfs {
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

    // ZFS type constants from sys/fs/zfs.h
    pub const ZFS_TYPE_FILESYSTEM: c_int = 1 << 0;
    pub const ZFS_TYPE_SNAPSHOT: c_int = 1 << 1;
    pub const ZFS_TYPE_POOL: c_int = 1 << 3;

    // ZFS property constants from libzfs.h
    pub const ZFS_PROP_CREATION: c_int = 1;
    pub const ZFS_PROP_USED: c_int = 2;

    // ZFS property type (placeholder - we'd need to define proper enum)
    pub type ZfsProp = c_int;

    // Rename flags structure matching libzfs.h
    #[repr(C)]
    pub struct RenameFlags {
        pub recursive: c_uint,    // : 1 bit field
        pub nounmount: c_uint,    // : 1 bit field
        pub forceunmount: c_uint, // : 1 bit field
    }

    unsafe extern "C" {
        // Library initialization
        pub fn libzfs_init() -> *mut LibzfsHandle;
        pub fn libzfs_fini(hdl: *mut LibzfsHandle);

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
    }
}
