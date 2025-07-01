use chrono::Utc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::RwLock;

use super::validation::validate_be_name;
use super::{BootEnvironment, Client, Error, MountMode, Snapshot};

/// A boot environment client populated with static data that operates
/// entirely in-memory with no side effects.
pub struct EmulatorClient {
    root: String,
    bes: RwLock<Vec<BootEnvironment>>,
}

impl EmulatorClient {
    pub fn new(bes: Vec<BootEnvironment>) -> Self {
        Self {
            root: "zfake/ROOT".to_string(),
            bes: RwLock::new(bes),
        }
    }

    /// Generate a fake GUID based on the boot environment name
    pub fn generate_guid(be_name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        be_name.hash(&mut hasher);
        hasher.finish()
    }

    pub fn empty() -> Self {
        Self {
            root: "zfake/ROOT".to_string(),
            bes: RwLock::new(vec![]),
        }
    }

    pub fn sampled() -> Self {
        Self::new(sample_boot_environments())
    }
}

impl Client for EmulatorClient {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&str>,
        _properties: &[String],
    ) -> Result<(), Error> {
        validate_be_name(be_name, &self.root)?;

        let mut bes = self.bes.write().unwrap();

        if bes.iter().any(|be| be.name == be_name) {
            return Err(Error::Conflict {
                name: be_name.to_string(),
            });
        }

        if let Some(src) = source {
            // TODO: Differentiate snapshot sources from other boot
            // environment sources.
            if !bes.iter().any(|be| be.name == src) {
                return Err(Error::NotFound {
                    name: src.to_owned(),
                });
            }
        }

        bes.push(BootEnvironment {
            name: be_name.to_string(),
            path: format!("{}/{}", self.root, be_name),
            guid: Self::generate_guid(be_name),
            description: description.map(|s| s.to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192, // ZFS datasets consume 8K to start.
            created: Utc::now().timestamp(),
        });
        Ok(())
    }

    fn new(
        &self,
        be_name: &str,
        description: Option<&str>,
        _host_id: Option<&str>,
        _properties: &[String],
    ) -> Result<(), Error> {
        let mut bes = self.bes.write().unwrap();

        // Check for conflicts.
        if bes.iter().any(|be| be.name == be_name) {
            return Err(Error::Conflict {
                name: be_name.to_string(),
            });
        }

        // Create new empty boot environment
        bes.push(BootEnvironment {
            name: be_name.to_string(),
            path: format!("{}/{}", self.root, be_name),
            guid: Self::generate_guid(be_name),
            description: description.map(|s| s.to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192, // ZFS datasets consume 8K to start.
            created: Utc::now().timestamp(),
        });
        Ok(())
    }

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        _force_no_verify: bool,
        snapshots: bool,
    ) -> Result<(), Error> {
        // First, check if the BE exists and validate constraints
        {
            let bes = self.bes.read().unwrap();
            let be = match bes.iter().find(|be| be.name == target) {
                Some(be) => be,
                None => {
                    return Err(Error::NotFound {
                        name: target.to_string(),
                    });
                }
            };

            if be.active {
                return Err(Error::CannotDestroyActive {
                    name: be.name.to_string(),
                });
            }

            if !force_unmount && be.mountpoint.is_some() {
                return Err(Error::BeMounted {
                    name: be.name.to_string(),
                    mountpoint: be.mountpoint.as_ref().unwrap().display().to_string(),
                });
            }
        } // Release the borrow here

        if snapshots {
            unimplemented!("Mocking does not yet track snapshots");
        }

        // Now we can safely borrow mutably to remove the BE
        self.bes.write().unwrap().retain(|x| x.name != target);

        Ok(())
    }

    fn mount(&self, be_name: &str, mountpoint: &str, _mode: MountMode) -> Result<(), Error> {
        // First, validate preconditions with immutable borrow
        {
            let bes = self.bes.read().unwrap();

            // Find the boot environment
            let be = match bes.iter().find(|be| be.name == be_name) {
                Some(be) => be,
                None => {
                    return Err(Error::NotFound {
                        name: be_name.to_string(),
                    });
                }
            };

            // Check if it's already mounted
            if be.mountpoint.is_some() {
                return Err(Error::BeMounted {
                    name: be_name.to_string(),
                    mountpoint: be.mountpoint.as_ref().unwrap().display().to_string(),
                });
            }

            // Check if another BE is already mounted at this path
            if bes.iter().any(|other_be| {
                other_be
                    .mountpoint
                    .as_ref()
                    .map_or(false, |mp| mp.display().to_string() == mountpoint)
            }) {
                return Err(Error::MountPointInUse {
                    path: mountpoint.to_string(),
                });
            }
        } // Release immutable borrow

        // Now perform the mount with mutable borrow
        let mut bes = self.bes.write().unwrap();
        if let Some(be) = bes.iter_mut().find(|be| be.name == be_name) {
            be.mountpoint = Some(std::path::PathBuf::from(mountpoint));
        }

        Ok(())
    }

    fn unmount(&self, target: &str, _force: bool) -> Result<Option<PathBuf>, Error> {
        let mut bes = self.bes.write().unwrap();

        // Target can be either a BE name or a mountpoint path
        let be = match bes.iter_mut().find(|be| {
            be.name == target
                || be
                    .mountpoint
                    .as_ref()
                    .map_or(false, |mp| mp.display().to_string() == target)
        }) {
            Some(be) => be,
            None => {
                return Err(Error::NotFound {
                    name: target.to_string(),
                });
            }
        };

        // Get the mountpoint and unmount
        let mountpoint = be.mountpoint.clone();
        be.mountpoint = None;
        Ok(mountpoint)
    }

    fn hostid(&self, be_name: &str) -> Result<Option<u32>, Error> {
        let bes = self.bes.read().unwrap();

        // Find the boot environment
        let be = match bes.iter().find(|be| be.name == be_name) {
            Some(be) => be,
            None => {
                return Err(Error::NotFound {
                    name: be_name.to_string(),
                });
            }
        };

        // Check if the BE is mounted
        if be.mountpoint.is_none() {
            return Err(Error::NotMounted {
                name: be_name.to_string(),
            });
        }

        // For the mock implementation, return a predictable hostid
        // In a real implementation this would read from the BE's /etc/hostid
        // Return None for BEs with "no-hostid" in the name to test that case
        if be_name.contains("no-hostid") {
            Ok(None)
        } else {
            Ok(Some(0x00deadbeef))
        }
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error> {
        validate_be_name(new_name, &self.root)?;
        let mut bes = self.bes.write().unwrap();

        // Check if source BE exists
        let be_index = match bes.iter().position(|be| be.name == be_name) {
            Some(index) => index,
            None => {
                return Err(Error::NotFound {
                    name: be_name.to_string(),
                });
            }
        };

        // Check if new name already exists
        if bes.iter().any(|be| be.name == new_name) {
            return Err(Error::Conflict {
                name: new_name.to_string(),
            });
        }

        // Perform the rename
        bes[be_index].name = new_name.to_string();
        bes[be_index].path = format!("{}/{}", self.root, new_name);

        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), Error> {
        let mut bes = self.bes.write().unwrap();

        // Find the target boot environment
        let target_index = match bes.iter().position(|be| be.name == be_name) {
            Some(index) => index,
            None => {
                return Err(Error::NotFound {
                    name: be_name.to_string(),
                });
            }
        };

        if temporary {
            // Set temporary activation (boot_once only)
            // Only one BE can have boot_once=true, and no BE should have next_boot=true when using temporary activation
            for be in bes.iter_mut() {
                be.boot_once = false;
                be.next_boot = false;
            }
            bes[target_index].boot_once = true;
        } else {
            // Permanent activation - this would normally require a reboot
            // For simulation purposes, we'll set it as the next boot environment
            // Only one BE can have next_boot=true, and no BE should have boot_once=true
            for be in bes.iter_mut() {
                be.next_boot = false;
                be.boot_once = false;
            }
            bes[target_index].next_boot = true;
        }

        Ok(())
    }

    fn deactivate(&self, be_name: &str) -> Result<(), Error> {
        let mut bes = self.bes.write().unwrap();
        let target_index = match bes.iter().position(|be| be.name == be_name) {
            Some(index) => index,
            None => {
                return Err(Error::NotFound {
                    name: be_name.to_string(),
                });
            }
        };
        bes[target_index].boot_once = false;
        Ok(())
    }

    fn rollback(&self, be_name: &str, _snapshot: &str) -> Result<(), Error> {
        if !self.bes.read().unwrap().iter().any(|be| be.name == be_name) {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }
        unimplemented!("Mocking does not yet track snapshots");
    }

    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error> {
        Ok(self.bes.read().unwrap().clone())
    }

    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, Error> {
        if !self.bes.read().unwrap().iter().any(|be| be.name == be_name) {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }
        Ok(sample_snapshots(be_name))
    }
}

fn sample_boot_environments() -> Vec<BootEnvironment> {
    vec![
        BootEnvironment {
            name: "default".to_string(),
            path: "zfake/ROOT/default".to_string(),
            guid: EmulatorClient::generate_guid("default"),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/")),
            active: true,
            next_boot: true,
            boot_once: false,
            space: 950_000_000,  // ~906M
            created: 1623301740, // 2021-06-10 01:09
        },
        BootEnvironment {
            name: "alt".to_string(),
            path: "zfake/ROOT/alt".to_string(),
            guid: EmulatorClient::generate_guid("alt"),
            description: Some("Testing".to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,         // 8K
            created: 1623305460, // 2021-06-10 02:11
        },
    ]
}

fn sample_snapshots(be_name: &str) -> Vec<Snapshot> {
    match be_name {
        "default" => vec![
            Snapshot {
                name: "default@2021-06-10-04:30".to_string(),
                path: "zfake/ROOT/default@2021-06-10-04:30".to_string(),
                space: 404_000,      // 404K
                created: 1623303000, // 2021-06-10 04:30
            },
            Snapshot {
                name: "default@2021-06-10-05:10".to_string(),
                path: "zfake/ROOT/default@2021-06-10-05:10".to_string(),
                space: 404_000,      // 404K
                created: 1623305400, // 2021-06-10 05:10
            },
        ],
        "alt" => vec![Snapshot {
            name: "alt@backup".to_string(),
            path: "zfake/ROOT/alt@backup".to_string(),
            space: 1024,         // 1K
            created: 1623306000, // 2021-06-10 05:06:40
        }],
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emulated_new() {
        let client = EmulatorClient::sampled();
        client
            .new("test-empty", Some("Empty BE"), None, &[])
            .unwrap();

        let bes = client.get_boot_environments().unwrap();
        let test_be = bes.iter().find(|be| be.name == "test-empty").unwrap();
        assert_eq!(test_be.description, Some("Empty BE".to_string()));
        assert_eq!(test_be.space, 8192);
    }

    #[test]
    fn test_emulated_new_conflict() {
        let client = EmulatorClient::sampled();
        let result = client.new("default", Some("Empty BE"), None, &[]);
        assert!(matches!(result, Err(Error::Conflict { .. })));
    }

    #[test]
    fn test_emulated_new_with_host_id() {
        let client = EmulatorClient::sampled();
        // Host ID is accepted but ignored in the mock implementation
        client
            .new("test-hostid", None, Some("test-host"), &[])
            .unwrap();

        let bes = client.get_boot_environments().unwrap();
        let test_be = bes.iter().find(|be| be.name == "test-hostid").unwrap();
        assert_eq!(test_be.description, None);
    }

    #[test]
    fn test_emulated_create() {
        let client = EmulatorClient::empty();

        // Test creating a new boot environment
        let result = client.create("test-be", Some("Test description"), None, &[]);
        assert!(result.is_ok());

        // Verify it was added
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "test-be");
        assert_eq!(bes[0].description, Some("Test description".to_string()));
        assert_eq!(bes[0].path, "zfake/ROOT/test-be");

        // Test creating a duplicate should fail
        let result = client.create("test-be", None, None, &[]);
        assert!(matches!(result, Err(Error::Conflict { name }) if name == "test-be"));

        // Verify we still have only one
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 1);
    }

    #[test]
    fn test_emulated_destroy_success() {
        // Create a test boot environment that can be destroyed
        let test_be = BootEnvironment {
            name: "destroyable".to_string(),
            path: "zfake/ROOT/destroyable".to_string(),
            guid: EmulatorClient::generate_guid("destroyable"),
            description: Some("Test BE for destruction".to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        // Verify it exists
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "destroyable");

        // Destroy it
        let result = client.destroy("destroyable", false, false, false);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 0);
    }

    #[test]
    fn test_emulated_destroy_not_found() {
        let client = EmulatorClient::empty();
        let result = client.destroy("nonexistent", false, false, false);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_destroy_active_be() {
        // Create an active boot environment
        let active_be = BootEnvironment {
            name: "active-be".to_string(),
            path: "zfake/ROOT/active-be".to_string(),
            guid: EmulatorClient::generate_guid("active-be"),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/")),
            active: true,
            next_boot: true,
            boot_once: false,
            space: 950_000_000,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![active_be]);

        // Try to destroy the active boot environment - should fail
        let result = client.destroy("active-be", false, false, false);
        assert!(matches!(result, Err(Error::CannotDestroyActive { name }) if name == "active-be"));

        // Verify it still exists
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "active-be");
    }

    #[test]
    fn test_emulated_destroy_mounted_be() {
        // Create a mounted boot environment
        let mounted_be = BootEnvironment {
            name: "mounted-be".to_string(),
            path: "zfake/ROOT/mounted-be".to_string(),
            guid: EmulatorClient::generate_guid("mounted-be"),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![mounted_be]);

        // Try to destroy without force_unmount - should fail
        let result = client.destroy("mounted-be", false, false, false);
        assert!(matches!(result, Err(Error::BeMounted { name, mountpoint })
            if name == "mounted-be" && mountpoint == "/mnt/test"));

        // Verify it still exists
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "mounted-be");

        // Try to destroy with force_unmount - should succeed
        let result = client.destroy("mounted-be", true, false, false);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 0);
    }

    #[test]
    fn test_emulated_create_and_destroy_integration() {
        let client = EmulatorClient::new(vec![]);

        // Start with empty
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 0);

        // Create a boot environment
        let result = client.create("temp-be", Some("Temporary BE"), None, &[]);
        assert!(result.is_ok());

        // Verify it exists
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "temp-be");
        assert_eq!(bes[0].description, Some("Temporary BE".to_string()));

        // Destroy it
        let result = client.destroy("temp-be", false, false, false);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 0);

        // Try to destroy it again - should fail
        let result = client.destroy("temp-be", false, false, false);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "temp-be"));
    }

    #[test]
    fn test_emulated_mount_success() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            path: "zfake/ROOT/test-be".to_string(),
            guid: EmulatorClient::generate_guid("test-be"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        // Mount the BE
        let result = client.mount("test-be", "/mnt/test", MountMode::ReadWrite);
        assert!(result.is_ok());

        // Verify it's mounted
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(
            bes[0].mountpoint,
            Some(std::path::PathBuf::from("/mnt/test"))
        );
    }

    #[test]
    fn test_emulated_mount_not_found() {
        let client = EmulatorClient::new(vec![]);
        let result = client.mount("nonexistent", "/mnt/test", MountMode::ReadWrite);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_mount_already_mounted() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            path: "zfake/ROOT/test-be".to_string(),
            guid: EmulatorClient::generate_guid("test-be"),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/mnt/existing")),
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };
        let client = EmulatorClient::new(vec![test_be]);
        let result = client.mount("test-be", "/mnt/test", MountMode::ReadWrite);
        assert!(matches!(result, Err(Error::BeMounted { name, mountpoint })
            if name == "test-be" && mountpoint == "/mnt/existing"));
    }

    #[test]
    fn test_emulated_mount_path_in_use() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            path: "zfake/ROOT/be1".to_string(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            path: "zfake/ROOT/be2".to_string(),
            guid: EmulatorClient::generate_guid("be2"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![be1, be2]);
        let result = client.mount("be2", "/mnt/test", MountMode::ReadWrite);
        assert!(matches!(result, Err(Error::MountPointInUse { path }) if path == "/mnt/test"));
    }

    #[test]
    fn test_emulated_unmount_success() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            path: "zfake/ROOT/test-be".to_string(),
            guid: EmulatorClient::generate_guid("test-be"),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        // Unmount by BE name
        let result = client.unmount("test-be", false);
        assert!(result.is_ok());

        // Verify it's unmounted
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes[0].mountpoint, None);
    }

    #[test]
    fn test_emulated_unmount_by_path() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            path: "zfake/ROOT/test-be".to_string(),
            guid: EmulatorClient::generate_guid("test-be"),
            description: None,
            mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        // Unmount by path
        let result = client.unmount("/mnt/test", false);
        assert!(result.is_ok());

        // Verify it's unmounted
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes[0].mountpoint, None);
    }

    #[test]
    fn test_emulated_unmount_not_mounted() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            path: "zfake/ROOT/test-be".to_string(),
            guid: EmulatorClient::generate_guid("test-be"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        let result = client.unmount("test-be", false);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_emulated_hostid() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            path: "zfake/ROOT/test-be".to_string(),
            guid: EmulatorClient::generate_guid("test-be"),
            description: None,
            mountpoint: Some(PathBuf::from("/mnt/test")), // Make it mounted
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        // Test hostid for existing mounted BE
        let result = client.hostid("test-be");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(0xdeadbeef));

        // Test hostid for non-existent BE
        let result = client.hostid("non-existent");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotFound { name } if name == "non-existent"));
    }

    #[test]
    fn test_emulated_hostid_not_found() {
        let test_be = BootEnvironment {
            name: "no-hostid-be".to_string(),
            path: "zfake/ROOT/no-hostid-be".to_string(),
            guid: EmulatorClient::generate_guid("no-hostid-be"),
            description: None,
            mountpoint: Some(PathBuf::from("/mnt/test")), // Make it mounted
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        // Test BE with no hostid (mounted)
        let result = client.hostid("no-hostid-be");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_emulated_hostid_not_mounted() {
        let test_be = BootEnvironment {
            name: "unmounted-be".to_string(),
            path: "zfake/ROOT/unmounted-be".to_string(),
            guid: EmulatorClient::generate_guid("unmounted-be"),
            description: None,
            mountpoint: None, // Not mounted
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        // Test hostid for unmounted BE - should return error
        let result = client.hostid("unmounted-be");
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::NotMounted { name } if name == "unmounted-be")
        );
    }

    #[test]
    fn test_emulated_rename_success() {
        let test_be = BootEnvironment {
            name: "old-name".to_string(),
            path: "zfake/ROOT/old-name".to_string(),
            guid: EmulatorClient::generate_guid("old-name"),
            description: Some("Test BE".to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);

        let result = client.rename("old-name", "new-name");
        assert!(result.is_ok());

        // Verify the rename
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes[0].name, "new-name");
        assert_eq!(bes[0].path, "zfake/ROOT/new-name");
        assert_eq!(bes[0].description, Some("Test BE".to_string()));
    }

    #[test]
    fn test_emulated_rename_not_found() {
        let client = EmulatorClient::empty();
        let result = client.rename("nonexistent", "new-name");
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_rename_conflict() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            path: "zfake/ROOT/be1".to_string(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            path: "zfake/ROOT/be2".to_string(),
            guid: EmulatorClient::generate_guid("be2"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![be1, be2]);

        let result = client.rename("be1", "be2");
        assert!(matches!(result, Err(Error::Conflict { name }) if name == "be2"));
    }

    #[test]
    fn test_emulated_activate_permanent() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            path: "zfake/ROOT/be1".to_string(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: true,
            next_boot: true,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            path: "zfake/ROOT/be2".to_string(),
            guid: EmulatorClient::generate_guid("be2"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![be1, be2]);

        // Activate be2 permanently
        let result = client.activate("be2", false);
        assert!(result.is_ok());

        // Verify activation
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[0].next_boot); // be1 should no longer be next_boot
        assert!(bes[1].next_boot); // be2 should be next_boot
    }

    #[test]
    fn test_emulated_activate_temporary() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            path: "zfake/ROOT/be1".to_string(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: true,
            next_boot: true,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            path: "zfake/ROOT/be2".to_string(),
            guid: EmulatorClient::generate_guid("be2"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![be1, be2]);

        // Activate be2 temporarily
        let result = client.activate("be2", true);
        assert!(result.is_ok());

        // Verify temporary activation
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[0].boot_once); // be1 should not have boot_once
        assert!(bes[1].boot_once); // be2 should have boot_once (temporary activation)
    }

    #[test]
    fn test_emulated_deactivate() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            path: "zfake/ROOT/be1".to_string(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: true,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            path: "zfake/ROOT/be2".to_string(),
            guid: EmulatorClient::generate_guid("be2"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![be1, be2]);

        // Remove temporary activation from be2
        let result = client.deactivate("be2");
        assert!(result.is_ok());

        // Verify temp activation removed
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[1].boot_once); // be2 should no longer have boot_once
    }

    #[test]
    fn test_emulated_deactivate_no_temp() {
        // Test deactivate when the specified boot environment doesn't have boot_once set
        let client = EmulatorClient::sampled();
        let result = client.deactivate("default");
        assert!(result.is_ok());

        // Verify the default BE still doesn't have boot_once set
        let bes = client.get_boot_environments().unwrap();
        let default_be = bes.iter().find(|be| be.name == "default").unwrap();
        assert!(!default_be.boot_once);
    }

    #[test]
    fn test_emulated_deactivate_not_found() {
        let client = EmulatorClient::sampled();
        let result = client.deactivate("nonexistent");
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_deactivate_specific_be() {
        // Test that deactivate only affects the specified boot environment
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            path: "zfake/ROOT/be1".to_string(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            path: "zfake/ROOT/be2".to_string(),
            guid: EmulatorClient::generate_guid("be2"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![be1, be2]);

        // First, temporarily activate be2
        client.activate("be2", true).unwrap();

        // Verify be2 is temporarily activated
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[0].boot_once); // be1 should not have boot_once
        assert!(bes[1].boot_once); // be2 should have boot_once

        // Now deactivate be2
        let result = client.deactivate("be2");
        assert!(result.is_ok());

        // Verify be2's boot_once flag was cleared
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[0].boot_once); // be1 should still not have boot_once
        assert!(!bes[1].boot_once); // be2 should no longer have boot_once
    }

    #[test]
    fn test_emulated_activate_mutual_exclusivity() {
        // Test that only one BE can have next_boot=true and only one can have boot_once=true
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            path: "zfake/ROOT/be1".to_string(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: true,
            next_boot: true, // Initially set as next boot
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            path: "zfake/ROOT/be2".to_string(),
            guid: EmulatorClient::generate_guid("be2"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![be1, be2]);

        // Activate be2 permanently - should clear be1's next_boot
        client.activate("be2", false).unwrap();
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[0].next_boot); // be1 should no longer be next_boot
        assert!(bes[1].next_boot); // be2 should now be next_boot
        assert!(!bes[0].boot_once); // no boot_once flags
        assert!(!bes[1].boot_once);

        // Activate be1 temporarily - should clear be2's next_boot and set be1's boot_once
        client.activate("be1", true).unwrap();
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[0].next_boot); // no next_boot flags when using temporary
        assert!(!bes[1].next_boot);
        assert!(bes[0].boot_once); // be1 should have boot_once
        assert!(!bes[1].boot_once); // be2 should not have boot_once

        // Activate be2 temporarily - should clear be1's boot_once and set be2's boot_once
        client.activate("be2", true).unwrap();
        let bes = client.get_boot_environments().unwrap();
        assert!(!bes[0].next_boot); // still no next_boot flags
        assert!(!bes[1].next_boot);
        assert!(!bes[0].boot_once); // be1 should no longer have boot_once
        assert!(bes[1].boot_once); // be2 should now have boot_once
    }

    #[test]
    fn test_emulated_activate_not_found() {
        let client = EmulatorClient::new(vec![]);
        let result = client.activate("nonexistent", false);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_create_or_rename_invalid_name() {
        let client = EmulatorClient::sampled();
        assert!(client.create("-invalid", None, None, &[]).is_err());
        assert!(client.create("invalid name", None, None, &[]).is_err());
        assert!(client.create("invalid@name", None, None, &[]).is_err());
        assert!(client.rename("default", "-invalid").is_err());
        assert!(client.rename("default", "invalid name").is_err());
        assert!(client.rename("default", "invalid@name").is_err());
    }

    #[test]
    fn test_emulated_integration_workflow() {
        let client = EmulatorClient::new(vec![]);

        // Create a boot environment
        let result = client.create("test-be", Some("Integration test"), None, &[]);
        assert!(result.is_ok());

        // Mount it
        let result = client.mount("test-be", "/mnt/test", MountMode::ReadWrite);
        assert!(result.is_ok());

        // Verify it's mounted
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(
            bes[0].mountpoint,
            Some(std::path::PathBuf::from("/mnt/test"))
        );

        // Unmount it
        let result = client.unmount("test-be", false);
        assert!(result.is_ok());

        // Verify it's unmounted
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes[0].mountpoint, None);

        // Rename it
        let result = client.rename("test-be", "renamed-be");
        assert!(result.is_ok());

        // Verify the rename
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes[0].name, "renamed-be");
        assert_eq!(bes[0].path, "zfake/ROOT/renamed-be");

        // Activate it temporarily
        let result = client.activate("renamed-be", true);
        assert!(result.is_ok());

        // Verify activation
        let bes = client.get_boot_environments().unwrap();
        assert!(bes[0].boot_once); // Should have boot_once for temporary activation

        // Destroy it (should work since it's not active)
        let result = client.destroy("renamed-be", false, false, false);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments().unwrap();
        assert_eq!(bes.len(), 0);
    }

    #[test]
    fn test_emulated_snapshots_success() {
        let client = EmulatorClient::sampled();

        // Get snapshots for default BE
        let snapshots = client.get_snapshots("default").unwrap();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].name, "default@2021-06-10-04:30");
        assert_eq!(snapshots[0].space, 404_000);
        assert_eq!(snapshots[0].created, 1623303000);
        assert_eq!(snapshots[1].name, "default@2021-06-10-05:10");
        assert_eq!(snapshots[1].space, 404_000);
        assert_eq!(snapshots[1].created, 1623305400);

        // Get snapshots for alt BE
        let snapshots = client.get_snapshots("alt").unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, "alt@backup");
        assert_eq!(snapshots[0].space, 1024);
        assert_eq!(snapshots[0].created, 1623306000);
    }

    #[test]
    fn test_emulated_snapshots_not_found() {
        let client = EmulatorClient::sampled();
        let result = client.get_snapshots("nonexistent");
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_snapshots_empty() {
        // Create a client with a BE that has no snapshots
        let test_be = BootEnvironment {
            name: "no-snapshots".to_string(),
            path: "zfake/ROOT/no-snapshots".to_string(),
            guid: EmulatorClient::generate_guid("no-snapshots"),
            description: None,
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![test_be]);
        let snapshots = client.get_snapshots("no-snapshots").unwrap();
        assert_eq!(snapshots.len(), 0);
    }
}
