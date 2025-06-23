use chrono::{Local, TimeZone};
use clap::{Parser, Subcommand, ValueEnum};

mod be {
    use chrono::Local;
    use clap::ValueEnum;
    use std::cell::RefCell;
    use std::path::PathBuf;
    use thiserror::Error as ThisError;

    #[derive(ThisError, Debug)]
    pub enum Error {
        #[error("Boot environment '{name}' not found")]
        NotFound { name: String },

        #[error("Boot environment '{name}' already exists")]
        Conflict { name: String },

        #[error("Mount point '{path}' is already in use")]
        MountPointInUse { path: String },

        #[error("Cannot destroy active boot environment '{name}'")]
        CannotDestroyActive { name: String },

        #[error("Cannot unmount boot environment '{name}': {reason}")]
        UnmountFailed { name: String, reason: String },

        #[error("Invalid boot environment name '{name}': {reason}")]
        InvalidName { name: String, reason: String },

        #[error("Boot environment name '{name}' is currently mounted at '{mountpoint}'")]
        BeMounted { name: String, mountpoint: String },

        #[error("ZFS operation failed: {message}")]
        ZfsError { message: String },

        #[error("I/O error: {0}")]
        Io(#[from] std::io::Error),

        #[error("Operation not supported in no-op mode")]
        NoOpError,
    }

    /// Whether a boot environment is mounted read-write (the default) or
    /// read-only.
    #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
    pub enum MountMode {
        /// Mount read-write.
        #[value(name = "rw")]
        ReadWrite,
        /// Mount read-only.
        #[value(name = "ro")]
        ReadOnly,
    }

    #[derive(Clone)]
    pub struct BootEnvironment {
        /// The name of this boot environment.
        pub name: String,
        /// The dataset backing this boot environment.
        #[allow(dead_code)]
        pub dataset: String,
        /// A description for this boot environment, if any.
        pub description: Option<String>,
        /// If the boot environment is currently mounted, this is its mountpoint.
        pub mountpoint: Option<PathBuf>,
        /// Whether the system is currently booted into this boot environment.
        pub active: bool,
        /// Whether the system will reboot into this environment.
        pub next_boot: bool,
        /// Bytes on the filesystem associated with this boot environment.
        pub space: u64,
        /// Unix timestamp for when this boot environment was created.
        pub created: i64,
    }

    /// Validates a boot environment name for ZFS dataset naming rules.
    pub fn validate_be_name(be_name: &str, beroot: &str) -> Result<(), Error> {
        // Total length including beroot prefix + '/' must be under 256 chars.
        if beroot.len() + be_name.len() > 255 {
            return Err(Error::InvalidName {
                name: be_name.to_string(),
                reason: "name too long".to_string(),
            });
        }

        // We could call out to zfs_validate_name() here but this is more fun!
        //
        // ZFS dataset names must match something like the regular expression
        // [a-zA-Z0-9][a-zA-Z0-9-_:. ]?.
        //
        // But FreeBSD is documented to break on boot environments that contain
        // spaces, so let's prohibit that, too.

        if be_name.is_empty() {
            return Err(Error::InvalidName {
                name: be_name.to_string(),
                reason: "name cannot be empty".to_string(),
            });
        }

        let first_char = be_name.chars().next().unwrap();
        if !first_char.is_ascii_alphanumeric() {
            return Err(Error::InvalidName {
                name: be_name.to_string(),
                reason: format!("name cannot begin with '{}'", first_char),
            });
        }

        for c in be_name.chars() {
            if !c.is_ascii_alphanumeric() && c != '.' && c != '-' && c != '_' && c != ':' {
                return Err(Error::InvalidName {
                    name: be_name.to_string(),
                    reason: format!("invalid character '{}' in name", c),
                });
            }
        }

        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_validate_be_name_valid() {
            // Valid names should pass
            assert!(validate_be_name("valid-name", "zfake/ROOT").is_ok());
            assert!(validate_be_name("test_env", "zfake/ROOT").is_ok());
            assert!(validate_be_name("env123", "zfake/ROOT").is_ok());
            assert!(validate_be_name("123numbers", "zfake/ROOT").is_ok());
            assert!(validate_be_name("test:colon", "zfake/ROOT").is_ok());
            assert!(validate_be_name("my.env", "zfake/ROOT").is_ok());
        }

        #[test]
        fn test_validate_be_name_invalid() {
            // Invalid names should fail
            assert!(validate_be_name("", "zfake/ROOT").is_err()); // empty
            assert!(validate_be_name("-invalid", "zfake/ROOT").is_err()); // starts with dash
            assert!(validate_be_name(".invalid", "zfake/ROOT").is_err()); // starts with dot
            assert!(validate_be_name("_invalid", "zfake/ROOT").is_err()); // starts with underscore
            assert!(validate_be_name("invalid name", "zfake/ROOT").is_err()); // space
            assert!(validate_be_name("invalid@name", "zfake/ROOT").is_err()); // invalid char
            assert!(validate_be_name("test/name", "zfake/ROOT").is_err()); // invalid char
        }

        #[test]
        fn test_validate_be_name_too_long() {
            let beroot = "zfake/ROOT";
            let long_name = "a".repeat(256 - beroot.len());
            assert!(validate_be_name(&long_name, beroot).is_err());
        }
    }

    pub trait Client {
        fn create(
            &self,
            be_name: &str,
            description: Option<&str>,
            source: Option<&str>,
            properties: &[String],
        ) -> Result<(), Error>;

        fn destroy(
            &self,
            target: &str,
            force_unmount: bool,
            force_no_verify: bool,
            snapshots: bool,
        ) -> Result<(), Error>;

        fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), Error>;

        fn unmount(&self, target: &str, force: bool) -> Result<(), Error>;

        fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error>;

        fn activate(&self, be_name: &str, temporary: bool, remove_temp: bool) -> Result<(), Error>;

        fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), Error>;

        /// Get a snapshot of the boot environments.
        fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error>;
    }

    pub mod mock {
        use super::*;

        fn sample_boot_environments() -> Vec<BootEnvironment> {
            vec![
                BootEnvironment {
                    name: "default".to_string(),
                    dataset: "zfake/ROOT/default".to_string(),
                    description: None,
                    mountpoint: Some(std::path::PathBuf::from("/")),
                    active: true,
                    next_boot: true,
                    space: 950_000_000,  // ~906M
                    created: 1623301740, // 2021-06-10 01:09
                },
                BootEnvironment {
                    name: "alt".to_string(),
                    dataset: "zfake/ROOT/alt".to_string(),
                    description: Some("Testing".to_string()),
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192,         // 8K
                    created: 1623305460, // 2021-06-10 02:11
                },
            ]
        }

        /// A boot environment client populated with static data that operates
        /// entirely in-memory with no side effects.
        pub struct EmulatorClient {
            root: String,
            bes: RefCell<Vec<BootEnvironment>>,
        }

        impl EmulatorClient {
            pub fn new(bes: Vec<BootEnvironment>) -> Self {
                Self {
                    root: "zfake/ROOT".to_string(),
                    bes: RefCell::new(bes),
                }
            }

            pub fn empty() -> Self {
                Self {
                    root: "zfake/ROOT".to_string(),
                    bes: RefCell::new(vec![]),
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

                let mut bes = self.bes.borrow_mut();

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
                    dataset: format!("{}/{}", self.root, be_name),
                    description: description.map(|s| s.to_string()),
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192, // ZFS datasets consume 8K to start.
                    created: Local::now().timestamp(),
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
                    let bes = self.bes.borrow();
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
                self.bes.borrow_mut().retain(|x| x.name != target);

                Ok(())
            }

            fn mount(
                &self,
                be_name: &str,
                mountpoint: &str,
                _mode: MountMode,
            ) -> Result<(), Error> {
                // First, validate preconditions with immutable borrow
                {
                    let bes = self.bes.borrow();

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
                let mut bes = self.bes.borrow_mut();
                if let Some(be) = bes.iter_mut().find(|be| be.name == be_name) {
                    be.mountpoint = Some(std::path::PathBuf::from(mountpoint));
                }

                Ok(())
            }

            fn unmount(&self, target: &str, _force: bool) -> Result<(), Error> {
                let mut bes = self.bes.borrow_mut();

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

                // Check if it's actually mounted
                if be.mountpoint.is_none() {
                    return Err(Error::UnmountFailed {
                        name: be.name.to_string(),
                        reason: "not mounted".to_string(),
                    });
                }

                // Unmount the BE
                be.mountpoint = None;
                Ok(())
            }

            fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error> {
                validate_be_name(new_name, &self.root)?;
                let mut bes = self.bes.borrow_mut();

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
                bes[be_index].dataset = format!("{}/{}", self.root, new_name);

                Ok(())
            }

            fn activate(
                &self,
                be_name: &str,
                temporary: bool,
                remove_temp: bool,
            ) -> Result<(), Error> {
                let mut bes = self.bes.borrow_mut();

                // Find the target boot environment
                let target_index = match bes.iter().position(|be| be.name == be_name) {
                    Some(index) => index,
                    None => {
                        return Err(Error::NotFound {
                            name: be_name.to_string(),
                        });
                    }
                };

                if remove_temp {
                    // Clear temporary activation flags
                    for be in bes.iter_mut() {
                        if be.next_boot && !be.active {
                            be.next_boot = false;
                        }
                    }
                } else if temporary {
                    // Set temporary activation (next_boot only)
                    for be in bes.iter_mut() {
                        be.next_boot = false;
                    }
                    bes[target_index].next_boot = true;
                } else {
                    // Permanent activation - this would normally require a reboot
                    // For simulation purposes, we'll set it as the next boot environment
                    for be in bes.iter_mut() {
                        be.next_boot = false;
                    }
                    bes[target_index].next_boot = true;
                }

                Ok(())
            }

            fn rollback(&self, be_name: &str, _snapshot: &str) -> Result<(), Error> {
                if !self.bes.borrow().iter().any(|be| be.name == be_name) {
                    return Err(Error::NotFound {
                        name: be_name.to_string(),
                    });
                }
                unimplemented!("Mocking does not yet track snapshots");
            }

            fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error> {
                Ok(self.bes.borrow().clone())
            }
        }

        #[cfg(test)]
        mod tests {
            use super::*;

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
                assert_eq!(bes[0].dataset, "zfake/ROOT/test-be");

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
                    dataset: "zfake/ROOT/destroyable".to_string(),
                    description: Some("Test BE for destruction".to_string()),
                    mountpoint: None,
                    active: false,
                    next_boot: false,
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
                    dataset: "zfake/ROOT/active-be".to_string(),
                    description: None,
                    mountpoint: Some(std::path::PathBuf::from("/")),
                    active: true,
                    next_boot: true,
                    space: 950_000_000,
                    created: 1623301740,
                };

                let client = EmulatorClient::new(vec![active_be]);

                // Try to destroy the active boot environment - should fail
                let result = client.destroy("active-be", false, false, false);
                assert!(
                    matches!(result, Err(Error::CannotDestroyActive { name }) if name == "active-be")
                );

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
                    dataset: "zfake/ROOT/mounted-be".to_string(),
                    description: None,
                    mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
                    active: false,
                    next_boot: false,
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
                    dataset: "zfake/ROOT/test-be".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: false,
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
                    dataset: "zfake/ROOT/test-be".to_string(),
                    description: None,
                    mountpoint: Some(std::path::PathBuf::from("/mnt/existing")),
                    active: false,
                    next_boot: false,
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
                    dataset: "zfake/ROOT/be1".to_string(),
                    description: None,
                    mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
                    active: false,
                    next_boot: false,
                    space: 8192,
                    created: 1623301740,
                };

                let be2 = BootEnvironment {
                    name: "be2".to_string(),
                    dataset: "zfake/ROOT/be2".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192,
                    created: 1623305460,
                };

                let client = EmulatorClient::new(vec![be1, be2]);
                let result = client.mount("be2", "/mnt/test", MountMode::ReadWrite);
                assert!(
                    matches!(result, Err(Error::MountPointInUse { path }) if path == "/mnt/test")
                );
            }

            #[test]
            fn test_emulated_unmount_success() {
                let test_be = BootEnvironment {
                    name: "test-be".to_string(),
                    dataset: "zfake/ROOT/test-be".to_string(),
                    description: None,
                    mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
                    active: false,
                    next_boot: false,
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
                    dataset: "zfake/ROOT/test-be".to_string(),
                    description: None,
                    mountpoint: Some(std::path::PathBuf::from("/mnt/test")),
                    active: false,
                    next_boot: false,
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
                    dataset: "zfake/ROOT/test-be".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192,
                    created: 1623301740,
                };

                let client = EmulatorClient::new(vec![test_be]);

                let result = client.unmount("test-be", false);
                assert!(matches!(result, Err(Error::UnmountFailed { name, reason })
                    if name == "test-be" && reason == "not mounted"));
            }

            #[test]
            fn test_emulated_rename_success() {
                let test_be = BootEnvironment {
                    name: "old-name".to_string(),
                    dataset: "zfake/ROOT/old-name".to_string(),
                    description: Some("Test BE".to_string()),
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192,
                    created: 1623301740,
                };

                let client = EmulatorClient::new(vec![test_be]);

                let result = client.rename("old-name", "new-name");
                assert!(result.is_ok());

                // Verify the rename
                let bes = client.get_boot_environments().unwrap();
                assert_eq!(bes[0].name, "new-name");
                assert_eq!(bes[0].dataset, "zfake/ROOT/new-name");
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
                    dataset: "zfake/ROOT/be1".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192,
                    created: 1623301740,
                };

                let be2 = BootEnvironment {
                    name: "be2".to_string(),
                    dataset: "zfake/ROOT/be2".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: false,
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
                    dataset: "zfake/ROOT/be1".to_string(),
                    description: None,
                    mountpoint: None,
                    active: true,
                    next_boot: true,
                    space: 8192,
                    created: 1623301740,
                };

                let be2 = BootEnvironment {
                    name: "be2".to_string(),
                    dataset: "zfake/ROOT/be2".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192,
                    created: 1623305460,
                };

                let client = EmulatorClient::new(vec![be1, be2]);

                // Activate be2 permanently
                let result = client.activate("be2", false, false);
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
                    dataset: "zfake/ROOT/be1".to_string(),
                    description: None,
                    mountpoint: None,
                    active: true,
                    next_boot: true,
                    space: 8192,
                    created: 1623301740,
                };

                let be2 = BootEnvironment {
                    name: "be2".to_string(),
                    dataset: "zfake/ROOT/be2".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: false,
                    space: 8192,
                    created: 1623305460,
                };

                let client = EmulatorClient::new(vec![be1, be2]);

                // Activate be2 temporarily
                let result = client.activate("be2", true, false);
                assert!(result.is_ok());

                // Verify temporary activation
                let bes = client.get_boot_environments().unwrap();
                assert!(!bes[0].next_boot); // be1 should no longer be next_boot
                assert!(bes[1].next_boot); // be2 should be next_boot
            }

            #[test]
            fn test_emulated_activate_remove_temp() {
                let be1 = BootEnvironment {
                    name: "be1".to_string(),
                    dataset: "zfake/ROOT/be1".to_string(),
                    description: None,
                    mountpoint: None,
                    active: true,
                    next_boot: false,
                    space: 8192,
                    created: 1623301740,
                };

                let be2 = BootEnvironment {
                    name: "be2".to_string(),
                    dataset: "zfake/ROOT/be2".to_string(),
                    description: None,
                    mountpoint: None,
                    active: false,
                    next_boot: true, // Temporarily activated
                    space: 8192,
                    created: 1623305460,
                };

                let client = EmulatorClient::new(vec![be1, be2]);

                // Remove temporary activation
                let result = client.activate("be1", false, true);
                assert!(result.is_ok());

                // Verify temp activation removed
                let bes = client.get_boot_environments().unwrap();
                assert!(!bes[1].next_boot); // be2 should no longer be next_boot
            }

            #[test]
            fn test_emulated_activate_not_found() {
                let client = EmulatorClient::new(vec![]);
                let result = client.activate("nonexistent", false, false);
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
                assert_eq!(bes[0].dataset, "zfake/ROOT/renamed-be");

                // Activate it temporarily
                let result = client.activate("renamed-be", true, false);
                assert!(result.is_ok());

                // Verify activation
                let bes = client.get_boot_environments().unwrap();
                assert!(bes[0].next_boot);

                // Destroy it (should work since it's not active)
                let result = client.destroy("renamed-be", false, false, false);
                assert!(result.is_ok());

                // Verify it's gone
                let bes = client.get_boot_environments().unwrap();
                assert_eq!(bes.len(), 0);
            }
        }
    }
}

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
