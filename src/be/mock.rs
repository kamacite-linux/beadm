// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::Utc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::RwLock;

use super::validation::{validate_be_name, validate_component};
use super::{
    BootEnvironment, Client, Error, Label, MountMode, Root, Snapshot, generate_snapshot_name,
    generate_temp_mountpoint,
};

/// A boot environment client populated with static data that operates
/// entirely in-memory with no side effects.
pub struct EmulatorClient {
    active_root: Root,
    bes: RwLock<Vec<BootEnvironment>>,
}

impl EmulatorClient {
    pub fn new(bes: Vec<BootEnvironment>) -> Self {
        Self {
            active_root: Root::from_str("zfake/ROOT").unwrap(),
            bes: RwLock::new(bes),
        }
    }

    /// Generate a fake GUID based on the boot environment name
    pub fn generate_guid(be_name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        be_name.hash(&mut hasher);
        hasher.finish()
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            active_root: Root::from_str("zfake/ROOT").unwrap(),
            bes: RwLock::new(vec![]),
        }
    }

    pub fn sampled() -> Self {
        Self::new(sample_boot_environments())
    }

    /// Get the effective root to use for an operation.
    fn effective_root<'a>(&'a self, root: Option<&'a Root>) -> &'a Root {
        root.unwrap_or(&self.active_root)
    }
}

impl Client for EmulatorClient {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&Label>,
        _properties: &[String],
        root: Option<&Root>,
    ) -> Result<(), Error> {
        let root = self.effective_root(root);
        validate_be_name(be_name, root.as_str())?;

        let mut bes = self.bes.write().unwrap();

        let source_space = match source {
            Some(Label::Snapshot(name, snapshot)) => {
                // Case #1: beadm create -e EXISTING@SNAPSHOT NAME, which
                // creates the clone from an existing snapshot of a boot
                // environment.

                validate_component(name, true)?;
                validate_component(snapshot, false)?;

                // Check if the source boot environment exists with matching root
                let source_be = bes
                    .iter()
                    .find(|be| be.name == *name && be.root == *root)
                    .ok_or_else(|| Error::not_found(&format!("{}@{}", name, snapshot)))?;

                // Clone from snapshot - inherit space from source BE
                source_be.space
            }
            Some(Label::Name(name)) => {
                // Case #2: beadm create -e EXISTING NAME, which creates the
                // clone from a new snapshot of a source boot environment.
                // Validate that src is a valid component (not a path)
                validate_component(name, true)?;

                // Find the source boot environment to clone with matching root
                let source_be = bes
                    .iter()
                    .find(|be| be.name == *name && be.root == *root)
                    .ok_or_else(|| Error::not_found(name))?;

                // Clone from existing BE - inherit space
                source_be.space
            }
            None => {
                // Case #3: beadm create NAME, which creates the clone from a
                // snapshot of the active boot environment.
                let active_be = bes
                    .iter()
                    .find(|be| be.active && be.root == *root)
                    .ok_or_else(|| Error::NoActiveBootEnvironment)?;

                // Clone from active BE - inherit space
                active_be.space
            }
        };

        // Check for conflicts after determining the source is valid (only within the same root)
        if bes.iter().any(|be| be.name == be_name && be.root == *root) {
            return Err(Error::conflict(be_name));
        }

        bes.push(BootEnvironment {
            name: be_name.to_string(),
            root: root.clone(),
            guid: Self::generate_guid(be_name),
            description: description.map(|s| s.to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: source_space, // Inherit space from source
            created: Utc::now().timestamp(),
        });
        Ok(())
    }

    fn create_empty(
        &self,
        be_name: &str,
        description: Option<&str>,
        _host_id: Option<&str>,
        _properties: &[String],
        root: Option<&Root>,
    ) -> Result<(), Error> {
        let root = self.effective_root(root);
        let mut bes = self.bes.write().unwrap();

        // Check for conflicts (only within the same root).
        if bes.iter().any(|be| be.name == be_name && be.root == *root) {
            return Err(Error::conflict(be_name));
        }

        // Create new empty boot environment
        bes.push(BootEnvironment {
            name: be_name.to_string(),
            root: root.clone(),
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
        target: &Label,
        force_unmount: bool,
        snapshots: bool,
        root: Option<&Root>,
    ) -> Result<(), Error> {
        let root = self.effective_root(root);

        match target {
            Label::Name(be_name) => {
                // Destroy a boot environment
                // First, check if the BE exists and validate constraints
                {
                    let bes = self.bes.read().unwrap();
                    let be = match bes
                        .iter()
                        .find(|be| be.name == *be_name && be.root == *root)
                    {
                        Some(be) => be,
                        None => {
                            return Err(Error::NotFound {
                                name: be_name.to_string(),
                            });
                        }
                    };

                    if be.active {
                        return Err(Error::CannotDestroyActive {
                            name: be.name.to_string(),
                        });
                    }

                    if !force_unmount && be.mountpoint.is_some() {
                        return Err(Error::Mounted {
                            name: be.name.to_string(),
                            mountpoint: be.mountpoint.as_ref().unwrap().display().to_string(),
                        });
                    }
                } // Release the borrow here

                if snapshots {
                    unimplemented!("Mocking does not yet track snapshots");
                }

                // Now we can safely borrow mutably to remove the BE (matching both name and root)
                self.bes
                    .write()
                    .unwrap()
                    .retain(|x| !(x.name == *be_name && x.root == *root));

                Ok(())
            }
            Label::Snapshot(be_name, _snapshot_name) => {
                // Destroy a snapshot - for mock implementation, we just validate the BE exists with matching root
                let bes = self.bes.read().unwrap();
                if !bes.iter().any(|be| be.name == *be_name && be.root == *root) {
                    return Err(Error::not_found(be_name));
                }

                // For mock implementation, snapshots are generated on-the-fly
                // so we can't actually destroy them, but we can pretend to succeed
                Ok(())
            }
        }
    }

    fn mount(
        &self,
        be_name: &str,
        mountpoint: Option<&Path>,
        _mode: MountMode,
        root: Option<&Root>,
    ) -> Result<PathBuf, Error> {
        let root = self.effective_root(root);
        let mut bes = self.bes.write().unwrap();

        // Find the boot environment with matching root
        let be = match bes.iter().find(|be| be.name == be_name && be.root == *root) {
            Some(be) => be,
            None => {
                return Err(Error::NotFound {
                    name: be_name.to_string(),
                });
            }
        };

        // Check if it's already mounted
        if let Some(ref existing) = be.mountpoint {
            if mountpoint.map_or_else(|| false, |mp| mp == existing) {
                // We're already done.
                return Ok(existing.clone());
            }
            return Err(Error::Mounted {
                name: be_name.to_string(),
                mountpoint: existing.display().to_string(),
            });
        }

        let mountpoint = if let Some(mp) = mountpoint {
            // Check if another BE is already mounted at this path
            if bes.iter().any(|other_be| {
                other_be
                    .mountpoint
                    .as_ref()
                    .map_or(false, |existing| existing == mp)
            }) {
                return Err(Error::MountPointInUse {
                    path: mp.display().to_string(),
                });
            }
            mp.to_path_buf()
        } else {
            // Note: this won't actually create the directory.
            generate_temp_mountpoint()
        };

        let be = bes.iter_mut().find(|be| be.name == be_name).unwrap();
        be.mountpoint = Some(mountpoint.clone());
        Ok(mountpoint)
    }

    fn unmount(
        &self,
        target: &str,
        _force: bool,
        root: Option<&Root>,
    ) -> Result<Option<PathBuf>, Error> {
        let root = self.effective_root(root);
        let mut bes = self.bes.write().unwrap();

        // Target can be either a BE name or a mountpoint path (with matching root)
        let be = match bes.iter_mut().find(|be| {
            be.root == *root
                && (be.name == target
                    || be
                        .mountpoint
                        .as_ref()
                        .map_or(false, |mp| mp.display().to_string() == target))
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

    fn hostid(&self, be_name: &str, root: Option<&Root>) -> Result<Option<u32>, Error> {
        let root = self.effective_root(root);
        let bes = self.bes.read().unwrap();

        // Find the boot environment with matching root
        let be = match bes.iter().find(|be| be.name == be_name && be.root == *root) {
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

    fn rename(&self, be_name: &str, new_name: &str, root: Option<&Root>) -> Result<(), Error> {
        let root = self.effective_root(root);
        validate_be_name(new_name, root.as_str())?;
        let mut bes = self.bes.write().unwrap();

        // Check if source BE exists with matching root
        let be_index = match bes
            .iter()
            .position(|be| be.name == be_name && be.root == *root)
        {
            Some(index) => index,
            None => {
                return Err(Error::NotFound {
                    name: be_name.to_string(),
                });
            }
        };

        // Check if new name already exists (only within the same root)
        if bes.iter().any(|be| be.name == new_name && be.root == *root) {
            return Err(Error::Conflict {
                name: new_name.to_string(),
            });
        }

        // Perform the rename
        bes[be_index].name = new_name.to_string();

        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool, root: Option<&Root>) -> Result<(), Error> {
        let root = self.effective_root(root);
        let mut bes = self.bes.write().unwrap();

        // Find the target boot environment with matching root
        let target_index = match bes
            .iter()
            .position(|be| be.name == be_name && be.root == *root)
        {
            Some(index) => index,
            None => {
                return Err(Error::NotFound {
                    name: be_name.to_string(),
                });
            }
        };

        if temporary {
            // Set temporary activation (boot_once only)
            // Only one BE can have boot_once=true within the same root, and no BE should have next_boot=true when using temporary activation
            for be in bes.iter_mut().filter(|be| be.root == *root) {
                be.boot_once = false;
                be.next_boot = false;
            }
            bes[target_index].boot_once = true;
        } else {
            // Permanent activation - this would normally require a reboot
            // For simulation purposes, we'll set it as the next boot environment
            // Only one BE can have next_boot=true within the same root, and no BE should have boot_once=true
            for be in bes.iter_mut().filter(|be| be.root == *root) {
                be.next_boot = false;
                be.boot_once = false;
            }
            bes[target_index].next_boot = true;
        }

        Ok(())
    }

    fn clear_boot_once(&self, root: Option<&Root>) -> Result<(), Error> {
        let root = self.effective_root(root);
        let mut bes = self.bes.write().unwrap();

        let temporary_be_index = bes.iter().position(|be| be.boot_once && be.root == *root);
        if temporary_be_index.is_none() {
            return Ok(()); // Nothing to clear.
        }

        if let Some(index) = temporary_be_index {
            bes[index].boot_once = false;
        }

        // Since the mock doesn't store the previously-activated boot
        // environment explicitly, we simulate the restoration by finding the
        // *active* boot environment (within the same root) and setting that as next_boot.
        if let Some(active_index) = bes.iter().position(|be| be.active && be.root == *root) {
            bes[active_index].next_boot = true;
        }

        Ok(())
    }

    fn rollback(&self, be_name: &str, _snapshot: &str, root: Option<&Root>) -> Result<(), Error> {
        let root = self.effective_root(root);
        if !self
            .bes
            .read()
            .unwrap()
            .iter()
            .any(|be| be.name == be_name && be.root == *root)
        {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }
        unimplemented!("Mocking does not yet track snapshots");
    }

    fn get_boot_environments(&self, root: Option<&Root>) -> Result<Vec<BootEnvironment>, Error> {
        let root = self.effective_root(root);
        Ok(self
            .bes
            .read()
            .unwrap()
            .iter()
            .filter(|be| be.root == *root)
            .cloned()
            .collect())
    }

    fn get_snapshots(&self, be_name: &str, root: Option<&Root>) -> Result<Vec<Snapshot>, Error> {
        let root = self.effective_root(root);
        if !self
            .bes
            .read()
            .unwrap()
            .iter()
            .any(|be| be.name == be_name && be.root == *root)
        {
            return Err(Error::NotFound {
                name: be_name.to_string(),
            });
        }
        Ok(sample_snapshots(be_name))
    }

    fn snapshot(
        &self,
        source: Option<&Label>,
        _description: Option<&str>,
        root: Option<&Root>,
    ) -> Result<String, Error> {
        let root = self.effective_root(root);
        let (name, snapshot) = match source {
            Some(label) => match label {
                Label::Name(name) => (name.clone(), generate_snapshot_name()),
                Label::Snapshot(name, snapshot) => (name.clone(), snapshot.clone()),
            },
            None => {
                // Form: beadm snapshot (snapshot active BE with auto-generated name)
                let bes = self.bes.read().unwrap();
                let active_be = bes
                    .iter()
                    .find(|be| be.active && be.root == *root)
                    .ok_or_else(|| Error::NoActiveBootEnvironment)?;
                (active_be.name.clone(), generate_snapshot_name())
            }
        };

        // Ensure the boot environment exists with matching root
        if !self
            .bes
            .read()
            .unwrap()
            .iter()
            .any(|be| be.name == name && be.root == *root)
        {
            return Err(Error::not_found(&name));
        }

        // In a real implementation, we would add the snapshot to storage with the
        // description, but for the mock client we just validate and return the name.
        // The description parameter is accepted but ignored in the mock.
        Ok(format!("{}@{}", name, snapshot))
    }

    fn init(&self, pool: &str) -> Result<(), Error> {
        // For the mock implementation, we simply validate the pool name format
        // and simulate success.
        if pool.is_empty() || pool.contains('/') || pool.contains('@') {
            return Err(Error::InvalidName {
                name: pool.to_string(),
                reason: "pool name cannot contain '/' or '@' characters or be empty".to_string(),
            });
        }
        Ok(())
    }

    fn describe(
        &self,
        target: &Label,
        description: &str,
        root: Option<&Root>,
    ) -> Result<(), Error> {
        let root = self.effective_root(root);
        match target {
            Label::Snapshot(name, _snapshot) => {
                // For mock implementation, we can't actually modify snapshots
                // since they're generated on-the-fly, but we validate at least
                // that the boot environment exists with matching root and then pretend to succeed.
                if !self
                    .bes
                    .read()
                    .unwrap()
                    .iter()
                    .any(|be| be.name == *name && be.root == *root)
                {
                    return Err(Error::not_found(name));
                }
                Ok(())
            }
            Label::Name(name) => {
                let mut bes = self.bes.write().unwrap();
                if let Some(be) = bes
                    .iter_mut()
                    .find(|be| be.name == *name && be.root == *root)
                {
                    be.description = Some(description.to_string());
                    Ok(())
                } else {
                    Err(Error::not_found(name))
                }
            }
        }
    }
}

fn sample_boot_environments() -> Vec<BootEnvironment> {
    vec![
        BootEnvironment {
            name: "default".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
            root: Root::from_str("zfake/ROOT").unwrap(),
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
                root: Root::from_str("zfake/ROOT").unwrap(),
                description: Some("Automatic snapshot".to_string()),
                space: 404_000,      // 404K
                created: 1623303000, // 2021-06-10 04:30
            },
            Snapshot {
                name: "default@2021-06-10-05:10".to_string(),
                root: Root::from_str("zfake/ROOT").unwrap(),
                description: None,
                space: 404_000,      // 404K
                created: 1623305400, // 2021-06-10 05:10
            },
        ],
        "alt" => vec![Snapshot {
            name: "alt@backup".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
            description: Some("Manual backup".to_string()),
            space: 1024,         // 1K
            created: 1623306000, // 2021-06-10 05:06:40
        }],
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_emulated_new() {
        let client = EmulatorClient::sampled();
        client
            .create_empty("test-empty", Some("Empty BE"), None, &[], None)
            .unwrap();

        let bes = client.get_boot_environments(None).unwrap();
        let test_be = bes.iter().find(|be| be.name == "test-empty").unwrap();
        assert_eq!(test_be.description, Some("Empty BE".to_string()));
        assert_eq!(test_be.space, 8192);
    }

    #[test]
    fn test_emulated_new_conflict() {
        let client = EmulatorClient::sampled();
        let result = client.create_empty("default", Some("Empty BE"), None, &[], None);
        assert!(matches!(result, Err(Error::Conflict { .. })));
    }

    #[test]
    fn test_emulated_new_with_host_id() {
        let client = EmulatorClient::sampled();
        // Host ID is accepted but ignored in the mock implementation
        client
            .create_empty("test-hostid", None, Some("test-host"), &[], None)
            .unwrap();

        let bes = client.get_boot_environments(None).unwrap();
        let test_be = bes.iter().find(|be| be.name == "test-hostid").unwrap();
        assert_eq!(test_be.description, None);
    }

    #[test]
    fn test_emulated_create() {
        let client = EmulatorClient::empty();

        // Test creating without a source when there's no active BE should fail
        let result = client.create("test-be", Some("Test description"), None, &[], None);
        assert!(matches!(result, Err(Error::NoActiveBootEnvironment)));

        // Create a source BE first using create_empty
        client
            .create_empty("source-be", None, None, &[], None)
            .unwrap();

        // Mark it as active so we can clone from it
        let mut bes = client.bes.write().unwrap();
        bes[0].active = true;
        drop(bes);

        // Now creating from active BE should work
        let result = client.create("test-be", Some("Test description"), None, &[], None);
        assert!(result.is_ok());

        // Verify it was added
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 2);
        let test_be = bes.iter().find(|be| be.name == "test-be").unwrap();
        assert_eq!(test_be.description, Some("Test description".to_string()));

        // Test creating a duplicate should fail
        let result = client.create("test-be", None, None, &[], None);
        assert!(matches!(result, Err(Error::Conflict { name }) if name == "test-be"));

        // Verify we still have only two
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 2);
    }

    #[test]
    fn test_emulated_destroy_success() {
        // Create a test boot environment that can be destroyed
        let test_be = BootEnvironment {
            name: "destroyable".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "destroyable");

        // Destroy it
        let result = client.destroy(&Label::Name("destroyable".to_string()), false, false, None);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 0);
    }

    #[test]
    fn test_emulated_destroy_not_found() {
        let client = EmulatorClient::empty();
        let result = client.destroy(&Label::Name("nonexistent".to_string()), false, false, None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_destroy_active_be() {
        // Create an active boot environment
        let active_be = BootEnvironment {
            name: "active-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.destroy(&Label::Name("active-be".to_string()), false, false, None);
        assert!(matches!(result, Err(Error::CannotDestroyActive { name }) if name == "active-be"));

        // Verify it still exists
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "active-be");
    }

    #[test]
    fn test_emulated_destroy_mounted_be() {
        // Create a mounted boot environment
        let mounted_be = BootEnvironment {
            name: "mounted-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.destroy(&Label::Name("mounted-be".to_string()), false, false, None);
        assert!(matches!(result, Err(Error::Mounted { name, mountpoint })
            if name == "mounted-be" && mountpoint == "/mnt/test"));

        // Verify it still exists
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "mounted-be");

        // Try to destroy with force_unmount - should succeed
        let result = client.destroy(&Label::Name("mounted-be".to_string()), true, false, None);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 0);
    }

    #[test]
    fn test_emulated_create_and_destroy_integration() {
        let client = EmulatorClient::new(vec![]);

        // Start with empty
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 0);

        // Create a boot environment using create_empty (since there's no active BE)
        let result = client.create_empty("temp-be", Some("Temporary BE"), None, &[], None);
        assert!(result.is_ok());

        // Verify it exists
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "temp-be");
        assert_eq!(bes[0].description, Some("Temporary BE".to_string()));

        // Destroy it
        let result = client.destroy(&Label::Name("temp-be".to_string()), false, false, None);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 0);

        // Try to destroy it again - should fail
        let result = client.destroy(&Label::Name("temp-be".to_string()), false, false, None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "temp-be"));
    }

    #[test]
    fn test_emulated_mount_success() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let path = PathBuf::from("/mnt/test");
        let result = client.mount("test-be", Some(path.as_path()), MountMode::ReadWrite, None);
        assert!(result.is_ok());

        // Verify it's mounted
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(
            bes[0].mountpoint,
            Some(std::path::PathBuf::from("/mnt/test"))
        );
    }

    #[test]
    fn test_emulated_mount_not_found() {
        let client = EmulatorClient::new(vec![]);
        let result = client.mount("nonexistent", None, MountMode::ReadWrite, None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_mount_already_mounted() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let path = PathBuf::from("/mnt/test");
        let result = client.mount("test-be", Some(path.as_path()), MountMode::ReadWrite, None);
        assert!(matches!(result, Err(Error::Mounted { name, mountpoint })
            if name == "test-be" && mountpoint == "/mnt/existing"));
    }

    #[test]
    fn test_emulated_mount_path_in_use() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let path = PathBuf::from("/mnt/test");
        let result = client.mount("be2", Some(path.as_path()), MountMode::ReadWrite, None);
        assert!(matches!(result, Err(Error::MountPointInUse { path }) if path == "/mnt/test"));
    }

    #[test]
    fn test_emulated_unmount_success() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.unmount("test-be", false, None);
        assert!(result.is_ok());

        // Verify it's unmounted
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes[0].mountpoint, None);
    }

    #[test]
    fn test_emulated_unmount_by_path() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.unmount("/mnt/test", false, None);
        assert!(result.is_ok());

        // Verify it's unmounted
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes[0].mountpoint, None);
    }

    #[test]
    fn test_emulated_unmount_not_mounted() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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

        let result = client.unmount("test-be", false, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_emulated_hostid() {
        let test_be = BootEnvironment {
            name: "test-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.hostid("test-be", None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(0xdeadbeef));

        // Test hostid for non-existent BE
        let result = client.hostid("non-existent", None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotFound { name } if name == "non-existent"));
    }

    #[test]
    fn test_emulated_hostid_not_found() {
        let test_be = BootEnvironment {
            name: "no-hostid-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.hostid("no-hostid-be", None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_emulated_hostid_not_mounted() {
        let test_be = BootEnvironment {
            name: "unmounted-be".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.hostid("unmounted-be", None);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::NotMounted { name } if name == "unmounted-be")
        );
    }

    #[test]
    fn test_emulated_rename_success() {
        let test_be = BootEnvironment {
            name: "old-name".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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

        let result = client.rename("old-name", "new-name", None);
        assert!(result.is_ok());

        // Verify the rename
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes[0].name, "new-name");
        assert_eq!(bes[0].description, Some("Test BE".to_string()));
    }

    #[test]
    fn test_emulated_rename_not_found() {
        let client = EmulatorClient::empty();
        let result = client.rename("nonexistent", "new-name", None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_rename_conflict() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
            root: Root::from_str("zfake/ROOT").unwrap(),
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

        let result = client.rename("be1", "be2", None);
        assert!(matches!(result, Err(Error::Conflict { name }) if name == "be2"));
    }

    #[test]
    fn test_emulated_activate_permanent() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.activate("be2", false, None);
        assert!(result.is_ok());

        // Verify activation
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].next_boot); // be1 should no longer be next_boot
        assert!(bes[1].next_boot); // be2 should be next_boot
    }

    #[test]
    fn test_emulated_activate_temporary() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let result = client.activate("be2", true, None);
        assert!(result.is_ok());

        // Verify temporary activation
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].boot_once); // be1 should not have boot_once
        assert!(bes[1].boot_once); // be2 should have boot_once (temporary activation)
    }

    #[test]
    fn test_emulated_activate_mutual_exclusivity() {
        // Test that only one BE can have next_boot=true and only one can have boot_once=true
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        client.activate("be2", false, None).unwrap();
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].next_boot); // be1 should no longer be next_boot
        assert!(bes[1].next_boot); // be2 should now be next_boot
        assert!(!bes[0].boot_once); // no boot_once flags
        assert!(!bes[1].boot_once);

        // Activate be1 temporarily - should clear be2's next_boot and set be1's boot_once
        client.activate("be1", true, None).unwrap();
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].next_boot); // no next_boot flags when using temporary
        assert!(!bes[1].next_boot);
        assert!(bes[0].boot_once); // be1 should have boot_once
        assert!(!bes[1].boot_once); // be2 should not have boot_once

        // Activate be2 temporarily - should clear be1's boot_once and set be2's boot_once
        client.activate("be2", true, None).unwrap();
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].next_boot); // still no next_boot flags
        assert!(!bes[1].next_boot);
        assert!(!bes[0].boot_once); // be1 should no longer have boot_once
        assert!(bes[1].boot_once); // be2 should now have boot_once
    }

    #[test]
    fn test_emulated_activate_not_found() {
        let client = EmulatorClient::new(vec![]);
        let result = client.activate("nonexistent", false, None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_create_or_rename_invalid_name() {
        let client = EmulatorClient::sampled();
        assert!(client.create("-invalid", None, None, &[], None).is_err());
        assert!(
            client
                .create("invalid name", None, None, &[], None)
                .is_err()
        );
        assert!(
            client
                .create("invalid@name", None, None, &[], None)
                .is_err()
        );
        assert!(client.rename("default", "-invalid", None).is_err());
        assert!(client.rename("default", "invalid name", None).is_err());
        assert!(client.rename("default", "invalid@name", None).is_err());
    }

    #[test]
    fn test_emulated_integration_workflow() {
        let client = EmulatorClient::new(vec![]);

        // Create a boot environment using create_empty (no active BE yet)
        let result = client.create_empty("test-be", Some("Integration test"), None, &[], None);
        assert!(result.is_ok());

        // Mount it
        let result = client.mount("test-be", None, MountMode::ReadWrite, None);
        assert!(result.is_ok());

        // Verify it's mounted
        let bes = client.get_boot_environments(None).unwrap();
        assert!(bes[0].mountpoint.is_some());

        // Unmount it
        let result = client.unmount("test-be", false, None);
        assert!(result.is_ok());

        // Verify it's unmounted
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes[0].mountpoint, None);

        // Rename it
        let result = client.rename("test-be", "renamed-be", None);
        assert!(result.is_ok());

        // Verify the rename
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes[0].name, "renamed-be");

        // Activate it temporarily
        let result = client.activate("renamed-be", true, None);
        assert!(result.is_ok());

        // Verify activation
        let bes = client.get_boot_environments(None).unwrap();
        assert!(bes[0].boot_once); // Should have boot_once for temporary activation

        // Destroy it (should work since it's not active)
        let result = client.destroy(&Label::Name("renamed-be".to_string()), false, false, None);
        assert!(result.is_ok());

        // Verify it's gone
        let bes = client.get_boot_environments(None).unwrap();
        assert_eq!(bes.len(), 0);
    }

    #[test]
    fn test_emulated_snapshots_success() {
        let client = EmulatorClient::sampled();

        // Get snapshots for default BE
        let snapshots = client.get_snapshots("default", None).unwrap();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].name, "default@2021-06-10-04:30");
        assert_eq!(snapshots[0].space, 404_000);
        assert_eq!(snapshots[0].created, 1623303000);
        assert_eq!(snapshots[1].name, "default@2021-06-10-05:10");
        assert_eq!(snapshots[1].space, 404_000);
        assert_eq!(snapshots[1].created, 1623305400);

        // Get snapshots for alt BE
        let snapshots = client.get_snapshots("alt", None).unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, "alt@backup");
        assert_eq!(snapshots[0].space, 1024);
        assert_eq!(snapshots[0].created, 1623306000);
    }

    #[test]
    fn test_emulated_snapshots_not_found() {
        let client = EmulatorClient::sampled();
        let result = client.get_snapshots("nonexistent", None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_snapshots_empty() {
        // Create a client with a BE that has no snapshots
        let test_be = BootEnvironment {
            name: "no-snapshots".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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
        let snapshots = client.get_snapshots("no-snapshots", None).unwrap();
        assert_eq!(snapshots.len(), 0);
    }

    #[test]
    fn test_emulated_create_from_existing() {
        let client = EmulatorClient::sampled();

        // Create a new BE from an existing one
        let result = client.create(
            "from-default",
            Some("Cloned from default"),
            Some(&Label::from_str("default").unwrap()),
            &[],
            None,
        );
        assert!(result.is_ok());

        // Verify it was created with inherited space from the source
        let bes = client.get_boot_environments(None).unwrap();
        let new_be = bes.iter().find(|be| be.name == "from-default").unwrap();
        assert_eq!(new_be.description, Some("Cloned from default".to_string()));
        // Should inherit space from default (950_000_000)
        assert_eq!(new_be.space, 950_000_000);
    }

    #[test]
    fn test_emulated_create_from_snapshot() {
        let client = EmulatorClient::sampled();

        // Create a new BE from a snapshot
        let result = client.create(
            "from-snapshot",
            Some("From snapshot"),
            Some(&Label::from_str("default@2021-06-10-04:30").unwrap()),
            &[],
            None,
        );
        assert!(result.is_ok());

        // Verify it was created with inherited space from the source BE
        let bes = client.get_boot_environments(None).unwrap();
        let new_be = bes.iter().find(|be| be.name == "from-snapshot").unwrap();
        assert_eq!(new_be.description, Some("From snapshot".to_string()));
        // Should inherit space from default (950_000_000)
        assert_eq!(new_be.space, 950_000_000);
    }

    #[test]
    fn test_emulated_create_from_active() {
        let client = EmulatorClient::sampled();

        // Create a new BE from the active one (no source specified)
        let result = client.create("from-active", Some("Cloned from active"), None, &[], None);
        assert!(result.is_ok());

        // Verify it was created with inherited space from the active BE
        let bes = client.get_boot_environments(None).unwrap();
        let new_be = bes.iter().find(|be| be.name == "from-active").unwrap();
        assert_eq!(new_be.description, Some("Cloned from active".to_string()));
        // Should inherit space from default (active BE, 950_000_000)
        assert_eq!(new_be.space, 950_000_000);
    }

    #[test]
    fn test_emulated_create_from_nonexistent_source() {
        let client = EmulatorClient::sampled();

        // Try to create from non-existent BE
        let result = client.create(
            "from-nonexistent",
            None,
            Some(&Label::from_str("nonexistent").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));

        // Try to create from non-existent snapshot
        let result = client.create(
            "from-bad-snapshot",
            None,
            Some(&Label::from_str("nonexistent@snap").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::NotFound { .. })));
    }

    #[test]
    fn test_emulated_create_invalid_snapshot_format() {
        let _client = EmulatorClient::sampled();

        // Try to create from invalid snapshot format - this should fail at parse time
        let parse_result = Label::from_str("default@snap@extra");
        assert!(matches!(parse_result, Err(Error::InvalidName { .. })));
    }

    #[test]
    fn test_emulated_create_invalid_components() {
        let client = EmulatorClient::sampled();

        // Try to create from source with invalid characters (path-like)
        let result = client.create(
            "new-be",
            None,
            Some(&Label::from_str("zroot/ROOT/default").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::InvalidName { .. })));

        // Try to create from source with invalid component name
        let result = client.create(
            "new-be",
            None,
            Some(&Label::from_str("-invalid").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::InvalidName { .. })));

        // Try to create from snapshot with invalid BE component
        let result = client.create(
            "new-be",
            None,
            Some(&Label::from_str("zroot/ROOT/default@snap").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::InvalidName { .. })));

        // Try to create from snapshot with invalid snapshot component
        let result = client.create(
            "new-be",
            None,
            Some(&Label::from_str("default@invalid#name").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::InvalidName { .. })));

        // Try to create from snapshot with space in BE name
        let result = client.create(
            "new-be",
            None,
            Some(&Label::from_str("invalid name@snap").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::InvalidName { .. })));

        // Try to create from snapshot with space in snapshot name
        let result = client.create(
            "new-be",
            None,
            Some(&Label::from_str("default@invalid snap").unwrap()),
            &[],
            None,
        );
        assert!(matches!(result, Err(Error::InvalidName { .. })));
    }

    #[test]
    fn test_emulated_clear_boot_once_success() {
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: true, // This is the currently active BE
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623301740,
        };

        let be2 = BootEnvironment {
            name: "be2".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
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

        // First, activate be2 temporarily
        client.activate("be2", true, None).unwrap();

        // Verify be2 is temporarily activated
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].boot_once); // be1 should not have boot_once
        assert!(!bes[0].next_boot); // be1 should not have next_boot (cleared by temporary activation)
        assert!(bes[1].boot_once); // be2 should have boot_once

        // Now clear the temporary activation
        let result = client.clear_boot_once(None);
        assert!(result.is_ok());

        // Verify the state is restored
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].boot_once); // be1 should not have boot_once
        assert!(bes[0].next_boot); // be1 should be restored as next_boot (it was active)
        assert!(!bes[1].boot_once); // be2 should no longer have boot_once
        assert!(!bes[1].next_boot); // be2 should not have next_boot
    }

    #[test]
    fn test_emulated_clear_boot_once_no_temporary() {
        // Test clear_boot_once when no temporary boot environment is active
        let client = EmulatorClient::sampled();

        // Verify no temporary activation is set
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes.iter().any(|be| be.boot_once));

        // Try to clear nextboot when no temporary activation exists
        let result = client.clear_boot_once(None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_emulated_clear_boot_once_no_active() {
        // Test clear_boot_once when there's a temporary BE but no active BE
        let be1 = BootEnvironment {
            name: "be1".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
            guid: EmulatorClient::generate_guid("be1"),
            description: None,
            mountpoint: None,
            active: false, // No active BE
            next_boot: false,
            boot_once: true, // Temporary activation
            space: 8192,
            created: 1623301740,
        };

        let client = EmulatorClient::new(vec![be1]);

        // Clear nextboot should work even when there's no active BE
        let result = client.clear_boot_once(None);
        assert!(result.is_ok());

        // Verify temporary activation was cleared
        let bes = client.get_boot_environments(None).unwrap();
        assert!(!bes[0].boot_once); // boot_once should be cleared
        assert!(!bes[0].next_boot); // next_boot should remain false (no active BE to restore)
    }

    #[test]
    fn test_emulated_clear_boot_once_integration() {
        // Complete integration test showing the full temporary activation/clear cycle
        let active_be = BootEnvironment {
            name: "current".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
            guid: EmulatorClient::generate_guid("current"),
            description: Some("Currently active BE".to_string()),
            mountpoint: Some(PathBuf::from("/")),
            active: true,    // This is the current active BE
            next_boot: true, // Initially set as next boot
            boot_once: false,
            space: 950_000_000,
            created: 1623301740,
        };

        let temp_be = BootEnvironment {
            name: "temporary".to_string(),
            root: Root::from_str("zfake/ROOT").unwrap(),
            guid: EmulatorClient::generate_guid("temporary"),
            description: Some("For temporary testing".to_string()),
            mountpoint: None,
            active: false,
            next_boot: false,
            boot_once: false,
            space: 8192,
            created: 1623305460,
        };

        let client = EmulatorClient::new(vec![active_be, temp_be]);

        // Verify initial state
        let bes = client.get_boot_environments(None).unwrap();
        assert!(
            bes.iter()
                .any(|be| be.name == "current" && be.active && be.next_boot)
        );
        assert!(bes.iter().any(|be| be.name == "temporary" && !be.boot_once));

        // Activate temporary BE temporarily
        client.activate("temporary", true, None).unwrap();

        // Verify temporary activation
        let bes = client.get_boot_environments(None).unwrap();
        let current = bes.iter().find(|be| be.name == "current").unwrap();
        let temporary = bes.iter().find(|be| be.name == "temporary").unwrap();

        assert!(current.active); // Still the active BE
        assert!(!current.next_boot); // No longer next_boot (cleared by temporary activation)
        assert!(!current.boot_once); // Not temporarily activated
        assert!(!temporary.active); // Not the active BE
        assert!(!temporary.next_boot); // Not permanently activated
        assert!(temporary.boot_once); // Temporarily activated

        // Clear the temporary activation
        let result = client.clear_boot_once(None);
        assert!(result.is_ok());

        // Verify the state is restored
        let bes = client.get_boot_environments(None).unwrap();
        let current = bes.iter().find(|be| be.name == "current").unwrap();
        let temporary = bes.iter().find(|be| be.name == "temporary").unwrap();

        assert!(current.active); // Still the active BE
        assert!(current.next_boot); // Restored as next_boot
        assert!(!current.boot_once); // Not temporarily activated
        assert!(!temporary.active); // Not the active BE
        assert!(!temporary.next_boot); // Not permanently activated
        assert!(!temporary.boot_once); // Temporary activation cleared
    }

    #[test]
    fn test_emulated_describe_boot_environment() {
        let client = EmulatorClient::sampled();

        // Get initial state
        let bes = client.get_boot_environments(None).unwrap();
        let alt_be = bes.iter().find(|be| be.name == "alt").unwrap();
        assert_eq!(alt_be.description, Some("Testing".to_string()));

        // Change the description
        let target = Label::from_str("alt").unwrap();
        let result = client.describe(&target, "Updated description", None);
        assert!(result.is_ok());

        // Verify the description was changed
        let bes = client.get_boot_environments(None).unwrap();
        let alt_be = bes.iter().find(|be| be.name == "alt").unwrap();
        assert_eq!(alt_be.description, Some("Updated description".to_string()));

        // Test setting description on boot environment without description
        let target = Label::from_str("default").unwrap();
        let result = client.describe(&target, "New description for default", None);
        assert!(result.is_ok());

        let bes = client.get_boot_environments(None).unwrap();
        let default_be = bes.iter().find(|be| be.name == "default").unwrap();
        assert_eq!(
            default_be.description,
            Some("New description for default".to_string())
        );
    }

    #[test]
    fn test_emulated_describe_not_found() {
        let client = EmulatorClient::sampled();

        // Try to describe a non-existent boot environment
        let target = Label::from_str("nonexistent").unwrap();
        let result = client.describe(&target, "Some description", None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));
    }

    #[test]
    fn test_emulated_describe_snapshot() {
        let client = EmulatorClient::sampled();

        // Test describing a snapshot - should succeed (mock implementation validates format)
        let target = Label::from_str("default@2021-06-10-04:30").unwrap();
        let result = client.describe(&target, "Updated snapshot description", None);
        assert!(result.is_ok());

        // Test describing a snapshot with non-existent BE
        let target = Label::from_str("nonexistent@snapshot").unwrap();
        let result = client.describe(&target, "Description", None);
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "nonexistent"));

        // Note: Invalid snapshot formats like "default@snap@extra" are now handled
        // by Target::FromStr during parsing, so this test is no longer needed here.
    }

    #[test]
    fn test_emulated_describe_empty_description() {
        let client = EmulatorClient::sampled();

        // Test setting empty description
        let target = Label::from_str("alt").unwrap();
        let result = client.describe(&target, "", None);
        assert!(result.is_ok());

        let bes = client.get_boot_environments(None).unwrap();
        let alt_be = bes.iter().find(|be| be.name == "alt").unwrap();
        assert_eq!(alt_be.description, Some("".to_string()));
    }

    // Tests for root parameter functionality

    #[test]
    fn test_root_parameter_get_boot_environments_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Get boot environments with matching root
        let bes = client.get_boot_environments(Some(&root)).unwrap();
        assert_eq!(bes.len(), 2);
        assert!(bes.iter().all(|be| be.root == root));
    }

    #[test]
    fn test_root_parameter_get_boot_environments_with_mismatched_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Get boot environments with non-matching root - should return empty
        let bes = client.get_boot_environments(Some(&other_root)).unwrap();
        assert_eq!(bes.len(), 0);
    }

    #[test]
    fn test_root_parameter_create_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Create with matching root should work
        let result = client.create("test-be", Some("Test"), None, &[], Some(&root));
        assert!(result.is_ok());

        let bes = client.get_boot_environments(Some(&root)).unwrap();
        let test_be = bes.iter().find(|be| be.name == "test-be").unwrap();
        assert_eq!(test_be.root, root);
    }

    #[test]
    fn test_root_parameter_create_with_different_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Create with different root but no active BE in that root should fail
        let result = client.create("test-be", Some("Test"), None, &[], Some(&other_root));
        assert!(matches!(result, Err(Error::NoActiveBootEnvironment)));

        // Use create_empty to create a BE in the other root
        let result = client.create_empty("test-be", Some("Test"), None, &[], Some(&other_root));
        assert!(result.is_ok());

        // Should be in the other root
        let bes = client.get_boot_environments(Some(&other_root)).unwrap();
        assert_eq!(bes.len(), 1);
        assert_eq!(bes[0].name, "test-be");
        assert_eq!(bes[0].root, other_root);

        // Should not be in the default root
        let default_root = Root::from_str("zfake/ROOT").unwrap();
        let bes = client.get_boot_environments(Some(&default_root)).unwrap();
        assert!(!bes.iter().any(|be| be.name == "test-be"));
    }

    #[test]
    fn test_root_parameter_destroy_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Destroy with matching root should work
        let result = client.destroy(&Label::Name("alt".to_string()), false, false, Some(&root));
        assert!(result.is_ok());

        let bes = client.get_boot_environments(Some(&root)).unwrap();
        assert!(!bes.iter().any(|be| be.name == "alt"));
    }

    #[test]
    fn test_root_parameter_destroy_with_mismatched_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Destroy with non-matching root should fail
        let result = client.destroy(
            &Label::Name("alt".to_string()),
            false,
            false,
            Some(&other_root),
        );
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "alt"));

        // Verify alt still exists in default root
        let default_root = Root::from_str("zfake/ROOT").unwrap();
        let bes = client.get_boot_environments(Some(&default_root)).unwrap();
        assert!(bes.iter().any(|be| be.name == "alt"));
    }

    #[test]
    fn test_root_parameter_mount_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Mount with matching root should work
        let result = client.mount("alt", None, MountMode::ReadWrite, Some(&root));
        assert!(result.is_ok());

        let bes = client.get_boot_environments(Some(&root)).unwrap();
        let alt_be = bes.iter().find(|be| be.name == "alt").unwrap();
        assert!(alt_be.mountpoint.is_some());
    }

    #[test]
    fn test_root_parameter_mount_with_mismatched_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Mount with non-matching root should fail
        let result = client.mount("alt", None, MountMode::ReadWrite, Some(&other_root));
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "alt"));
    }

    #[test]
    fn test_root_parameter_activate_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Activate with matching root should work
        let result = client.activate("alt", false, Some(&root));
        assert!(result.is_ok());

        let bes = client.get_boot_environments(Some(&root)).unwrap();
        let alt_be = bes.iter().find(|be| be.name == "alt").unwrap();
        assert!(alt_be.next_boot);
    }

    #[test]
    fn test_root_parameter_activate_with_mismatched_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Activate with non-matching root should fail
        let result = client.activate("alt", false, Some(&other_root));
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "alt"));
    }

    #[test]
    fn test_root_parameter_multiple_roots_same_client() {
        let client = EmulatorClient::empty();
        let root1 = Root::from_str("zpool1/ROOT").unwrap();
        let root2 = Root::from_str("zpool2/ROOT").unwrap();

        // Create BEs in different roots with the same name (using create_empty since no active BEs)
        client
            .create_empty("same-name", Some("In root1"), None, &[], Some(&root1))
            .unwrap();
        client
            .create_empty("same-name", Some("In root2"), None, &[], Some(&root2))
            .unwrap();

        // Each root should see only its own BE
        let bes1 = client.get_boot_environments(Some(&root1)).unwrap();
        assert_eq!(bes1.len(), 1);
        assert_eq!(bes1[0].description, Some("In root1".to_string()));

        let bes2 = client.get_boot_environments(Some(&root2)).unwrap();
        assert_eq!(bes2.len(), 1);
        assert_eq!(bes2[0].description, Some("In root2".to_string()));

        // Destroying in one root should not affect the other
        client
            .destroy(
                &Label::Name("same-name".to_string()),
                false,
                false,
                Some(&root1),
            )
            .unwrap();

        let bes1 = client.get_boot_environments(Some(&root1)).unwrap();
        assert_eq!(bes1.len(), 0);

        let bes2 = client.get_boot_environments(Some(&root2)).unwrap();
        assert_eq!(bes2.len(), 1);
    }

    #[test]
    fn test_root_parameter_create_from_source_in_same_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Create from source in the same root should work
        let result = client.create(
            "clone-of-default",
            None,
            Some(&Label::from_str("default").unwrap()),
            &[],
            Some(&root),
        );
        assert!(result.is_ok());

        let bes = client.get_boot_environments(Some(&root)).unwrap();
        assert!(bes.iter().any(|be| be.name == "clone-of-default"));
    }

    #[test]
    fn test_root_parameter_create_from_source_in_different_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Create from source in a different root should fail (source not found)
        let result = client.create(
            "clone-of-default",
            None,
            Some(&Label::from_str("default").unwrap()),
            &[],
            Some(&other_root),
        );
        assert!(matches!(result, Err(Error::NotFound { .. })));
    }

    #[test]
    fn test_root_parameter_rename_within_same_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Rename within the same root should work
        let result = client.rename("alt", "alt-renamed", Some(&root));
        assert!(result.is_ok());

        let bes = client.get_boot_environments(Some(&root)).unwrap();
        assert!(bes.iter().any(|be| be.name == "alt-renamed"));
        assert!(!bes.iter().any(|be| be.name == "alt"));
    }

    #[test]
    fn test_root_parameter_rename_conflict_only_in_same_root() {
        let client = EmulatorClient::empty();
        let root1 = Root::from_str("zpool1/ROOT").unwrap();
        let root2 = Root::from_str("zpool2/ROOT").unwrap();

        // Create "target" in root1 and "source" in root2 (using create_empty since no active BEs)
        client
            .create_empty("target", None, None, &[], Some(&root1))
            .unwrap();
        client
            .create_empty("source", None, None, &[], Some(&root2))
            .unwrap();

        // Rename source to target in root2 should work (no conflict across roots)
        let result = client.rename("source", "target", Some(&root2));
        assert!(result.is_ok());

        // Both roots should now have a BE named "target"
        let bes1 = client.get_boot_environments(Some(&root1)).unwrap();
        assert_eq!(bes1.len(), 1);
        assert_eq!(bes1[0].name, "target");

        let bes2 = client.get_boot_environments(Some(&root2)).unwrap();
        assert_eq!(bes2.len(), 1);
        assert_eq!(bes2[0].name, "target");
    }

    #[test]
    fn test_root_parameter_activate_only_affects_same_root() {
        let client = EmulatorClient::empty();
        let root1 = Root::from_str("zpool1/ROOT").unwrap();
        let root2 = Root::from_str("zpool2/ROOT").unwrap();

        // Create BEs in different roots (using create_empty since no active BEs)
        client
            .create_empty("be1", None, None, &[], Some(&root1))
            .unwrap();
        client
            .create_empty("be2", None, None, &[], Some(&root1))
            .unwrap();
        client
            .create_empty("be3", None, None, &[], Some(&root2))
            .unwrap();

        // Activate be1 in root1
        client.activate("be1", false, Some(&root1)).unwrap();

        // Check that only be1 is activated in root1
        let bes1 = client.get_boot_environments(Some(&root1)).unwrap();
        assert!(bes1.iter().find(|be| be.name == "be1").unwrap().next_boot);
        assert!(!bes1.iter().find(|be| be.name == "be2").unwrap().next_boot);

        // Check that be3 in root2 is not affected
        let bes2 = client.get_boot_environments(Some(&root2)).unwrap();
        assert!(!bes2.iter().find(|be| be.name == "be3").unwrap().next_boot);
    }

    #[test]
    fn test_root_parameter_get_snapshots_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Get snapshots with matching root should work
        let result = client.get_snapshots("default", Some(&root));
        assert!(result.is_ok());
        assert!(result.unwrap().len() > 0);
    }

    #[test]
    fn test_root_parameter_get_snapshots_with_mismatched_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Get snapshots with non-matching root should fail
        let result = client.get_snapshots("default", Some(&other_root));
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "default"));
    }

    #[test]
    fn test_root_parameter_snapshot_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Snapshot with matching root should work
        let result = client.snapshot(
            Some(&Label::from_str("default").unwrap()),
            Some("Test snapshot"),
            Some(&root),
        );
        assert!(result.is_ok());
        assert!(result.unwrap().starts_with("default@"));
    }

    #[test]
    fn test_root_parameter_snapshot_with_mismatched_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Snapshot with non-matching root should fail
        let result = client.snapshot(
            Some(&Label::from_str("default").unwrap()),
            Some("Test snapshot"),
            Some(&other_root),
        );
        assert!(matches!(result, Err(Error::NotFound { .. })));
    }

    #[test]
    fn test_root_parameter_describe_with_matching_root() {
        let client = EmulatorClient::sampled();
        let root = Root::from_str("zfake/ROOT").unwrap();

        // Describe with matching root should work
        let result = client.describe(
            &Label::from_str("alt").unwrap(),
            "Updated description",
            Some(&root),
        );
        assert!(result.is_ok());

        let bes = client.get_boot_environments(Some(&root)).unwrap();
        let alt_be = bes.iter().find(|be| be.name == "alt").unwrap();
        assert_eq!(alt_be.description, Some("Updated description".to_string()));
    }

    #[test]
    fn test_root_parameter_describe_with_mismatched_root() {
        let client = EmulatorClient::sampled();
        let other_root = Root::from_str("zother/ROOT").unwrap();

        // Describe with non-matching root should fail
        let result = client.describe(
            &Label::from_str("alt").unwrap(),
            "Updated description",
            Some(&other_root),
        );
        assert!(matches!(result, Err(Error::NotFound { name }) if name == "alt"));
    }
}
