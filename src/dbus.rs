// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::be::Error as BeError;
use crate::be::{BootEnvironment, Client, Label, MountMode, Root, Snapshot};
use event_listener::Listener;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use tracing;
use tracing_subscriber;
use zbus::object_server::SignalEmitter;
use zbus::{blocking, interface};
use zvariant::ObjectPath;

// D-Bus service constants
const SERVICE_NAME: &str = "ca.kamacite.BootEnvironments1";
const MANAGER_INTERFACE: &str = "ca.kamacite.BootEnvironmentManager";
const BOOT_ENV_INTERFACE: &str = "ca.kamacite.BootEnvironment";
const BOOT_ENV_PATH: &str = "/ca/kamacite/BootEnvironments";

/// Translate a boot environment GUID to a D-Bus object path.
fn be_object_path(guid: u64) -> ObjectPath<'static> {
    // This is safe to unwrap because hex strings are always valid object path components.
    ObjectPath::try_from(format!("{}/{:016x}", BOOT_ENV_PATH, guid)).unwrap()
}

// A D-Bus proxy (remote object) for boot environment administration.
//
// Implements the traditional `beadm` commands as D-Bus method calls.
pub struct ClientProxy {
    connection: blocking::Connection,
}

impl ClientProxy {
    /// Connect to the beadm D-Bus service or return an error if either the
    /// service or D-Bus itself is unavailable.
    ///
    /// This will also ping the D-Bus service to check if it's available.
    pub fn new() -> Result<Self, BeError> {
        // This is equivalent to async_io::block_on(zbus::Connection::system())?.
        let connection = zbus::blocking::Connection::system()?;
        connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some("org.freedesktop.DBus.Peer"),
            "Ping",
            &(),
        )?;
        Ok(Self { connection })
    }
}

impl Client for ClientProxy {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&Label>,
        properties: &[String],
        root: Option<&Root>,
    ) -> Result<(), BeError> {
        let desc = description.unwrap_or("");
        let src = source.map(|label| label.to_string()).unwrap_or_default();
        let props: Vec<String> = properties.to_vec();
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();

        let _result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Create",
                &(be_name, desc, src, props, beroot),
            )?
            .body()
            .deserialize()?;

        Ok(())
    }

    fn create_empty(
        &self,
        be_name: &str,
        description: Option<&str>,
        host_id: Option<&str>,
        properties: &[String],
        root: Option<&Root>,
    ) -> Result<(), BeError> {
        let desc = description.unwrap_or("");
        let hid = host_id.unwrap_or("");
        let props: Vec<String> = properties.to_vec();
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();

        let _result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "CreateEmpty",
                &(be_name, desc, hid, props, beroot),
            )?
            .body()
            .deserialize()?;

        Ok(())
    }

    fn destroy(
        &self,
        target: &Label,
        force_unmount: bool,
        snapshots: bool,
        root: Option<&Root>,
    ) -> Result<(), BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        match target {
            Label::Name(name) => self.connection.call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Destroy",
                &(name, force_unmount, snapshots, beroot),
            ),
            Label::Snapshot(name, snapshot) => self.connection.call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "DestroySnapshot",
                &(name, snapshot, beroot),
            ),
        }?;
        Ok(())
    }

    fn mount(
        &self,
        be_name: &str,
        mountpoint: Option<&Path>,
        mode: MountMode,
        root: Option<&Root>,
    ) -> Result<PathBuf, BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        let read_only = match mode {
            MountMode::ReadOnly => true,
            MountMode::ReadWrite => false,
        };
        let mountpoint = mountpoint.map_or("".to_string(), |mp| mp.to_string_lossy().to_string());
        let result: PathBuf = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Mount",
                &(be_name, mountpoint, read_only, beroot),
            )?
            .body()
            .deserialize()?;
        Ok(result)
    }

    fn unmount(
        &self,
        be_name: &str,
        force: bool,
        root: Option<&Root>,
    ) -> Result<Option<PathBuf>, BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        let result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Unmount",
                &(be_name, force, beroot),
            )?
            .body()
            .deserialize()?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PathBuf::from(result)))
        }
    }

    fn hostid(&self, _be_name: &str, _root: Option<&Root>) -> Result<Option<u32>, BeError> {
        // TODO: Decide whether to implement this.
        Ok(None)
    }

    fn rename(&self, be_name: &str, new_name: &str, root: Option<&Root>) -> Result<(), BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Rename",
            &(be_name, new_name, beroot),
        )?;
        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool, root: Option<&Root>) -> Result<(), BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Activate",
            &(be_name, temporary, beroot),
        )?;
        Ok(())
    }

    fn clear_boot_once(&self, root: Option<&Root>) -> Result<(), BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "ClearBootOnce",
            &(beroot,),
        )?;
        Ok(())
    }

    fn rollback(&self, be_name: &str, snapshot: &str, root: Option<&Root>) -> Result<(), BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Rollback",
            &(be_name, snapshot, beroot),
        )?;
        Ok(())
    }

    fn get_boot_environments(&self, _root: Option<&Root>) -> Result<Vec<BootEnvironment>, BeError> {
        let body = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some("org.freedesktop.DBus.ObjectManager"),
                "GetManagedObjects",
                &(),
            )?
            .body();

        let managed_objects: BTreeMap<ObjectPath, BTreeMap<String, BootEnvironment>> =
            body.deserialize()?;

        // TODO: Filter out results based on root.
        let mut boot_environments = Vec::new();
        for (_path, interfaces) in managed_objects {
            if let Some(be) = interfaces.get(BOOT_ENV_INTERFACE) {
                boot_environments.push(be.clone());
            }
        }
        Ok(boot_environments)
    }

    fn get_snapshots(&self, be_name: &str, root: Option<&Root>) -> Result<Vec<Snapshot>, BeError> {
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        let snapshots_data: Vec<(String, Root, String, u64, i64)> = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "GetSnapshots",
                &(be_name, beroot),
            )?
            .body()
            .deserialize()?;

        let snapshots = snapshots_data
            .into_iter()
            .map(|(name, root, description, space, created)| Snapshot {
                name,
                root,
                description: if description.is_empty() {
                    None
                } else {
                    Some(description)
                },
                space,
                created,
            })
            .collect();

        Ok(snapshots)
    }

    fn snapshot(
        &self,
        source: Option<&Label>,
        description: Option<&str>,
        root: Option<&Root>,
    ) -> Result<String, BeError> {
        let src = source.map(|label| label.to_string()).unwrap_or_default();
        let desc = description.unwrap_or("");
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        let result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Snapshot",
                &(src, desc, beroot),
            )?
            .body()
            .deserialize()?;
        Ok(result)
    }

    fn init(&self, pool: &str) -> Result<(), BeError> {
        let _result: () = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Init",
                &(pool,),
            )?
            .body()
            .deserialize()?;
        Ok(())
    }

    fn describe(
        &self,
        target: &Label,
        description: &str,
        root: Option<&Root>,
    ) -> Result<(), BeError> {
        let target_str = target.to_string();
        let beroot = root.map(|r| r.as_str()).unwrap_or_default();
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Describe",
            &(target_str, description, beroot),
        )?;
        Ok(())
    }
}

// ============================================================================
// D-Bus Server (BeadmServer and related components)
// ============================================================================

/// Individual boot environment D-Bus object
#[derive(Clone)]
pub struct BootEnvironmentObject<T> {
    data: Arc<RwLock<BootEnvironment>>,
    client: Arc<T>,
}

impl<T: Client + 'static> BootEnvironmentObject<T> {
    pub fn new(data: BootEnvironment, client: Arc<T>) -> Self {
        Self {
            data: Arc::new(RwLock::new(data)),
            client,
        }
    }

    /// Synchronize the object with the current state of the boot environment
    /// and emit property changed signals as needed.
    pub async fn sync(
        &self,
        current: BootEnvironment,
        signal_emitter: &SignalEmitter<'_>,
    ) -> zbus::fdo::Result<()> {
        // Check if any fields have actually changed.
        struct Changed {
            name: bool,
            root: bool,
            description: bool,
            mountpoint: bool,
            next_boot: bool,
            boot_once: bool,
            space: bool,
        }
        let changed = self
            .data
            .read() // Use map() to simplify guard lifetime across await calls below.
            .map(|stored| Changed {
                name: stored.name != current.name,
                root: stored.root != current.root,
                description: stored.description != current.description,
                mountpoint: stored.mountpoint != current.mountpoint,
                next_boot: stored.next_boot != current.next_boot,
                boot_once: stored.boot_once != current.boot_once,
                space: stored.space != current.space,
            })
            .expect("Failed to acquire read lock");

        if !(changed.name
            || changed.root
            || changed.description
            || changed.mountpoint
            || changed.next_boot
            || changed.boot_once
            || changed.space)
        {
            return Ok(());
        }

        {
            *self.data.write().expect("Failed to acquire write lock") = current;
        } // Write lock dropped.

        // Emit signals now that the data has been updated (and the write lock
        // released).
        if changed.name {
            self.name_changed(signal_emitter).await?;
        }
        if changed.root {
            self.root_changed(signal_emitter).await?;
        }
        if changed.description {
            self.description_changed(signal_emitter).await?;
        }
        if changed.mountpoint {
            self.mountpoint_changed(signal_emitter).await?;
        }
        if changed.next_boot {
            self.next_boot_changed(signal_emitter).await?;
        }
        if changed.boot_once {
            self.boot_once_changed(signal_emitter).await?;
        }
        if changed.space {
            self.space_changed(signal_emitter).await?;
        }

        Ok(())
    }
}

#[interface(name = "ca.kamacite.BootEnvironment")]
impl<T: Client + 'static> BootEnvironmentObject<T> {
    /// The name of this boot environment.
    #[zbus(property)]
    fn name(&self) -> String {
        self.data.read().unwrap().name.clone()
    }

    /// The boot environment root.
    #[zbus(property)]
    fn root(&self) -> String {
        self.data.read().unwrap().root.as_str().to_string()
    }

    /// The ZFS dataset GUID.
    #[zbus(property(emits_changed_signal = "const"))]
    fn guid(&self) -> u64 {
        self.data.read().unwrap().guid
    }

    /// A description for this boot environment, if any.
    #[zbus(property)]
    fn description(&self) -> String {
        self.data
            .read()
            .unwrap()
            .description
            .clone()
            .unwrap_or_default()
    }

    /// If the boot environment is currently mounted, this is its mountpoint.
    #[zbus(property)]
    fn mountpoint(&self) -> String {
        self.data
            .read()
            .unwrap()
            .mountpoint
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default()
    }

    /// Whether the system is currently booted into this boot environment.
    #[zbus(property(emits_changed_signal = "const"))]
    fn active(&self) -> bool {
        self.data.read().unwrap().active
    }

    /// Whether the system will reboot into this environment.
    #[zbus(property)]
    fn next_boot(&self) -> bool {
        self.data.read().unwrap().next_boot
    }

    /// Whether the system will reboot into this environment temporarily.
    #[zbus(property)]
    fn boot_once(&self) -> bool {
        self.data.read().unwrap().boot_once
    }

    /// Bytes on the filesystem associated with this boot environment.
    #[zbus(property)]
    fn space(&self) -> u64 {
        self.data.read().unwrap().space
    }

    /// Unix timestamp for when this boot environment was created.
    #[zbus(property(emits_changed_signal = "const"))]
    fn created(&self) -> i64 {
        self.data.read().unwrap().created
    }

    /// Mark this boot environment as the default root filesystem.
    async fn activate(
        &self,
        temporary: bool,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let data = self.data.read().unwrap();
        self.client
            .activate(&data.name, temporary, Some(&data.root))?;
        tracing::info!(name = data.name, temporary, "Activated boot environment");
        Ok(())
    }

    /// Destroy this boot environment.
    async fn destroy(
        &self,
        force_unmount: bool,
        snapshots: bool,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let data = self.data.read().unwrap();
        self.client.destroy(
            &Label::Name(data.name.clone()),
            force_unmount,
            snapshots,
            Some(&data.root),
        )?;
        tracing::info!(
            name = data.name,
            force_unmount,
            snapshots,
            "Destroyed boot environment"
        );
        Ok(())
    }

    /// Destroy a snapshot of this boot environment.
    async fn destroy_snapshot(
        &self,
        snapshot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let data = self.data.read().unwrap();
        let label = Label::Snapshot(data.name.clone(), snapshot.to_string());
        self.client
            .destroy(&label, false, false, Some(&data.root))?;
        tracing::info!(snapshot = label.to_string(), "Destroyed snapshot");
        Ok(())
    }

    /// Mount this boot environment.
    fn mount(&self, mountpoint: &str, read_only: bool) -> zbus::fdo::Result<()> {
        let mode = if read_only {
            MountMode::ReadOnly
        } else {
            MountMode::ReadWrite
        };
        let mountpoint = if mountpoint.is_empty() {
            None
        } else {
            Some(PathBuf::from(mountpoint))
        };
        let data = self.data.read().unwrap();
        let result = self.client.mount(
            &data.name,
            mountpoint.as_ref().map(|mp| mp.as_path()),
            mode,
            Some(&data.root),
        )?;
        tracing::info!(
            name = data.name,
            mountpoint = result.display().to_string(),
            read_only,
            "Mounted boot environment"
        );
        Ok(())
    }

    /// Unmount this boot environment.
    #[zbus(out_args("mountpoint"))]
    fn unmount(&self, force: bool) -> zbus::fdo::Result<String> {
        let data = self.data.read().unwrap();
        let mountpoint = self
            .client
            .unmount(&data.name, force, Some(&data.root))?
            .map(|p| p.display().to_string());
        tracing::info!(name = data.name, mountpoint, "Unmounted boot environment");
        Ok(mountpoint.unwrap_or_default())
    }

    /// Rename this boot environment.
    async fn rename(
        &self,
        new_name: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let data = self.data.read().unwrap();
        self.client.rename(&data.name, new_name, Some(&data.root))?;
        tracing::info!(name = data.name, new_name, "Renamed boot environment");
        Ok(())
    }

    /// Roll this boot environment back to a snapshot.
    async fn rollback(
        &self,
        snapshot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let data = self.data.read().unwrap();
        self.client
            .rollback(&data.name, snapshot, Some(&data.root))?;
        tracing::info!(
            name = data.name,
            snapshot,
            "Rolled boot environment back to snapshot"
        );
        Ok(())
    }

    /// Get snapshots for this boot environment.
    #[zbus(out_args("snapshots"))]
    fn get_snapshots(&self) -> zbus::fdo::Result<Vec<(String, Root, String, u64, i64)>> {
        let data = self.data.read().unwrap();
        let snapshots = self.client.get_snapshots(&data.name, Some(&data.root))?;
        Ok(snapshots
            .into_iter()
            .map(|snap| {
                (
                    snap.name,
                    snap.root,
                    snap.description.unwrap_or_default(),
                    snap.space,
                    snap.created,
                )
            })
            .collect())
    }

    // TODO: This is probably not useful, so hide it for now.

    // /// Get host ID for this boot environment
    // fn get_hostid(&self) -> zbus::fdo::Result<u32> {
    //     let hostid = self.client.hostid(&self.data.read().unwrap().name)?;
    //     Ok(hostid.unwrap_or(0))
    // }

    /// Create a snapshot of this boot environment.
    #[zbus(out_args("snapshot"))]
    async fn snapshot(
        &self,
        snapshot_name: &str,
        description: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<String> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let data = self.data.read().unwrap();
        let label = if snapshot_name.is_empty() {
            Label::Name(data.name.clone())
        } else {
            Label::Snapshot(data.name.clone(), snapshot_name.to_string())
        };
        let desc = if !description.is_empty() {
            Some(description)
        } else {
            None
        };
        let snapshot = self.client.snapshot(Some(&label), desc, Some(&data.root))?;
        tracing::info!(snapshot, "Created snapshot");
        Ok(snapshot)
    }

    /// Set a description for this boot environment.
    async fn describe(
        &self,
        description: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let data = self.data.read().unwrap();
        self.client.describe(
            &Label::Name(data.name.clone()),
            description,
            Some(&data.root),
        )?;
        tracing::info!(name = data.name, description, "Set description");
        Ok(())
    }
}

/// Main beadm manager implementing ObjectManager
#[derive(Clone)]
pub struct BootEnvironmentManager<T> {
    client: Arc<T>,
    guids: Arc<Mutex<HashSet<u64>>>,
}

impl<T: Client> BootEnvironmentManager<T> {
    pub fn new(client: T) -> Self {
        Self {
            client: Arc::new(client),
            guids: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

#[interface(name = "ca.kamacite.BootEnvironmentManager")]
impl<T: Client + 'static> BootEnvironmentManager<T> {
    /// Refresh managed objects.
    pub async fn refresh(
        &self,
        #[zbus(object_server)] object_server: &zbus::ObjectServer,
    ) -> zbus::fdo::Result<()> {
        let mut envs: HashMap<u64, BootEnvironment> = self
            .client
            .get_boot_environments(None)?
            .into_iter()
            .map(|env| (env.guid, env))
            .collect();
        let mut guids = self.guids.lock().unwrap().clone(); // Clone to get Send.

        // Sync current boot environments to the objects we already have.
        let mut to_remove = Vec::new();
        for guid in guids.iter() {
            let path = be_object_path(*guid);
            if let Some(current) = envs.remove(guid) {
                let iface = object_server
                    .interface::<_, BootEnvironmentObject<T>>(path)
                    .await?;
                iface
                    .get()
                    .await
                    .sync(current, iface.signal_emitter())
                    .await?;
            } else {
                to_remove.push(*guid);
            }
        }

        // Remove objects for boot environments that no longer exist.
        for guid in to_remove.into_iter() {
            let path = be_object_path(guid);
            object_server
                .remove::<BootEnvironmentObject<T>, _>(&path)
                .await?;
            guids.remove(&guid);
            tracing::debug!(path = path.to_string(), "Removed boot environment object");
        }

        // Add objects for new boot environments.
        for (guid, env) in envs.drain() {
            if guids.insert(guid) {
                let obj = BootEnvironmentObject::new(env.clone(), self.client.clone());
                let path = be_object_path(guid);
                if object_server.at(&path, obj).await? {
                    tracing::debug!(path = path.to_string(), "Added boot environment object");
                }
            }
        }

        *self.guids.lock().unwrap() = guids;
        Ok(())
    }

    /// Mark a boot environment as the default root filesystem.
    async fn activate(
        &self,
        name: &str,
        temporary: bool,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client
            .activate(name, temporary, root_from_arg(beroot)?.as_ref())?;
        tracing::info!(name, temporary, "Activated boot environment");
        self.refresh(conn.object_server()).await?;
        Ok(())
    }

    /// Clear any temporary boot environment activations.
    async fn clear_temporary_activations(
        &self,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client
            .clear_boot_once(root_from_arg(beroot)?.as_ref())?;
        tracing::info!("Removed temporary boot environment activations");
        self.refresh(conn.object_server()).await?;
        Ok(())
    }

    /// Create a boot environment from an existing boot environment or snapshot.
    #[zbus(out_args("object_path"))]
    async fn create(
        &self,
        name: &str,
        description: &str,
        source: &str,
        properties: Vec<String>,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<ObjectPath<'static>> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let desc = if description.is_empty() {
            None
        } else {
            Some(description)
        };
        let src = if source.is_empty() {
            None
        } else {
            Some(source.parse::<Label>()?)
        };

        self.client.create(
            name,
            desc,
            src.as_ref(),
            &properties,
            root_from_arg(beroot)?.as_ref(),
        )?;

        // Get the newly created BE to find its GUID
        let bes = self.client.get_boot_environments(None)?;
        let guid = bes
            .into_iter()
            .find(|be| be.name == name)
            .map(|be| be.guid)
            .ok_or_else(|| BeError::not_found(name))?;

        tracing::info!(
            name,
            source = src.as_ref().map(|s| s.to_string()),
            description = desc,
            "Created boot environment"
        );
        self.refresh(conn.object_server()).await?;
        Ok(be_object_path(guid))
    }

    /// Create a new empty boot environment.
    #[zbus(out_args("object_path"))]
    async fn create_empty(
        &self,
        name: &str,
        description: &str,
        properties: Vec<String>,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<ObjectPath<'static>> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let desc = if description.is_empty() {
            None
        } else {
            Some(description)
        };
        self.client
            .create_empty(name, desc, None, &properties, None)?;

        // Get the newly created BE to find its GUID
        let bes = self
            .client
            .get_boot_environments(root_from_arg(beroot)?.as_ref())?;
        let guid = bes
            .into_iter()
            .find(|be| be.name == name)
            .map(|be| be.guid)
            .ok_or_else(|| BeError::not_found(name))?;

        tracing::info!(name, description = desc, "Created empty boot environment");
        self.refresh(conn.object_server()).await?;
        Ok(be_object_path(guid))
    }

    /// Create a snapshot of a boot environment.
    #[zbus(out_args("snapshot"))]
    async fn snapshot(
        &self,
        target: &str,
        description: &str,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<String> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let target_opt = if target.is_empty() {
            None
        } else {
            Some(target.parse::<Label>()?)
        };
        let desc_opt = if description.is_empty() {
            None
        } else {
            Some(description)
        };
        let snapshot = self.client.snapshot(
            target_opt.as_ref(),
            desc_opt,
            root_from_arg(beroot)?.as_ref(),
        )?;
        tracing::info!(snapshot, "Created snapshot");
        self.refresh(conn.object_server()).await?;
        Ok(snapshot)
    }

    /// Destroy an existing boot environment or snapshot.
    async fn destroy(
        &self,
        name: &str,
        force_unmount: bool,
        snapshots: bool,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let label = Label::Name(name.to_string());
        self.client.destroy(
            &label,
            force_unmount,
            snapshots,
            root_from_arg(beroot)?.as_ref(),
        )?;
        tracing::info!(name, force_unmount, snapshots, "Destroyed boot environment");
        self.refresh(conn.object_server()).await?;
        Ok(())
    }

    /// Destroy an existing boot environment snapshot.
    async fn destroy_snapshot(
        &self,
        name: &str,
        snapshot: &str,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let label = Label::Snapshot(name.to_string(), snapshot.to_string());
        self.client
            .destroy(&label, false, false, root_from_arg(beroot)?.as_ref())?;
        tracing::info!(snapshot = label.to_string(), "Destroyed snapshot");
        self.refresh(conn.object_server()).await?;
        Ok(())
    }

    /// Mount a boot environment.
    #[zbus(out_args("mountpoint"))]
    async fn mount(
        &self,
        name: &str,
        mountpoint: &str,
        read_only: bool,
        beroot: &str,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<PathBuf> {
        // Note: this is not a privileged operation (yet), because mounting a
        // boot environment doesn't give you any more permissions to modify it
        // than you already have.
        let mode = if read_only {
            MountMode::ReadOnly
        } else {
            MountMode::ReadWrite
        };
        let mountpoint = if mountpoint.is_empty() {
            None
        } else {
            Some(PathBuf::from(mountpoint))
        };
        let result = self.client.mount(
            name,
            mountpoint.as_ref().map(|mp| mp.as_path()),
            mode,
            root_from_arg(beroot)?.as_ref(),
        )?;
        tracing::info!(
            name,
            mountpoint = result.display().to_string(),
            read_only,
            "Mounted boot environment"
        );
        self.refresh(conn.object_server()).await?;
        Ok(result)
    }

    /// Unmount an inactive boot environment.
    #[zbus(out_args("mountpoint"))]
    async fn unmount(
        &self,
        name: &str,
        force: bool,
        beroot: &str,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<String> {
        let mountpoint = self
            .client
            .unmount(name, force, root_from_arg(beroot)?.as_ref())?
            .map(|p| p.display().to_string());
        tracing::info!(name, mountpoint, "Unmounted boot environment");
        self.refresh(conn.object_server()).await?;
        Ok(mountpoint.unwrap_or_default())
    }

    /// Rename a boot environment.
    async fn rename(
        &self,
        name: &str,
        new_name: &str,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client
            .rename(name, new_name, root_from_arg(beroot)?.as_ref())?;
        tracing::info!(name, new_name, "Renamed boot environment");
        self.refresh(conn.object_server()).await?;
        Ok(())
    }

    /// Set a description for an existing boot environment or snapshot.
    async fn describe(
        &self,
        target: &str,
        description: &str,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let label = target.parse::<Label>()?;
        self.client
            .describe(&label, description, root_from_arg(beroot)?.as_ref())?;
        tracing::info!(target, description, "Set description");
        self.refresh(conn.object_server()).await?;
        Ok(())
    }

    /// Roll back a boot environment to an earlier snapshot.
    async fn rollback(
        &self,
        name: &str,
        snapshot: &str,
        beroot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client
            .rollback(name, snapshot, root_from_arg(beroot)?.as_ref())?;
        tracing::info!(name, snapshot, "Rolled boot environment back to snapshot");
        self.refresh(conn.object_server()).await?;
        Ok(())
    }

    /// Get snapshots for a boot environment.
    #[zbus(out_args("snapshots"))]
    fn get_snapshots(
        &self,
        be_name: &str,
        beroot: &str,
    ) -> zbus::fdo::Result<Vec<(String, Root, String, u64, i64)>> {
        let snapshots = self
            .client
            .get_snapshots(be_name, root_from_arg(beroot)?.as_ref())?;
        Ok(snapshots
            .into_iter()
            .map(|snap| {
                (
                    snap.name,
                    snap.root,
                    snap.description.unwrap_or_default(),
                    snap.space,
                    snap.created,
                )
            })
            .collect())
    }

    /// Create the ZFS dataset layout for boot environments.
    async fn init(
        &self,
        pool: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client.init(pool)?;
        tracing::info!(pool, "Initialized boot environment dataset layout");
        Ok(())
    }
}

async fn check_authorization(
    conn: &zbus::Connection,
    header: &zbus::message::Header<'_>,
    action_id: &str,
) -> Result<(), zbus::Error> {
    // Check if the sender is privileged (i.e. root, currently).
    let sender_name = match header.sender() {
        Some(name) => zbus::names::BusName::Unique(name.clone()),
        None => {
            tracing::error!(action_id, "Denying authorization due to missing sender");
            return Err(zbus::fdo::Error::AccessDenied("Access denied".to_string()).into());
        }
    };
    let dbus_proxy = zbus::fdo::DBusProxy::new(conn).await?;
    let uid = dbus_proxy.get_connection_unix_user(sender_name).await?;
    if uid == 0 {
        tracing::debug!(action_id, uid, "Authorization granted for privileged user");
        return Ok(());
    }

    // Otherwise check authorization via polkit.
    //
    // Note: This won't work if beadm is running on the user bus, because Polkit
    // isn't available. You'll get an org.freedesktop.DBus.Error.ServiceUnknown.
    let proxy = zbus_polkit::policykit1::AuthorityProxy::new(conn).await?;
    tracing::debug!(action_id, uid, "Checking authorization via polkit");
    let subject = match zbus_polkit::policykit1::Subject::new_for_message_header(header) {
        Ok(subject) => subject,
        Err(e) => {
            tracing::error!(
                action_id,
                error = e.to_string(),
                "Denying authorization due to invalid subject"
            );
            return Err(zbus::fdo::Error::AccessDenied("Access denied".to_string()).into());
        }
    };
    let result = proxy
        .check_authorization(
            &subject,
            action_id,
            &std::collections::HashMap::new(),
            zbus_polkit::policykit1::CheckAuthorizationFlags::AllowUserInteraction.into(),
            "", // No cancellation support.
        )
        .await
        .map_err(|e| {
            tracing::error!(
                action_id,
                error = e.to_string(),
                "Polkit authorization check failed"
            );
            zbus::fdo::Error::AccessDenied("Access denied".to_string())
        })?;
    if result.is_authorized {
        tracing::debug!(action_id, "Authorization granted");
        Ok(())
    } else if result.is_challenge {
        tracing::debug!(action_id, "Authorization requires user interaction");
        Err(zbus::fdo::Error::InteractiveAuthorizationRequired(
            "Interactive authorization required".to_string(),
        )
        .into())
    } else {
        tracing::debug!(action_id, "Authorization failed");
        Err(zbus::fdo::Error::AccessDenied("Access denied".to_string()).into())
    }
}

fn root_from_arg(root: &str) -> Result<Option<Root>, zbus::fdo::Error> {
    if root.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Root::from_str(root)?))
    }
}

/// Start a D-Bus service for boot environment administration.
pub async fn serve<T: Client + 'static>(client: T, use_session_bus: bool) -> zbus::Result<()> {
    // Logs in journald don't need colours.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .event_format(tracing_subscriber::fmt::format().with_ansi(false).compact())
        .init();

    let builder = if use_session_bus {
        zbus::connection::Builder::session()?
    } else {
        zbus::connection::Builder::system()?
    };

    // We don't use the usual zbus builder pattern for all our interfaces here,
    // because we want `beadm list` to see all the boot environments when it
    // triggers activation, and that means that we have to populate all of the
    // objects *before* we request ownership of the well-known name.
    //
    // Instead, start by registering the manager.
    let connection = builder
        .serve_at(BOOT_ENV_PATH, BootEnvironmentManager::new(client))?
        .build()
        .await?;

    // Populate the tree of boot environment objects.
    let iface_ref = connection
        .object_server()
        .interface::<_, BootEnvironmentManager<T>>(BOOT_ENV_PATH)
        .await?;
    let manager = iface_ref.get().await;
    manager.refresh(&connection.object_server()).await?;

    // Add the ObjectManager interface *after* the initial population of boot
    // environment objects to avoid emitting signals before anyone is listening
    // to them.
    connection
        .object_server()
        .at(BOOT_ENV_PATH, zbus::fdo::ObjectManager)
        .await?;

    // Finally, request ownership of the well-known name.
    connection.request_name(SERVICE_NAME).await?;

    let bus = if use_session_bus { "session" } else { "system" };
    tracing::info!(service_name = SERVICE_NAME, bus, "D-Bus service started");

    // Wait up to five minutes of inactivity before shutting down again.
    let mut idle_count = 0;
    while idle_count < 5 {
        if connection
            .monitor_activity()
            .wait_timeout(std::time::Duration::from_secs(60))
            .is_none()
        {
            idle_count += 1;
        } else {
            tracing::trace!("Activity detected, reseting idle timeout");
            idle_count = 0;
        }
        // Periodically refresh objects in case they are changed out from under
        // us (by e.g. a direct zfs CLI call).
        if let Err(e) = manager.refresh(&connection.object_server()).await {
            tracing::error!("Error refreshing objects: {}", e);
        }
    }
    tracing::info!(
        service_name = SERVICE_NAME,
        "D-Bus service stopping due to inactivity"
    );
    // Emulate systemd's bus_event_loop_with_idle() and notify that we're
    // stopping before releasing the name. Apparently this results in better
    // queuing behaviour.
    let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Stopping]);
    connection.release_name(SERVICE_NAME).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_be_object_path() {
        assert_eq!(
            be_object_path(0x1234567890abcdef).as_str(),
            format!("{}/1234567890abcdef", BOOT_ENV_PATH)
        );
        assert_eq!(
            be_object_path(0x0).as_str(),
            format!("{}/0000000000000000", BOOT_ENV_PATH)
        );
    }
}
