use crate::be::Error as BeError;
use crate::be::{BootEnvironment, Client, Label, MountMode, Snapshot};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use tracing;
use tracing_subscriber;
use zbus::object_server::SignalEmitter;
use zbus::{Connection, blocking, interface};
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
    pub fn new(connection: Connection) -> Result<Self, BeError> {
        Ok(Self {
            connection: connection.into(),
        })
    }
}

impl Client for ClientProxy {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&Label>,
        properties: &[String],
    ) -> Result<(), BeError> {
        let desc = description.unwrap_or("");
        let src = source.map(|label| label.to_string()).unwrap_or_default();
        let props: Vec<String> = properties.to_vec();

        let _result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Create",
                &(be_name, desc, src, props),
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
    ) -> Result<(), BeError> {
        let desc = description.unwrap_or("");
        let hid = host_id.unwrap_or("");
        let props: Vec<String> = properties.to_vec();

        let _result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "CreateEmpty",
                &(be_name, desc, hid, props),
            )?
            .body()
            .deserialize()?;

        Ok(())
    }

    fn destroy(&self, target: &Label, force_unmount: bool, snapshots: bool) -> Result<(), BeError> {
        match target {
            Label::Name(name) => self.connection.call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Destroy",
                &(name, force_unmount, snapshots),
            ),
            Label::Snapshot(name, snapshot) => self.connection.call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "DestroySnapshot",
                &(name, snapshot),
            ),
        }?;
        Ok(())
    }

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), BeError> {
        let read_only = match mode {
            MountMode::ReadOnly => true,
            MountMode::ReadWrite => false,
        };
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Mount",
            &(be_name, mountpoint, read_only),
        )?;
        Ok(())
    }

    fn unmount(&self, be_name: &str, force: bool) -> Result<Option<PathBuf>, BeError> {
        let result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Unmount",
                &(be_name, force),
            )?
            .body()
            .deserialize()?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PathBuf::from(result)))
        }
    }

    fn hostid(&self, _be_name: &str) -> Result<Option<u32>, BeError> {
        // TODO: Decide whether to implement this.
        Ok(None)
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), BeError> {
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Rename",
            &(be_name, new_name),
        )?;
        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), BeError> {
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Activate",
            &(be_name, temporary),
        )?;
        Ok(())
    }

    fn clear_boot_once(&self) -> Result<(), BeError> {
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "ClearBootOnce",
            &(),
        )?;
        Ok(())
    }

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), BeError> {
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Rollback",
            &(be_name, snapshot),
        )?;
        Ok(())
    }

    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, BeError> {
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

        let mut boot_environments = Vec::new();
        for (_path, interfaces) in managed_objects {
            if let Some(be) = interfaces.get(BOOT_ENV_INTERFACE) {
                boot_environments.push(be.clone());
            }
        }
        Ok(boot_environments)
    }

    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, BeError> {
        let snapshots_data: Vec<(String, String, String, u64, i64)> = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "GetSnapshots",
                &(be_name,),
            )?
            .body()
            .deserialize()?;

        let snapshots = snapshots_data
            .into_iter()
            .map(|(name, path, description, space, created)| Snapshot {
                name,
                path,
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
    ) -> Result<String, BeError> {
        let src = source.map(|label| label.to_string()).unwrap_or_default();
        let desc = description.unwrap_or("");
        let result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                BOOT_ENV_PATH,
                Some(MANAGER_INTERFACE),
                "Snapshot",
                &(src, desc),
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

    fn describe(&self, target: &Label, description: &str) -> Result<(), BeError> {
        let target_str = target.to_string();
        self.connection.call_method(
            Some(SERVICE_NAME),
            BOOT_ENV_PATH,
            Some(MANAGER_INTERFACE),
            "Describe",
            &(target_str, description),
        )?;
        Ok(())
    }
}

// ============================================================================
// D-Bus Server (BeadmServer and related components)
// ============================================================================

/// Individual boot environment D-Bus object
#[derive(Clone)]
pub struct BootEnvironmentObject {
    data: Arc<RwLock<BootEnvironment>>,
    client: Arc<dyn Client>,
}

impl BootEnvironmentObject {
    pub fn new(data: BootEnvironment, client: Arc<dyn Client>) -> Self {
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
            path: bool,
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
                path: stored.path != current.path,
                description: stored.description != current.description,
                mountpoint: stored.mountpoint != current.mountpoint,
                next_boot: stored.next_boot != current.next_boot,
                boot_once: stored.boot_once != current.boot_once,
                space: stored.space != current.space,
            })
            .expect("Failed to acquire read lock");

        if !(changed.name
            || changed.path
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
        if changed.path {
            self.path_changed(signal_emitter).await?;
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
impl BootEnvironmentObject {
    /// The name of this boot environment.
    #[zbus(property)]
    fn name(&self) -> String {
        self.data.read().unwrap().name.clone()
    }

    /// The ZFS dataset path (e.g., `zroot/ROOT/default`).
    #[zbus(property)]
    fn path(&self) -> String {
        self.data.read().unwrap().path.clone()
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
        let name = &self.data.read().unwrap().name;
        self.client.activate(name, temporary)?;
        tracing::info!(name, temporary, "Activated boot environment");
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
        let name = &self.data.read().unwrap().name;
        self.client
            .destroy(&Label::Name(name.clone()), force_unmount, snapshots)?;
        tracing::info!(name, force_unmount, snapshots, "Destroyed boot environment");
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
        let name = &self.data.read().unwrap().name;
        let label = Label::Snapshot(name.clone(), snapshot.to_string());
        self.client.destroy(&label, false, false)?;
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
        let name = &self.data.read().unwrap().name;
        self.client.mount(name, mountpoint, mode)?;
        tracing::info!(name, mountpoint, read_only, "Mounted boot environment");
        Ok(())
    }

    /// Unmount this boot environment.
    #[zbus(out_args("mountpoint"))]
    fn unmount(&self, force: bool) -> zbus::fdo::Result<String> {
        let name = &self.data.read().unwrap().name;
        let mountpoint = self
            .client
            .unmount(name, force)?
            .map(|p| p.display().to_string());
        tracing::info!(name, mountpoint, "Unmounted boot environment");
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
        let name = &self.data.read().unwrap().name;
        self.client.rename(name, new_name)?;
        tracing::info!(name, new_name, "Renamed boot environment");
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
        let name = &self.data.read().unwrap().name;
        self.client.rollback(name, snapshot)?;
        tracing::info!(name, snapshot, "Rolled boot environment back to snapshot");
        Ok(())
    }

    /// Get snapshots for this boot environment.
    #[zbus(out_args("snapshots"))]
    fn get_snapshots(&self) -> zbus::fdo::Result<Vec<(String, String, String, u64, i64)>> {
        let snapshots = self.client.get_snapshots(&self.data.read().unwrap().name)?;
        Ok(snapshots
            .into_iter()
            .map(|snap| {
                (
                    snap.name,
                    snap.path,
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
        let name = &self.data.read().unwrap().name;
        let label = if snapshot_name.is_empty() {
            Label::Name(name.clone())
        } else {
            Label::Snapshot(name.clone(), snapshot_name.to_string())
        };
        let desc = if !description.is_empty() {
            Some(description)
        } else {
            None
        };
        let snapshot = self.client.snapshot(Some(&label), desc)?;
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
        let name = &self.data.read().unwrap().name;
        self.client
            .describe(&Label::Name(name.clone()), description)?;
        tracing::info!(name, description, "Set description");
        Ok(())
    }
}

/// Main beadm manager implementing ObjectManager
#[derive(Clone)]
pub struct BootEnvironmentManager {
    client: Arc<dyn Client>,
    guids: Arc<Mutex<HashSet<u64>>>,
}

impl BootEnvironmentManager {
    pub fn new(client: Arc<dyn Client>) -> Self {
        Self {
            client,
            guids: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

#[interface(name = "ca.kamacite.BootEnvironmentManager")]
impl BootEnvironmentManager {
    /// Refresh managed objects.
    pub async fn refresh(
        &self,
        #[zbus(object_server)] object_server: &zbus::ObjectServer,
    ) -> zbus::fdo::Result<()> {
        let mut envs: HashMap<u64, BootEnvironment> = self
            .client
            .get_boot_environments()?
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
                    .interface::<_, BootEnvironmentObject>(path)
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
                .remove::<BootEnvironmentObject, _>(&path)
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
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client.activate(name, temporary)?;
        tracing::info!(name, temporary, "Activated boot environment");
        Ok(())
    }

    /// Clear any temporary boot environment activations.
    async fn clear_temporary_activations(
        &self,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client.clear_boot_once()?;
        tracing::info!("Removed temporary boot environment activations");
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

        self.client.create(name, desc, src.as_ref(), &properties)?;

        // Get the newly created BE to find its GUID
        let bes = self.client.get_boot_environments()?;
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
        Ok(be_object_path(guid))
    }

    /// Create a new empty boot environment.
    #[zbus(out_args("object_path"))]
    async fn create_empty(
        &self,
        name: &str,
        description: &str,
        properties: Vec<String>,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<ObjectPath<'static>> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let desc = if description.is_empty() {
            None
        } else {
            Some(description)
        };
        self.client.create_empty(name, desc, None, &properties)?;

        // Get the newly created BE to find its GUID
        let bes = self.client.get_boot_environments()?;
        let guid = bes
            .into_iter()
            .find(|be| be.name == name)
            .map(|be| be.guid)
            .ok_or_else(|| BeError::not_found(name))?;

        tracing::info!(name, description = desc, "Created empty boot environment");
        Ok(be_object_path(guid))
    }

    /// Create a snapshot of a boot environment.
    #[zbus(out_args("snapshot"))]
    async fn snapshot(
        &self,
        target: &str,
        description: &str,
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
        let snapshot = self.client.snapshot(target_opt.as_ref(), desc_opt)?;
        tracing::info!(snapshot, "Created snapshot");
        Ok(snapshot)
    }

    /// Destroy an existing boot environment or snapshot.
    async fn destroy(
        &self,
        name: &str,
        force_unmount: bool,
        snapshots: bool,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let label = Label::Name(name.to_string());
        self.client.destroy(&label, force_unmount, snapshots)?;
        tracing::info!(name, force_unmount, snapshots, "Destroyed boot environment");
        Ok(())
    }

    /// Destroy an existing boot environment snapshot.
    async fn destroy_snapshot(
        &self,
        name: &str,
        snapshot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let label = Label::Snapshot(name.to_string(), snapshot.to_string());
        self.client.destroy(&label, false, false)?;
        tracing::info!(snapshot = label.to_string(), "Destroyed snapshot");
        Ok(())
    }

    /// Mount a boot environment.
    fn mount(&self, name: &str, mountpoint: &str, read_only: bool) -> zbus::fdo::Result<()> {
        // Note: this is not a privileged operation (yet), because mounting a
        // boot environment doesn't give you any more permissions to modify it
        // than you already have.
        let mode = if read_only {
            MountMode::ReadOnly
        } else {
            MountMode::ReadWrite
        };
        self.client.mount(name, mountpoint, mode)?;
        tracing::info!(name, mountpoint, read_only, "Mounted boot environment");
        Ok(())
    }

    /// Unmount an inactive boot environment.
    #[zbus(out_args("mountpoint"))]
    fn unmount(&self, name: &str, force: bool) -> zbus::fdo::Result<String> {
        let mountpoint = self
            .client
            .unmount(name, force)?
            .map(|p| p.display().to_string());
        tracing::info!(name, mountpoint, "Unmounted boot environment");
        Ok(mountpoint.unwrap_or_default())
    }

    /// Rename a boot environment.
    async fn rename(
        &self,
        name: &str,
        new_name: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client.rename(name, new_name)?;
        tracing::info!(name, new_name, "Renamed boot environment");
        Ok(())
    }

    /// Set a description for an existing boot environment or snapshot.
    async fn describe(
        &self,
        target: &str,
        description: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        let label = target.parse::<Label>()?;
        self.client.describe(&label, description)?;
        tracing::info!(target, description, "Set description");
        Ok(())
    }

    /// Roll back a boot environment to an earlier snapshot.
    async fn rollback(
        &self,
        name: &str,
        snapshot: &str,
        #[zbus(header)] header: zbus::message::Header<'_>,
        #[zbus(connection)] conn: &zbus::Connection,
    ) -> zbus::fdo::Result<()> {
        check_authorization(conn, &header, "ca.kamacite.BootEnvironments1.manage").await?;
        self.client.rollback(name, snapshot)?;
        tracing::info!(name, snapshot, "Rolled boot environment back to snapshot");
        Ok(())
    }

    /// Get snapshots for a boot environment.
    #[zbus(out_args("snapshots"))]
    fn get_snapshots(
        &self,
        be_name: &str,
    ) -> zbus::fdo::Result<Vec<(String, String, String, u64, i64)>> {
        let snapshots = self.client.get_snapshots(be_name)?;
        Ok(snapshots
            .into_iter()
            .map(|snap| {
                (
                    snap.name,
                    snap.path,
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

/// Start a D-Bus service for boot environment administration.
pub async fn serve<T: Client + 'static>(client: T, use_session_bus: bool) -> zbus::Result<()> {
    // Logs in journald don't need colours.
    tracing_subscriber::fmt()
        .event_format(tracing_subscriber::fmt::format().with_ansi(false).compact())
        .init();

    let client: Arc<dyn Client> = Arc::new(client);

    let builder = if use_session_bus {
        zbus::connection::Builder::session()?
    } else {
        zbus::connection::Builder::system()?
    };

    let connection = builder
        .name(SERVICE_NAME)?
        .serve_at(BOOT_ENV_PATH, BootEnvironmentManager::new(client.clone()))?
        .serve_at(BOOT_ENV_PATH, zbus::fdo::ObjectManager)?
        .build()
        .await?;

    let bus = if use_session_bus { "session" } else { "system" };
    tracing::info!(service_name = SERVICE_NAME, bus, "D-Bus service started");

    // Initial population of objects
    let iface_ref = connection
        .object_server()
        .interface::<_, BootEnvironmentManager>(BOOT_ENV_PATH)
        .await?;
    let manager = iface_ref.get().await;
    manager.refresh(&connection.object_server()).await?;

    // Keep the connection alive and periodically refresh objects
    loop {
        async_io::Timer::after(std::time::Duration::from_secs(5)).await;
        if let Err(e) = manager.refresh(&connection.object_server()).await {
            tracing::error!("Error refreshing objects: {}", e);
        }
    }
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
