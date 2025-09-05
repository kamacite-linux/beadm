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

    /// Get the GUID for a boot environment by name
    fn get_be_guid(&self, be_name: &str) -> Result<u64, BeError> {
        let bes = self.get_boot_environments()?;
        bes.into_iter()
            .find(|be| be.name == be_name)
            .map(|be| be.guid)
            .ok_or_else(|| BeError::not_found(be_name))
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

    fn destroy(&self, target: &str, force_unmount: bool, snapshots: bool) -> Result<(), BeError> {
        let guid = self.get_be_guid(target)?;
        self.connection.call_method(
            Some(SERVICE_NAME),
            &be_object_path(guid),
            Some(BOOT_ENV_INTERFACE),
            "Destroy",
            &(force_unmount, snapshots),
        )?;
        Ok(())
    }

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), BeError> {
        let read_only = match mode {
            MountMode::ReadOnly => true,
            MountMode::ReadWrite => false,
        };
        let guid = self.get_be_guid(be_name)?;
        self.connection.call_method(
            Some(SERVICE_NAME),
            &be_object_path(guid),
            Some(BOOT_ENV_INTERFACE),
            "Mount",
            &(mountpoint, read_only),
        )?;
        Ok(())
    }

    fn unmount(&self, be_name: &str, force: bool) -> Result<Option<PathBuf>, BeError> {
        let guid = self.get_be_guid(be_name)?;
        let result: String = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                &be_object_path(guid),
                Some(BOOT_ENV_INTERFACE),
                "Unmount",
                &(force,),
            )?
            .body()
            .deserialize()?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PathBuf::from(result)))
        }
    }

    fn hostid(&self, be_name: &str) -> Result<Option<u32>, BeError> {
        let guid = self.get_be_guid(be_name)?;
        let hostid: u32 = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                &be_object_path(guid),
                Some(BOOT_ENV_INTERFACE),
                "GetHostid",
                &(),
            )?
            .body()
            .deserialize()?;

        if hostid == 0 {
            Ok(None)
        } else {
            Ok(Some(hostid))
        }
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), BeError> {
        let guid = self.get_be_guid(be_name)?;
        self.connection.call_method(
            Some(SERVICE_NAME),
            &be_object_path(guid),
            Some(BOOT_ENV_INTERFACE),
            "Rename",
            &(new_name,),
        )?;
        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), BeError> {
        let guid = self.get_be_guid(be_name)?;
        self.connection.call_method(
            Some(SERVICE_NAME),
            &be_object_path(guid),
            Some(BOOT_ENV_INTERFACE),
            "Activate",
            &(temporary,),
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
        let guid = self.get_be_guid(be_name)?;
        self.connection.call_method(
            Some(SERVICE_NAME),
            &be_object_path(guid),
            Some(BOOT_ENV_INTERFACE),
            "Rollback",
            &(snapshot,),
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
        let guid = self.get_be_guid(be_name)?;
        let snapshots_data: Vec<(String, String, String, u64, i64)> = self
            .connection
            .call_method(
                Some(SERVICE_NAME),
                &be_object_path(guid),
                Some(BOOT_ENV_INTERFACE),
                "GetSnapshots",
                &(),
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

    fn snapshot(&self, source: Option<&Label>, description: Option<&str>) -> Result<String, BeError> {
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
    /// Boot environment name
    #[zbus(property)]
    fn name(&self) -> String {
        self.data.read().unwrap().name.clone()
    }

    /// Boot environment dataset path
    #[zbus(property)]
    fn path(&self) -> String {
        self.data.read().unwrap().path.clone()
    }

    /// Boot environment description
    #[zbus(property)]
    fn description(&self) -> String {
        self.data
            .read()
            .unwrap()
            .description
            .clone()
            .unwrap_or_default()
    }

    /// Current mountpoint (empty if not mounted)
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

    /// Whether this is the currently active boot environment
    #[zbus(property(emits_changed_signal = "const"))]
    fn active(&self) -> bool {
        self.data.read().unwrap().active
    }

    /// Whether this BE will be used for next boot
    #[zbus(property)]
    fn next_boot(&self) -> bool {
        self.data.read().unwrap().next_boot
    }

    /// Whether this BE is set for one-time boot
    #[zbus(property)]
    fn boot_once(&self) -> bool {
        self.data.read().unwrap().boot_once
    }

    /// Space used by this boot environment in bytes
    #[zbus(property)]
    fn space(&self) -> u64 {
        self.data.read().unwrap().space
    }

    /// Creation timestamp (Unix time)
    #[zbus(property(emits_changed_signal = "const"))]
    fn created(&self) -> i64 {
        self.data.read().unwrap().created
    }

    /// Destroy this boot environment
    fn destroy(&self, force_unmount: bool, snapshots: bool) -> zbus::fdo::Result<()> {
        self.client
            .destroy(&self.data.read().unwrap().name, force_unmount, snapshots)?;
        Ok(())
    }

    /// Mount this boot environment
    fn mount(&self, mountpoint: &str, read_only: bool) -> zbus::fdo::Result<()> {
        let mode = if read_only {
            MountMode::ReadOnly
        } else {
            MountMode::ReadWrite
        };

        self.client
            .mount(&self.data.read().unwrap().name, mountpoint, mode)?;
        Ok(())
    }

    /// Unmount this boot environment
    fn unmount(&self, force: bool) -> zbus::fdo::Result<String> {
        let result = self
            .client
            .unmount(&self.data.read().unwrap().name, force)?;
        Ok(result.map(|p| p.display().to_string()).unwrap_or_default())
    }

    /// Rename this boot environment
    fn rename(&self, new_name: &str) -> zbus::fdo::Result<()> {
        self.client
            .rename(&self.data.read().unwrap().name, new_name)?;
        Ok(())
    }

    /// Activate this boot environment
    fn activate(&self, temporary: bool) -> zbus::fdo::Result<()> {
        self.client
            .activate(&self.data.read().unwrap().name, temporary)?;
        Ok(())
    }

    /// Rollback to a snapshot
    fn rollback(&self, snapshot: &str) -> zbus::fdo::Result<()> {
        self.client
            .rollback(&self.data.read().unwrap().name, snapshot)?;
        Ok(())
    }

    /// Get snapshots for this boot environment
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

    /// Create a snapshot of this boot environment
    #[zbus(out_args("snapshot"))]
    fn snapshot(&self, snapshot_name: &str, description: &str) -> zbus::fdo::Result<String> {
        let be_name = &self.data.read().unwrap().name;
        let label = if snapshot_name.is_empty() {
            Label::Name(be_name.clone())
        } else {
            Label::Snapshot(be_name.clone(), snapshot_name.to_string())
        };
        let desc = if !description.is_empty() {
            Some(description)
        } else {
            None
        };
        let result = self.client.snapshot(Some(&label), desc)?;
        Ok(result)
    }

    /// Set a description for this boot environment.
    fn describe(&self, description: &str) -> zbus::fdo::Result<()> {
        let be_name = &self.data.read().unwrap().name;
        self.client
            .describe(&Label::Name(be_name.clone()), description)?;
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
        let object_manager = object_server
            .interface::<_, ObjectManager>(BOOT_ENV_PATH)
            .await?;
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

            // Emit an InterfacesRemoved signal, even if the object was not
            // destroyed by remove().
            ObjectManager::interfaces_removed(
                object_manager.signal_emitter(),
                &path,
                vec![BOOT_ENV_INTERFACE.to_string()],
            )
            .await?;

            guids.remove(&guid);
        }

        // Add objects for new boot environments.
        for (guid, env) in envs.drain() {
            if guids.insert(guid) {
                let obj = BootEnvironmentObject::new(env.clone(), self.client.clone());
                let path = be_object_path(guid);
                if object_server.at(&path, obj).await? {
                    // Emit an InterfacesAdded signal after successful at().
                    let mut interfaces = BTreeMap::new();
                    interfaces.insert(BOOT_ENV_INTERFACE.to_string(), &env);
                    ObjectManager::interfaces_added(
                        object_manager.signal_emitter(),
                        &path,
                        interfaces,
                    )
                    .await?;
                }
            }
        }

        *self.guids.lock().unwrap() = guids;
        Ok(())
    }

    /// Create a new boot environment by cloning an existing one
    fn create(
        &self,
        name: &str,
        description: &str,
        source: &str,
        properties: Vec<String>,
    ) -> zbus::fdo::Result<ObjectPath<'static>> {
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

        Ok(be_object_path(guid))
    }

    /// Create a new empty boot environment
    fn create_empty(
        &self,
        name: &str,
        description: &str,
        host_id: &str,
        properties: Vec<String>,
    ) -> zbus::fdo::Result<ObjectPath<'static>> {
        let desc = if description.is_empty() {
            None
        } else {
            Some(description)
        };
        let hid = if host_id.is_empty() {
            None
        } else {
            Some(host_id)
        };

        self.client.create_empty(name, desc, hid, &properties)?;

        // Get the newly created BE to find its GUID
        let bes = self.client.get_boot_environments()?;
        let guid = bes
            .into_iter()
            .find(|be| be.name == name)
            .map(|be| be.guid)
            .ok_or_else(|| BeError::not_found(name))?;

        Ok(be_object_path(guid))
    }

    /// Clear temporary boot environment activation.
    fn clear_boot_once(&self) -> zbus::fdo::Result<()> {
        self.client.clear_boot_once()?;
        Ok(())
    }

    /// Create a snapshot of a boot environment
    fn snapshot(&self, target: &str, description: &str) -> zbus::fdo::Result<String> {
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
        let result = self.client.snapshot(target_opt.as_ref(), desc_opt)?;
        Ok(result)
    }

    /// Create the ZFS dataset layout for boot environments.
    fn init(&self, pool: &str) -> zbus::fdo::Result<()> {
        self.client.init(pool)?;
        Ok(())
    }

    /// Set a description for an existing boot environment or snapshot.
    fn describe(&self, target: &str, description: &str) -> zbus::fdo::Result<()> {
        let label = target.parse::<Label>()?;
        self.client.describe(&label, description)?;
        Ok(())
    }
}

/// ObjectManager interface implementation
#[derive(Clone)]
pub struct ObjectManager {
    client: Arc<dyn Client>,
}

impl ObjectManager {
    pub fn new(client: Arc<dyn Client>) -> Self {
        Self { client }
    }
}

#[interface(name = "org.freedesktop.DBus.ObjectManager")]
impl ObjectManager {
    /// Get all managed objects and their interfaces
    fn get_managed_objects(
        &self,
    ) -> zbus::fdo::Result<BTreeMap<ObjectPath<'static>, BTreeMap<String, BootEnvironment>>> {
        let mut objects = BTreeMap::new();
        for env in self.client.get_boot_environments()? {
            // We only manage objects with one interface.
            let path = be_object_path(env.guid);
            let mut interfaces = BTreeMap::new();
            interfaces.insert(BOOT_ENV_INTERFACE.to_string(), env);
            objects.insert(path, interfaces);
        }
        Ok(objects)
    }

    /// Signal emitted when new interfaces are added to an object
    #[zbus(signal)]
    async fn interfaces_added(
        emitter: &SignalEmitter<'_>,
        object_path: &ObjectPath<'_>,
        interfaces_and_properties: BTreeMap<String, &BootEnvironment>,
    ) -> zbus::Result<()>;

    /// Signal emitted when interfaces are removed from an object
    #[zbus(signal)]
    async fn interfaces_removed(
        emitter: &SignalEmitter<'_>,
        object_path: &ObjectPath<'_>,
        interfaces: Vec<String>,
    ) -> zbus::Result<()>;
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
        .serve_at(BOOT_ENV_PATH, ObjectManager::new(client))?
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
        std::thread::sleep(std::time::Duration::from_secs(5));
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
