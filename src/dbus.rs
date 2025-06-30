use crate::be::Error as BeError;
use crate::be::{BootEnvironment, Client, MountMode, Snapshot};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use zbus::blocking::ObjectServer;
use zbus::blocking::{Connection, connection};
use zbus::object_server::SignalEmitter;
use zbus::{Result as ZbusResult, interface};
use zvariant::ObjectPath;

/// Translate a boot environment GUID to a D-Bus object path.
fn be_object_path(guid: u64) -> ObjectPath<'static> {
    // This is safe to unwrap because hex strings are always valid object path components.
    ObjectPath::try_from(format!("/org/beadm/BootEnvironments/{:016x}", guid)).unwrap()
}

// ============================================================================
// D-Bus Client (RemoteClient)
// ============================================================================

pub struct RemoteClient {
    connection: Connection,
}

impl RemoteClient {
    pub fn new(use_session_bus: bool) -> Result<Self, BeError> {
        let connection = if use_session_bus {
            Connection::session()
        } else {
            Connection::system()
        }?;

        Ok(Self { connection })
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

impl Client for RemoteClient {
    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&str>,
        properties: &[String],
    ) -> Result<(), BeError> {
        let desc = description.unwrap_or("");
        let src = source.unwrap_or("");
        let props: Vec<String> = properties.to_vec();

        let _result: String = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                "/org/beadm/Manager",
                Some("org.beadm.Manager"),
                "create",
                &(be_name, desc, src, props),
            )?
            .body()
            .deserialize()?;

        Ok(())
    }

    fn new(
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
                Some("org.beadm.Manager"),
                "/org/beadm/Manager",
                Some("org.beadm.Manager"),
                "create_new",
                &(be_name, desc, hid, props),
            )?
            .body()
            .deserialize()?;

        Ok(())
    }

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        force_no_verify: bool,
        snapshots: bool,
    ) -> Result<(), BeError> {
        let guid = self.get_be_guid(target)?;
        self.connection.call_method(
            Some("org.beadm.Manager"),
            &be_object_path(guid),
            Some("org.beadm.BootEnvironment"),
            "destroy",
            &(force_unmount, force_no_verify, snapshots),
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
            Some("org.beadm.Manager"),
            &be_object_path(guid),
            Some("org.beadm.BootEnvironment"),
            "mount",
            &(mountpoint, read_only),
        )?;
        Ok(())
    }

    fn unmount(&self, target: &str, force: bool) -> Result<Option<PathBuf>, BeError> {
        let guid = self.get_be_guid(target)?;
        let result: String = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                &be_object_path(guid),
                Some("org.beadm.BootEnvironment"),
                "unmount",
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
                Some("org.beadm.Manager"),
                &be_object_path(guid),
                Some("org.beadm.BootEnvironment"),
                "get_hostid",
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
            Some("org.beadm.Manager"),
            &be_object_path(guid),
            Some("org.beadm.BootEnvironment"),
            "rename",
            &(new_name,),
        )?;
        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), BeError> {
        let guid = self.get_be_guid(be_name)?;
        self.connection.call_method(
            Some("org.beadm.Manager"),
            &be_object_path(guid),
            Some("org.beadm.BootEnvironment"),
            "activate",
            &(temporary,),
        )?;
        Ok(())
    }

    fn deactivate(&self, be_name: &str) -> Result<(), BeError> {
        let guid = self.get_be_guid(be_name)?;
        self.connection.call_method(
            Some("org.beadm.Manager"),
            &be_object_path(guid),
            Some("org.beadm.BootEnvironment"),
            "deactivate",
            &(),
        )?;
        Ok(())
    }

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), BeError> {
        let guid = self.get_be_guid(be_name)?;
        self.connection.call_method(
            Some("org.beadm.Manager"),
            &be_object_path(guid),
            Some("org.beadm.BootEnvironment"),
            "rollback",
            &(snapshot,),
        )?;
        Ok(())
    }

    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, BeError> {
        let body = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                "/org/beadm/Manager",
                Some("org.freedesktop.DBus.ObjectManager"),
                "GetManagedObjects",
                &(),
            )?
            .body();

        let managed_objects: BTreeMap<ObjectPath, BTreeMap<String, BootEnvironment>> =
            body.deserialize()?;

        let mut boot_environments = Vec::new();
        for (_path, interfaces) in managed_objects {
            if let Some(be) = interfaces.get("org.beadm.BootEnvironment") {
                boot_environments.push(be.clone());
            }
        }
        Ok(boot_environments)
    }

    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, BeError> {
        let guid = self.get_be_guid(be_name)?;
        let snapshots_data: Vec<(String, String, u64, i64)> = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                &be_object_path(guid),
                Some("org.beadm.BootEnvironment"),
                "get_snapshots",
                &(),
            )?
            .body()
            .deserialize()?;

        let snapshots = snapshots_data
            .into_iter()
            .map(|(name, path, space, created)| Snapshot {
                name,
                path,
                space,
                created,
            })
            .collect();

        Ok(snapshots)
    }
}

// ============================================================================
// D-Bus Server (BeadmServer and related components)
// ============================================================================

/// Individual boot environment D-Bus object
#[derive(Clone)]
pub struct BootEnvironmentObject {
    name: String,
    guid: u64,
    client: Arc<dyn Client + Send + Sync>,
}

impl BootEnvironmentObject {
    pub fn new(name: String, guid: u64, client: Arc<dyn Client + Send + Sync>) -> Self {
        Self { name, guid, client }
    }

    /// Helper method to get the BootEnvironment data for this object
    fn get_boot_environment(&self) -> Result<BootEnvironment, BeError> {
        let envs = self.client.get_boot_environments()?;
        envs.into_iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| BeError::not_found(&self.name))
    }
}

#[interface(name = "org.beadm.BootEnvironment")]
impl BootEnvironmentObject {
    /// Boot environment name
    #[zbus(property)]
    fn name(&self) -> &str {
        &self.name
    }

    /// Boot environment dataset path
    #[zbus(property)]
    fn path(&self) -> zbus::fdo::Result<String> {
        let env = self.get_boot_environment()?;
        Ok(env.path)
    }

    /// Boot environment description
    #[zbus(property)]
    fn description(&self) -> zbus::fdo::Result<String> {
        let env = self.get_boot_environment()?;
        Ok(env.description.unwrap_or_default())
    }

    /// Current mountpoint (empty if not mounted)
    #[zbus(property)]
    fn mountpoint(&self) -> zbus::fdo::Result<String> {
        let env = self.get_boot_environment()?;
        Ok(env
            .mountpoint
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default())
    }

    /// Whether this is the currently active boot environment
    #[zbus(property(emits_changed_signal = "const"))]
    fn active(&self) -> zbus::fdo::Result<bool> {
        let env = self.get_boot_environment()?;
        Ok(env.active)
    }

    /// Whether this BE will be used for next boot
    #[zbus(property)]
    fn next_boot(&self) -> zbus::fdo::Result<bool> {
        let env = self.get_boot_environment()?;
        Ok(env.next_boot)
    }

    /// Whether this BE is set for one-time boot
    #[zbus(property)]
    fn boot_once(&self) -> zbus::fdo::Result<bool> {
        let env = self.get_boot_environment()?;
        Ok(env.boot_once)
    }

    /// Space used by this boot environment in bytes
    #[zbus(property)]
    fn space(&self) -> zbus::fdo::Result<u64> {
        let env = self.get_boot_environment()?;
        Ok(env.space)
    }

    /// Creation timestamp (Unix time)
    #[zbus(property(emits_changed_signal = "const"))]
    fn created(&self) -> zbus::fdo::Result<i64> {
        let env = self.get_boot_environment()?;
        Ok(env.created)
    }

    /// Destroy this boot environment
    fn destroy(
        &self,
        force_unmount: bool,
        force_no_verify: bool,
        snapshots: bool,
    ) -> zbus::fdo::Result<()> {
        self.client
            .destroy(&self.name, force_unmount, force_no_verify, snapshots)?;
        Ok(())
    }

    /// Mount this boot environment
    fn mount(&self, mountpoint: &str, read_only: bool) -> zbus::fdo::Result<()> {
        let mode = if read_only {
            MountMode::ReadOnly
        } else {
            MountMode::ReadWrite
        };

        self.client.mount(&self.name, mountpoint, mode)?;
        Ok(())
    }

    /// Unmount this boot environment
    fn unmount(&self, force: bool) -> zbus::fdo::Result<String> {
        let result = self.client.unmount(&self.name, force)?;
        Ok(result.map(|p| p.display().to_string()).unwrap_or_default())
    }

    /// Rename this boot environment
    fn rename(&self, new_name: &str) -> zbus::fdo::Result<()> {
        self.client.rename(&self.name, new_name)?;
        Ok(())
    }

    /// Activate this boot environment
    fn activate(&self, temporary: bool) -> zbus::fdo::Result<()> {
        self.client.activate(&self.name, temporary)?;
        Ok(())
    }

    /// Deactivate this boot environment
    fn deactivate(&self) -> zbus::fdo::Result<()> {
        self.client.deactivate(&self.name)?;
        Ok(())
    }

    /// Rollback to a snapshot
    fn rollback(&self, snapshot: &str) -> zbus::fdo::Result<()> {
        self.client.rollback(&self.name, snapshot)?;
        Ok(())
    }

    /// Get snapshots for this boot environment
    fn get_snapshots(&self) -> zbus::fdo::Result<Vec<(String, String, u64, i64)>> {
        let snapshots = self.client.get_snapshots(&self.name)?;
        Ok(snapshots
            .into_iter()
            .map(|snap| (snap.name, snap.path, snap.space, snap.created))
            .collect())
    }

    /// Get host ID for this boot environment
    fn get_hostid(&self) -> zbus::fdo::Result<u32> {
        let hostid = self.client.hostid(&self.name)?;
        Ok(hostid.unwrap_or(0))
    }
}

/// Main beadm manager implementing ObjectManager
#[derive(Clone)]
pub struct BeadmManager {
    client: Arc<dyn Client + Send + Sync>,
    objects: Arc<Mutex<HashMap<String, Arc<BootEnvironmentObject>>>>,
}

impl BeadmManager {
    pub fn new(client: Arc<dyn Client + Send + Sync>) -> Self {
        Self {
            client,
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Refresh the managed objects to match current boot environments
    fn refresh_objects(
        &self,
        object_server: &ObjectServer,
        object_manager: &BeadmObjectManager,
    ) -> ZbusResult<()> {
        let envs = self.client.get_boot_environments()?;

        let mut objects = self.objects.lock().unwrap();

        // Remove objects that no longer exist
        let mut to_remove = Vec::new();
        for (path, obj) in objects.iter() {
            if !envs.iter().any(|be| be.guid == obj.guid) {
                to_remove.push(path.clone());
            }
        }

        for path in to_remove {
            objects.remove(&path);
            let _ = object_server.remove::<BootEnvironmentObject, _>(path.as_str());

            // Emit InterfacesRemoved signal
            object_manager
                .emit_interfaces_removed(&path, vec!["org.beadm.BootEnvironment".to_string()]);
        }

        // Add new objects
        for env in &envs {
            let path = be_object_path(env.guid);
            if !objects.contains_key(path.as_str()) {
                let obj = BootEnvironmentObject::new(
                    env.name.clone(),
                    env.guid,
                    Arc::clone(&self.client),
                );
                object_server.at(&path, obj.clone())?;
                objects.insert(path.as_str().to_string(), Arc::new(obj));

                // Emit an InterfacesAdded signal.
                let mut interfaces = BTreeMap::new();
                interfaces.insert("org.beadm.BootEnvironment".to_string(), env);
                object_manager.emit_interfaces_added(path.as_str(), interfaces);
            }
        }

        Ok(())
    }
}

#[interface(name = "org.beadm.Manager")]
impl BeadmManager {
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
            Some(source)
        };

        self.client.create(name, desc, src, &properties)?;

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
    fn create_new(
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

        self.client.new(name, desc, hid, &properties)?;

        // Get the newly created BE to find its GUID
        let bes = self.client.get_boot_environments()?;
        let guid = bes
            .into_iter()
            .find(|be| be.name == name)
            .map(|be| be.guid)
            .ok_or_else(|| BeError::not_found(name))?;

        Ok(be_object_path(guid))
    }
}

/// ObjectManager interface implementation
#[derive(Clone)]
pub struct BeadmObjectManager {
    client: Arc<dyn Client + Send + Sync>,
    signal_context: Arc<Mutex<Option<SignalEmitter<'static>>>>,
}

impl BeadmObjectManager {
    pub fn new(client: Arc<dyn Client + Send + Sync>) -> Self {
        Self {
            client,
            signal_context: Arc::new(Mutex::new(None)),
        }
    }

    /// Helper method to emit InterfacesAdded signal
    pub fn emit_interfaces_added(
        &self,
        object_path: &str,
        _interfaces_and_properties: BTreeMap<String, &BootEnvironment>,
    ) {
        if let Some(_ctx) = self.signal_context.lock().unwrap().as_ref() {
            // For now, we'll skip the async signal emission in blocking context
            // This would require a runtime or different architecture
            println!("Signal: InterfacesAdded for {}", object_path);
        }
    }

    /// Helper method to emit InterfacesRemoved signal
    pub fn emit_interfaces_removed(&self, object_path: &str, interfaces: Vec<String>) {
        if let Some(_ctx) = self.signal_context.lock().unwrap().as_ref() {
            // For now, we'll skip the async signal emission in blocking context
            // This would require a runtime or different architecture
            println!(
                "Signal: InterfacesRemoved for {} (interfaces: {:?})",
                object_path, interfaces
            );
        }
    }
}

#[interface(name = "org.freedesktop.DBus.ObjectManager")]
impl BeadmObjectManager {
    /// Get all managed objects and their interfaces
    fn get_managed_objects(
        &self,
    ) -> zbus::fdo::Result<BTreeMap<ObjectPath<'static>, BTreeMap<String, BootEnvironment>>> {
        let mut objects = BTreeMap::new();
        for env in self.client.get_boot_environments()? {
            // We only manage objects with one interface.
            let path = be_object_path(env.guid);
            let mut interfaces = BTreeMap::new();
            interfaces.insert("org.beadm.BootEnvironment".to_string(), env);
            objects.insert(path, interfaces);
        }
        Ok(objects)
    }

    /// Signal emitted when new interfaces are added to an object
    #[zbus(signal)]
    async fn interfaces_added(
        emitter: &SignalEmitter<'_>,
        object_path: ObjectPath<'_>,
        interfaces_and_properties: BTreeMap<String, &BootEnvironment>,
    ) -> zbus::Result<()>;

    /// Signal emitted when interfaces are removed from an object
    #[zbus(signal)]
    async fn interfaces_removed(
        emitter: &SignalEmitter<'_>,
        object_path: ObjectPath<'_>,
        interfaces: Vec<String>,
    ) -> zbus::Result<()>;
}

/// Start a D-Bus service for boot environment administration.
pub fn serve<T: Client + 'static>(client: T, use_session_bus: bool) -> ZbusResult<()> {
    let thread_safe_client: Arc<dyn Client + Send + Sync> =
        Arc::new(crate::be::threadsafe::ThreadSafeClient::new(client));

    let manager = Arc::new(BeadmManager::new(Arc::clone(&thread_safe_client)));
    let object_manager = Arc::new(BeadmObjectManager::new(thread_safe_client));

    let builder = if use_session_bus {
        connection::Builder::session()?
    } else {
        connection::Builder::system()?
    };

    let connection = builder
        .name("org.beadm.Manager")?
        .serve_at("/org/beadm/Manager", (*manager).clone())?
        .serve_at("/org/beadm/Manager", (*object_manager).clone())?
        .build()?;

    let bus_type = if use_session_bus { "session" } else { "system" };
    println!(
        "D-Bus service started at org.beadm.Manager on {} bus",
        bus_type
    );

    // Initial population of objects
    manager.refresh_objects(&connection.object_server(), &object_manager)?;

    // Keep the connection alive and periodically refresh objects
    loop {
        std::thread::sleep(std::time::Duration::from_secs(5));
        if let Err(e) = manager.refresh_objects(&connection.object_server(), &object_manager) {
            eprintln!("Error refreshing objects: {}", e);
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
            "/org/beadm/BootEnvironments/1234567890abcdef"
        );
        assert_eq!(
            be_object_path(0x0).as_str(),
            "/org/beadm/BootEnvironments/0000000000000000"
        );
    }
}
