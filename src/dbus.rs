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

/// Sanitize a boot environment name for use in D-Bus object paths
/// D-Bus object paths can only contain [A-Za-z0-9_/]
fn sanitize_be_name(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' => c,
            _ => '_',
        })
        .collect()
}

/// Generate D-Bus object path for a boot environment
fn be_object_path(name: &str) -> String {
    format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(name))
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
        }
        .map_err(|e| BeError::ZfsError {
            message: format!("Failed to connect to D-Bus: {}", e),
        })?;

        Ok(Self { connection })
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
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?
            .body()
            .deserialize()
            .map_err(|e| BeError::ZfsError {
                message: format!("Failed to deserialize response: {}", e),
            })?;

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
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?
            .body()
            .deserialize()
            .map_err(|e| BeError::ZfsError {
                message: format!("Failed to deserialize response: {}", e),
            })?;

        Ok(())
    }

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        force_no_verify: bool,
        snapshots: bool,
    ) -> Result<(), BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(target));

        self.connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "destroy",
                &(force_unmount, force_no_verify, snapshots),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?;

        Ok(())
    }

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), BeError> {
        let read_only = match mode {
            MountMode::ReadOnly => true,
            MountMode::ReadWrite => false,
        };

        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(be_name));

        self.connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "mount",
                &(mountpoint, read_only),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?;

        Ok(())
    }

    fn unmount(&self, target: &str, force: bool) -> Result<Option<PathBuf>, BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(target));

        let result: String = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "unmount",
                &(force,),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?
            .body()
            .deserialize()
            .map_err(|e| BeError::ZfsError {
                message: format!("Failed to deserialize response: {}", e),
            })?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(PathBuf::from(result)))
        }
    }

    fn hostid(&self, be_name: &str) -> Result<Option<u32>, BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(be_name));

        let hostid: u32 = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "get_hostid",
                &(),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?
            .body()
            .deserialize()
            .map_err(|e| BeError::ZfsError {
                message: format!("Failed to deserialize response: {}", e),
            })?;

        if hostid == 0 {
            Ok(None)
        } else {
            Ok(Some(hostid))
        }
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(be_name));

        self.connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "rename",
                &(new_name,),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?;

        Ok(())
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(be_name));

        self.connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "activate",
                &(temporary,),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?;

        Ok(())
    }

    fn deactivate(&self, be_name: &str) -> Result<(), BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(be_name));

        self.connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "deactivate",
                &(),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?;

        Ok(())
    }

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(be_name));

        self.connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "rollback",
                &(snapshot,),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?;

        Ok(())
    }

    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, BeError> {
        use zbus::zvariant::OwnedValue;

        let message = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                "/org/beadm/Manager",
                Some("org.freedesktop.DBus.ObjectManager"),
                "GetManagedObjects",
                &(),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("Failed to get managed objects: {}", e),
            })?;

        let managed_objects: BTreeMap<String, BTreeMap<String, BTreeMap<String, OwnedValue>>> =
            message
                .body()
                .deserialize()
                .map_err(|e| BeError::ZfsError {
                    message: format!("Failed to deserialize managed objects: {}", e),
                })?;

        let mut boot_environments = Vec::new();

        for (_object_path, interfaces) in managed_objects {
            if let Some(be_interface) = interfaces.get("org.beadm.BootEnvironment") {
                let name = be_interface
                    .get("Name")
                    .and_then(|v| {
                        if let Ok(s) = String::try_from(v.clone()) {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| BeError::ZfsError {
                        message: "Missing or invalid Name property".to_string(),
                    })?;

                let path = be_interface
                    .get("Path")
                    .and_then(|v| {
                        if let Ok(s) = String::try_from(v.clone()) {
                            Some(s)
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| BeError::ZfsError {
                        message: "Missing or invalid Path property".to_string(),
                    })?;

                let description = be_interface.get("Description").and_then(|v| {
                    if let Ok(s) = String::try_from(v.clone()) {
                        if s.is_empty() { None } else { Some(s) }
                    } else {
                        None
                    }
                });

                let mountpoint = be_interface.get("Mountpoint").and_then(|v| {
                    if let Ok(s) = String::try_from(v.clone()) {
                        if s.is_empty() {
                            None
                        } else {
                            Some(PathBuf::from(s))
                        }
                    } else {
                        None
                    }
                });

                let active = be_interface
                    .get("Active")
                    .and_then(|v| {
                        if let Ok(b) = bool::try_from(v.clone()) {
                            Some(b)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(false);

                let next_boot = be_interface
                    .get("NextBoot")
                    .and_then(|v| {
                        if let Ok(b) = bool::try_from(v.clone()) {
                            Some(b)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(false);

                let boot_once = be_interface
                    .get("BootOnce")
                    .and_then(|v| {
                        if let Ok(b) = bool::try_from(v.clone()) {
                            Some(b)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(false);

                let space = be_interface
                    .get("Space")
                    .and_then(|v| {
                        if let Ok(n) = u64::try_from(v.clone()) {
                            Some(n)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);

                let created = be_interface
                    .get("Created")
                    .and_then(|v| {
                        if let Ok(n) = i64::try_from(v.clone()) {
                            Some(n)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);

                boot_environments.push(BootEnvironment {
                    name,
                    path,
                    description,
                    mountpoint,
                    active,
                    next_boot,
                    boot_once,
                    space,
                    created,
                });
            }
        }

        Ok(boot_environments)
    }

    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, BeError> {
        let object_path = format!("/org/beadm/BootEnvironments/{}", sanitize_be_name(be_name));

        let snapshots_data: Vec<(String, String, u64, i64)> = self
            .connection
            .call_method(
                Some("org.beadm.Manager"),
                object_path.as_str(),
                Some("org.beadm.BootEnvironment"),
                "get_snapshots",
                &(),
            )
            .map_err(|e| BeError::ZfsError {
                message: format!("D-Bus call failed: {}", e),
            })?
            .body()
            .deserialize()
            .map_err(|e| BeError::ZfsError {
                message: format!("Failed to deserialize response: {}", e),
            })?;

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
    client: Arc<dyn Client + Send + Sync>,
}

impl BootEnvironmentObject {
    pub fn new(name: String, client: Arc<dyn Client + Send + Sync>) -> Self {
        Self { name, client }
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
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

        Ok(env.path.clone())
    }

    /// Boot environment description
    #[zbus(property)]
    fn description(&self) -> zbus::fdo::Result<String> {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

        Ok(env.description.clone().unwrap_or_default())
    }

    /// Current mountpoint (empty if not mounted)
    #[zbus(property)]
    fn mountpoint(&self) -> zbus::fdo::Result<String> {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

        Ok(env
            .mountpoint
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default())
    }

    /// Whether this is the currently active boot environment
    #[zbus(property)]
    fn active(&self) -> zbus::fdo::Result<bool> {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

        Ok(env.active)
    }

    /// Whether this BE will be used for next boot
    #[zbus(property)]
    fn next_boot(&self) -> zbus::fdo::Result<bool> {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

        Ok(env.next_boot)
    }

    /// Whether this BE is set for one-time boot
    #[zbus(property)]
    fn boot_once(&self) -> zbus::fdo::Result<bool> {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

        Ok(env.boot_once)
    }

    /// Space used by this boot environment in bytes
    #[zbus(property)]
    fn space(&self) -> zbus::fdo::Result<u64> {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

        Ok(env.space)
    }

    /// Creation timestamp (Unix time)
    #[zbus(property)]
    fn created(&self) -> zbus::fdo::Result<i64> {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let env = envs
            .iter()
            .find(|be| be.name == self.name)
            .ok_or_else(|| zbus::fdo::Error::Failed("Boot environment not found".to_string()))?;

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
            .destroy(&self.name, force_unmount, force_no_verify, snapshots)
            .map_err(|e| {
                zbus::fdo::Error::Failed(format!("Failed to destroy boot environment: {}", e))
            })
    }

    /// Mount this boot environment
    fn mount(&self, mountpoint: &str, read_only: bool) -> zbus::fdo::Result<()> {
        let mode = if read_only {
            MountMode::ReadOnly
        } else {
            MountMode::ReadWrite
        };

        self.client
            .mount(&self.name, mountpoint, mode)
            .map_err(|e| {
                zbus::fdo::Error::Failed(format!("Failed to mount boot environment: {}", e))
            })
    }

    /// Unmount this boot environment
    fn unmount(&self, force: bool) -> zbus::fdo::Result<String> {
        let result = self.client.unmount(&self.name, force).map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to unmount boot environment: {}", e))
        })?;

        Ok(result.map(|p| p.display().to_string()).unwrap_or_default())
    }

    /// Rename this boot environment
    fn rename(&self, new_name: &str) -> zbus::fdo::Result<()> {
        self.client.rename(&self.name, new_name).map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to rename boot environment: {}", e))
        })
    }

    /// Activate this boot environment
    fn activate(&self, temporary: bool) -> zbus::fdo::Result<()> {
        self.client.activate(&self.name, temporary).map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to activate boot environment: {}", e))
        })
    }

    /// Deactivate this boot environment
    fn deactivate(&self) -> zbus::fdo::Result<()> {
        self.client.deactivate(&self.name).map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to deactivate boot environment: {}", e))
        })
    }

    /// Rollback to a snapshot
    fn rollback(&self, snapshot: &str) -> zbus::fdo::Result<()> {
        self.client.rollback(&self.name, snapshot).map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to rollback boot environment: {}", e))
        })
    }

    /// Get snapshots for this boot environment
    fn get_snapshots(&self) -> zbus::fdo::Result<Vec<(String, String, u64, i64)>> {
        let snapshots = self
            .client
            .get_snapshots(&self.name)
            .map_err(|e| zbus::fdo::Error::Failed(format!("Failed to get snapshots: {}", e)))?;

        Ok(snapshots
            .into_iter()
            .map(|snap| (snap.name, snap.path, snap.space, snap.created))
            .collect())
    }

    /// Get host ID for this boot environment
    fn get_hostid(&self) -> zbus::fdo::Result<u32> {
        let hostid = self
            .client
            .hostid(&self.name)
            .map_err(|e| zbus::fdo::Error::Failed(format!("Failed to get hostid: {}", e)))?;

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
        let envs = self
            .client
            .get_boot_environments()
            .map_err(|e| zbus::Error::Failure(format!("Failed to get boot environments: {}", e)))?;

        let mut objects = self.objects.lock().unwrap();

        // Remove objects that no longer exist
        let mut to_remove = Vec::new();
        for (path, obj) in objects.iter() {
            if !envs.iter().any(|be| be.name == obj.name) {
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
            let path = be_object_path(&env.name);
            if !objects.contains_key(&path) {
                let obj = BootEnvironmentObject::new(env.name.clone(), Arc::clone(&self.client));
                object_server.at(path.as_str(), obj.clone())?;
                objects.insert(path.clone(), Arc::new(obj));

                // Create properties for the signal
                let mut properties = BTreeMap::new();
                properties.insert(
                    "Name".to_string(),
                    zbus::zvariant::Value::from(env.name.clone()),
                );
                properties.insert(
                    "Path".to_string(),
                    zbus::zvariant::Value::from(env.path.clone()),
                );
                properties.insert(
                    "Description".to_string(),
                    zbus::zvariant::Value::from(env.description.clone().unwrap_or_default()),
                );
                properties.insert(
                    "Mountpoint".to_string(),
                    zbus::zvariant::Value::from(
                        env.mountpoint
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_default(),
                    ),
                );
                properties.insert(
                    "Active".to_string(),
                    zbus::zvariant::Value::from(env.active),
                );
                properties.insert(
                    "NextBoot".to_string(),
                    zbus::zvariant::Value::from(env.next_boot),
                );
                properties.insert(
                    "BootOnce".to_string(),
                    zbus::zvariant::Value::from(env.boot_once),
                );
                properties.insert("Space".to_string(), zbus::zvariant::Value::from(env.space));
                properties.insert(
                    "Created".to_string(),
                    zbus::zvariant::Value::from(env.created),
                );

                let mut interfaces = BTreeMap::new();
                interfaces.insert("org.beadm.BootEnvironment".to_string(), properties);

                // Emit InterfacesAdded signal
                object_manager.emit_interfaces_added(&path, interfaces);
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
    ) -> zbus::fdo::Result<String> {
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

        self.client
            .create(name, desc, src, &properties)
            .map_err(|e| {
                zbus::fdo::Error::Failed(format!("Failed to create boot environment: {}", e))
            })?;

        Ok(be_object_path(name))
    }

    /// Create a new empty boot environment
    fn create_new(
        &self,
        name: &str,
        description: &str,
        host_id: &str,
        properties: Vec<String>,
    ) -> zbus::fdo::Result<String> {
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

        self.client.new(name, desc, hid, &properties).map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to create new boot environment: {}", e))
        })?;

        Ok(be_object_path(name))
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
        _interfaces_and_properties: BTreeMap<String, BTreeMap<String, zbus::zvariant::Value>>,
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
    ) -> zbus::fdo::Result<
        BTreeMap<String, BTreeMap<String, BTreeMap<String, zbus::zvariant::Value>>>,
    > {
        let envs = self.client.get_boot_environments().map_err(|e| {
            zbus::fdo::Error::Failed(format!("Failed to get boot environments: {}", e))
        })?;

        let mut objects = BTreeMap::new();

        for env in envs {
            let path = be_object_path(&env.name);
            let mut interfaces = BTreeMap::new();

            // Add org.beadm.BootEnvironment interface with properties
            let mut properties = BTreeMap::new();
            properties.insert("Name".to_string(), zbus::zvariant::Value::from(env.name));
            properties.insert("Path".to_string(), zbus::zvariant::Value::from(env.path));
            properties.insert(
                "Description".to_string(),
                zbus::zvariant::Value::from(env.description.unwrap_or_default()),
            );
            properties.insert(
                "Mountpoint".to_string(),
                zbus::zvariant::Value::from(
                    env.mountpoint
                        .map(|p| p.display().to_string())
                        .unwrap_or_default(),
                ),
            );
            properties.insert(
                "Active".to_string(),
                zbus::zvariant::Value::from(env.active),
            );
            properties.insert(
                "NextBoot".to_string(),
                zbus::zvariant::Value::from(env.next_boot),
            );
            properties.insert(
                "BootOnce".to_string(),
                zbus::zvariant::Value::from(env.boot_once),
            );
            properties.insert("Space".to_string(), zbus::zvariant::Value::from(env.space));
            properties.insert(
                "Created".to_string(),
                zbus::zvariant::Value::from(env.created),
            );

            interfaces.insert("org.beadm.BootEnvironment".to_string(), properties);
            objects.insert(path, interfaces);
        }

        Ok(objects)
    }

    /// Signal emitted when new interfaces are added to an object
    #[zbus(signal)]
    async fn interfaces_added(
        &self,
        _signal_ctxt: &SignalEmitter<'_>,
        object_path: &str,
        interfaces_and_properties: BTreeMap<String, BTreeMap<String, zbus::zvariant::Value<'_>>>,
    ) -> zbus::Result<()>;

    /// Signal emitted when interfaces are removed from an object
    #[zbus(signal)]
    async fn interfaces_removed(
        &self,
        _signal_ctxt: &SignalEmitter<'_>,
        object_path: &str,
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
    fn test_sanitize_be_name() {
        assert_eq!(sanitize_be_name("default"), "default");
        assert_eq!(sanitize_be_name("test-be"), "test_be");
        assert_eq!(sanitize_be_name("test.be"), "test_be");
        assert_eq!(sanitize_be_name("test@snapshot"), "test_snapshot");
        assert_eq!(sanitize_be_name("test/path"), "test_path");
    }

    #[test]
    fn test_be_object_path() {
        assert_eq!(
            be_object_path("default"),
            "/org/beadm/BootEnvironments/default"
        );
        assert_eq!(
            be_object_path("test-be"),
            "/org/beadm/BootEnvironments/test_be"
        );
    }
}
