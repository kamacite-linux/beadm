use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::{BootEnvironment, Client, Error, MountMode, Snapshot};

/// Thread-safe wrapper around any Client implementation
///
/// This wrapper uses Arc<Mutex<T>> to provide thread-safe access to non-thread-safe
/// Client implementations, enabling their use in multi-threaded contexts like D-Bus servers.
pub struct ThreadSafeClient<T: Client> {
    inner: Arc<Mutex<T>>,
}

impl<T: Client> ThreadSafeClient<T> {
    pub fn new(client: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(client)),
        }
    }
}

impl<T: Client> Clone for ThreadSafeClient<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Client> Client for ThreadSafeClient<T> {
    fn get_boot_environments(&self) -> Result<Vec<BootEnvironment>, Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.get_boot_environments()
    }

    fn create(
        &self,
        be_name: &str,
        description: Option<&str>,
        source: Option<&str>,
        properties: &[String],
    ) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.create(be_name, description, source, properties)
    }

    fn new(
        &self,
        be_name: &str,
        description: Option<&str>,
        host_id: Option<&str>,
        properties: &[String],
    ) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.new(be_name, description, host_id, properties)
    }

    fn destroy(
        &self,
        target: &str,
        force_unmount: bool,
        force_no_verify: bool,
        snapshots: bool,
    ) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.destroy(target, force_unmount, force_no_verify, snapshots)
    }

    fn mount(&self, be_name: &str, mountpoint: &str, mode: MountMode) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.mount(be_name, mountpoint, mode)
    }

    fn unmount(&self, target: &str, force: bool) -> Result<Option<PathBuf>, Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.unmount(target, force)
    }

    fn rename(&self, be_name: &str, new_name: &str) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.rename(be_name, new_name)
    }

    fn activate(&self, be_name: &str, temporary: bool) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.activate(be_name, temporary)
    }

    fn deactivate(&self, be_name: &str) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.deactivate(be_name)
    }

    fn rollback(&self, be_name: &str, snapshot: &str) -> Result<(), Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.rollback(be_name, snapshot)
    }

    fn get_snapshots(&self, be_name: &str) -> Result<Vec<Snapshot>, Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.get_snapshots(be_name)
    }

    fn hostid(&self, be_name: &str) -> Result<Option<u32>, Error> {
        let client = self.inner.lock().map_err(|_| Error::ZfsError {
            message: "Failed to acquire client lock".to_string(),
        })?;
        client.hostid(be_name)
    }
}

// Implement Send and Sync for ThreadSafeClient to make it truly thread-safe
unsafe impl<T: Client> Send for ThreadSafeClient<T> {}
unsafe impl<T: Client> Sync for ThreadSafeClient<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::be::mock::EmulatorClient;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_thread_safe_wrapper_basic_operations() {
        let client = EmulatorClient::sampled();
        let thread_safe_client = ThreadSafeClient::new(client);

        // Test basic operations work
        let envs = thread_safe_client.get_boot_environments().unwrap();
        assert!(!envs.is_empty());

        // Test create and destroy
        thread_safe_client
            .create("test-be", Some("Test description"), None, &[])
            .unwrap();

        let envs = thread_safe_client.get_boot_environments().unwrap();
        assert!(envs.iter().any(|be| be.name == "test-be"));

        thread_safe_client
            .destroy("test-be", false, false, false)
            .unwrap();
    }

    #[test]
    fn test_thread_safe_wrapper_concurrent_access() {
        let client = EmulatorClient::sampled();
        let thread_safe_client = Arc::new(ThreadSafeClient::new(client));

        let mut handles = vec![];

        // Spawn multiple threads that access the client concurrently
        for i in 0..5 {
            let client_clone = Arc::clone(&thread_safe_client);
            let handle = thread::spawn(move || {
                // Each thread creates a BE, lists BEs, then destroys the BE
                let be_name = format!("thread-be-{}", i);

                client_clone
                    .create(&be_name, Some("Thread test"), None, &[])
                    .unwrap();

                let envs = client_clone.get_boot_environments().unwrap();
                assert!(envs.iter().any(|be| be.name == be_name));

                client_clone.destroy(&be_name, false, false, false).unwrap();
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify no thread-created BEs remain
        let final_envs = thread_safe_client.get_boot_environments().unwrap();
        for env in &final_envs {
            assert!(!env.name.starts_with("thread-be-"));
        }
    }

    #[test]
    fn test_thread_safe_wrapper_clone() {
        let client = EmulatorClient::sampled();
        let thread_safe_client = ThreadSafeClient::new(client);
        let cloned_client = thread_safe_client.clone();

        // Both should work and access the same underlying client
        let envs1 = thread_safe_client.get_boot_environments().unwrap();
        let envs2 = cloned_client.get_boot_environments().unwrap();

        assert_eq!(envs1.len(), envs2.len());
    }
}
