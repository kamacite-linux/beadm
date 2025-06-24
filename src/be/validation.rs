use super::Error;

/// Validates a boot environment name for ZFS dataset naming rules.
pub(crate) fn validate_be_name(be_name: &str, beroot: &str) -> Result<(), Error> {
    // Total length including beroot prefix + '/' must be under 256 chars.
    if beroot.len() + be_name.len() > 255 {
        return Err(Error::InvalidName {
            name: be_name.to_string(),
            reason: "name too long".to_string(),
        });
    }
    validate_component(be_name, true)
}

/// Validates a ZFS dataset name.
pub(crate) fn validate_dataset_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::InvalidName {
            name: name.to_string(),
            reason: "name cannot be empty".to_string(),
        });
    }
    for (i, comp) in name.split("/").enumerate() {
        if comp == "" {
            return Err(Error::InvalidName {
                name: name.to_string(),
                reason: if i == 0 {
                    "leading slash".to_string()
                } else {
                    "trailing slash".to_string()
                },
            });
        }
        validate_component(comp, true).map_err(|err| match err {
            Error::InvalidName {
                name: _ignored,
                reason,
            } => Error::InvalidName {
                name: name.to_string(),
                reason,
            },
            other => other,
        })?;
    }
    Ok(())
}

/// Validates a ZFS component (i.e. part of a dataset or snapshot name).
pub(crate) fn validate_component(name: &str, is_dataset: bool) -> Result<(), Error> {
    // We could call out to zfs_validate_name() here but this is more fun!
    //
    // ZFS dataset component must match something like the regular expression
    // [a-zA-Z0-9][a-zA-Z0-9-_:. ]?.
    //
    // But FreeBSD is documented to break on boot environments that contain
    // spaces, so let's prohibit that, too.

    if name.len() > 255 {
        return Err(Error::InvalidName {
            name: name.to_string(),
            reason: "name too long".to_string(),
        });
    }

    if name.is_empty() {
        return Err(Error::InvalidName {
            name: name.to_string(),
            reason: "name cannot be empty".to_string(),
        });
    }

    let mut chars = name.chars();

    // Snapshots can apparently begin with a non-alphanumeric character.
    if is_dataset {
        let first_char = chars.next().unwrap();
        if !first_char.is_ascii_alphanumeric() {
            return Err(Error::InvalidName {
                name: name.to_string(),
                reason: format!("name cannot begin with '{}'", first_char),
            });
        }
    }

    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '.' && c != '-' && c != '_' && c != ':' {
            return Err(Error::InvalidName {
                name: name.to_string(),
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
    fn test_be_name_validation() {
        assert!(validate_be_name("valid-name", "zfake/ROOT").is_ok());
        assert!(validate_be_name("test_env", "zfake/ROOT").is_ok());
        assert!(validate_be_name("env123", "zfake/ROOT").is_ok());
        assert!(validate_be_name("123numbers", "zfake/ROOT").is_ok());
        assert!(validate_be_name("test:colon", "zfake/ROOT").is_ok());
        assert!(validate_be_name("my.env", "zfake/ROOT").is_ok());

        assert!(validate_be_name(&"a".repeat(246), "zfake/ROOT").is_err()); // hit the length limit
        assert!(validate_be_name("", "zfake/ROOT").is_err()); // empty
        assert!(validate_be_name("-invalid", "zfake/ROOT").is_err()); // starts with dash
        assert!(validate_be_name(".invalid", "zfake/ROOT").is_err()); // starts with dot
        assert!(validate_be_name("_invalid", "zfake/ROOT").is_err()); // starts with underscore
        assert!(validate_be_name("invalid name", "zfake/ROOT").is_err()); // space
        assert!(validate_be_name("invalid@name", "zfake/ROOT").is_err()); // invalid char
        assert!(validate_be_name("test/name", "zfake/ROOT").is_err()); // invalid char
    }

    #[test]
    fn test_dataset_name_validation() {
        assert!(validate_dataset_name("tank").is_ok());
        assert!(validate_dataset_name("tank/ROOT").is_ok());
        assert!(validate_dataset_name("rpool/ROOT/default").is_ok());
        assert!(validate_dataset_name("tank/data/projects/work/client1/files").is_ok());

        // Invalid dataset names
        assert!(validate_dataset_name("").is_err()); // empty
        assert!(validate_dataset_name("/tank").is_err()); // leading slash
        assert!(validate_dataset_name("tank/").is_err()); // trailing slash
        assert!(validate_dataset_name("tank//ROOT").is_err()); // double slash
        assert!(validate_dataset_name("tank/ROOT/").is_err()); // trailing slash
        assert!(validate_dataset_name("tank/ /ROOT").is_err()); // space in component
        assert!(validate_dataset_name("tank/@invalid").is_err()); // invalid char
        assert!(validate_dataset_name("tank/ROOT/test name").is_err()); // space
        assert!(validate_dataset_name("-invalid/ROOT").is_err()); // starts with dash
        assert!(validate_dataset_name("tank/.invalid").is_err()); // component starts with dot
        assert!(validate_dataset_name("tank/_invalid").is_err()); // component starts with underscore
    }

    #[test]
    fn test_component_validation_datasets() {
        // Valid dataset components
        assert!(validate_component("tank", true).is_ok());
        assert!(validate_component("ROOT", true).is_ok());
        assert!(validate_component("default", true).is_ok());
        assert!(validate_component("test123", true).is_ok());
        assert!(validate_component("my-env", true).is_ok());
        assert!(validate_component("test_env", true).is_ok());
        assert!(validate_component("env.backup", true).is_ok());
        assert!(validate_component("ns:tagged", true).is_ok());
        assert!(validate_component("123numbers", true).is_ok());

        // Invalid dataset components
        assert!(validate_component("", true).is_err()); // empty
        assert!(validate_component("-invalid", true).is_err()); // starts with dash
        assert!(validate_component(".invalid", true).is_err()); // starts with dot
        assert!(validate_component("_invalid", true).is_err()); // starts with underscore
        assert!(validate_component(":invalid", true).is_err()); // starts with colon
        assert!(validate_component("invalid name", true).is_err()); // space
        assert!(validate_component("invalid@name", true).is_err()); // @ symbol
        assert!(validate_component("invalid/name", true).is_err()); // slash
        assert!(validate_component("invalid#name", true).is_err()); // hash
        assert!(validate_component("invalid%name", true).is_err()); // percent
        assert!(validate_component(&"a".repeat(256), true).is_err()); // too long
    }

    #[test]
    fn test_component_validation_snapshots() {
        // Valid snapshot components (less restrictive first character)
        assert!(validate_component("backup", false).is_ok());
        assert!(validate_component("2023-12-01", false).is_ok());
        assert!(validate_component("-checkpoint", false).is_ok()); // can start with dash
        assert!(validate_component(".hidden", false).is_ok()); // can start with dot
        assert!(validate_component("_private", false).is_ok()); // can start with underscore
        assert!(validate_component(":tagged", false).is_ok()); // can start with colon
        assert!(validate_component("123", false).is_ok());
        assert!(validate_component("a", false).is_ok());

        // Still invalid characters for snapshots
        assert!(validate_component("", false).is_err()); // empty
        assert!(validate_component("invalid name", false).is_err()); // space
        assert!(validate_component("invalid@name", false).is_err()); // @ symbol
        assert!(validate_component("invalid/name", false).is_err()); // slash
        assert!(validate_component("invalid#name", false).is_err()); // hash
        assert!(validate_component("invalid%name", false).is_err()); // percent
        assert!(validate_component(&"a".repeat(256), false).is_err()); // too long
    }

    #[test]
    fn test_dataset_name_edge_cases() {
        // Test various edge cases for multi-component datasets

        // Maximum depth nesting
        let deep_path = (0..20)
            .map(|i| format!("level{}", i))
            .collect::<Vec<_>>()
            .join("/");
        assert!(validate_dataset_name(&deep_path).is_ok());

        // Mixed valid characters in all components
        assert!(
            validate_dataset_name("pool1/data-set/sub_component/ns:tagged/file.backup").is_ok()
        );

        // Components with all valid special characters
        assert!(validate_dataset_name("a123/b-c/d_e/f.g/h:i").is_ok());

        // Numbers-only components are valid
        assert!(validate_dataset_name("123/456/789").is_ok());

        // Single character components
        assert!(validate_dataset_name("a/b/c/d/e").is_ok());
    }

    #[test]
    fn test_dataset_validation_error_messages() {
        // Test that error messages correctly identify the full dataset name
        let result = validate_dataset_name("tank/-invalid/ROOT");
        assert!(result.is_err());
        if let Err(Error::InvalidName { name, reason: _ }) = result {
            assert_eq!(name, "tank/-invalid/ROOT");
        } else {
            panic!("Expected InvalidName error");
        }

        let result = validate_dataset_name("tank/ROOT/ ");
        assert!(result.is_err());
        if let Err(Error::InvalidName { name, reason: _ }) = result {
            assert_eq!(name, "tank/ROOT/ ");
        } else {
            panic!("Expected InvalidName error");
        }
    }
}
