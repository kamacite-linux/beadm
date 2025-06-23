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
}
