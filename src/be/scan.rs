// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fs;
use std::path::Path;

/// Relevant content from an `/etc/os-release` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OsRelease {
    /// The `ID` parameter identifying the distribution.
    pub id: String,
    /// The "pretty" name of the distribution.
    pub pretty: String,
}

impl Default for OsRelease {
    fn default() -> Self {
        // Match the defaults from os-release(5).
        Self {
            id: "linux".to_string(),
            pretty: "Linux".to_string(),
        }
    }
}

impl OsRelease {
    /// Read and parse an `/etc/os-release` file from a path.
    pub fn from_path<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        fs::read_to_string(path).map(Self::parse)
    }

    /// Parse an `/etc/os-release` file.
    pub fn parse<S: AsRef<str>>(contents: S) -> Self {
        let mut out = Self::default();
        for line in contents.as_ref().lines() {
            if let Some(value) = line.strip_prefix("ID=") {
                out.id = value.trim_matches('"').to_string();
            } else if let Some(value) = line.strip_prefix("PRETTY_NAME=") {
                out.pretty = value.trim_matches('"').to_string();
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_os_release_parsing() {
        // Sourced from: https://github.com/which-distro/os-release
        let cases = vec![
            (
                r#"PRETTY_NAME="Ubuntu 24.04 LTS"
NAME="Ubuntu"
VERSION_ID="24.04"
VERSION="24.04 LTS (Noble Numbat)"
VERSION_CODENAME=noble
ID=ubuntu
ID_LIKE=debian
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
UBUNTU_CODENAME=noble
LOGO=ubuntu-logo
"#,
                OsRelease {
                    id: "ubuntu".to_string(),
                    pretty: "Ubuntu 24.04 LTS".to_string(),
                },
            ),
            (
                r#"PRETTY_NAME="Debian GNU/Linux 12 (bookworm)"
NAME="Debian GNU/Linux"
VERSION_ID="12"
VERSION="12 (bookworm)"
VERSION_CODENAME=bookworm
ID=debian
HOME_URL="https://www.debian.org/"
SUPPORT_URL="https://www.debian.org/support"
BUG_REPORT_URL="https://bugs.debian.org/"
"#,
                OsRelease {
                    id: "debian".to_string(),
                    pretty: "Debian GNU/Linux 12 (bookworm)".to_string(),
                    ..Default::default()
                },
            ),
            (
                r#"NAME=FreeBSD
VERSION="13.2-RELEASE"
VERSION_ID="13.2"
ID=freebsd
ANSI_COLOR="0;31"
PRETTY_NAME="FreeBSD 13.2-RELEASE"
CPE_NAME="cpe:/o:freebsd:freebsd:13.2"
HOME_URL="https://FreeBSD.org/"
BUG_REPORT_URL="https://bugs.FreeBSD.org/"
"#,
                OsRelease {
                    id: "freebsd".to_string(),
                    pretty: "FreeBSD 13.2-RELEASE".to_string(),
                    ..Default::default()
                },
            ),
            (
                r#"NAME="OmniOS"
PRETTY_NAME="OmniOS Community Edition v11 r151048"
CPE_NAME="cpe:/o:omniosce:omnios:11:151048:0"
ID=omnios
VERSION=r151048
VERSION_ID=r151048
BUILD_ID=151048.0.2023.11.04
HOME_URL="https://omnios.org/"
SUPPORT_URL="https://omnios.org/"
BUG_REPORT_URL="https://github.com/omniosorg/omnios-build/issues/new"
"#,
                OsRelease {
                    id: "omnios".to_string(),
                    pretty: "OmniOS Community Edition v11 r151048".to_string(),
                    ..Default::default()
                },
            ),
            (
                r#"NAME="Arch Linux"
PRETTY_NAME="Arch Linux"
ID=arch
BUILD_ID=rolling
VERSION_ID=TEMPLATE_VERSION_ID
ANSI_COLOR="38;2;23;147;209"
HOME_URL="https://archlinux.org/"
DOCUMENTATION_URL="https://wiki.archlinux.org/"
SUPPORT_URL="https://bbs.archlinux.org/"
BUG_REPORT_URL="https://bugs.archlinux.org/"
PRIVACY_POLICY_URL="https://terms.archlinux.org/docs/privacy-policy/"
LOGO=archlinux-logo
"#,
                OsRelease {
                    id: "arch".to_string(),
                    pretty: "Arch Linux".to_string(),
                },
            ),
            (
                r#"NAME="Alpine Linux"
ID=alpine
VERSION_ID=3.21.4
PRETTY_NAME="Alpine Linux v3.21"
HOME_URL="https://alpinelinux.org/"
BUG_REPORT_URL="https://gitlab.alpinelinux.org/alpine/aports/-/issues"
"#,
                OsRelease {
                    id: "alpine".to_string(),
                    pretty: "Alpine Linux v3.21".to_string(),
                },
            ),
            (
                r#"NAME=Gentoo
ID=gentoo
PRETTY_NAME="Gentoo/Linux"
ANSI_COLOR="1;32"
HOME_URL="https://www.gentoo.org/"
SUPPORT_URL="https://www.gentoo.org/support/"
BUG_REPORT_URL="https://bugs.gentoo.org/"
"#,
                OsRelease {
                    id: "gentoo".to_string(),
                    pretty: "Gentoo/Linux".to_string(),
                },
            ),
            (
                r#"NAME="Chimera"
ID="chimera"
PRETTY_NAME="Chimera Linux"
HOME_URL="https://chimera-linux.org"
DOCUMENTATION_URL="https://chimera-linux.org/docs"
LOGO="chimera-logo"
ANSI_COLOR="0;38;2;214;79;93"
"#,
                OsRelease {
                    id: "chimera".to_string(),
                    pretty: "Chimera Linux".to_string(),
                },
            ),
            (
                r#"NAME="Void"
ID="void"
PRETTY_NAME="Void Linux"
HOME_URL="https://voidlinux.org/"
DOCUMENTATION_URL="https://docs.voidlinux.org/"
LOGO="void-logo"
ANSI_COLOR="0;38;2;71;128;97"

DISTRIB_ID="void"
"#,
                OsRelease {
                    id: "void".to_string(),
                    pretty: "Void Linux".to_string(),
                },
            ),
            ("", Default::default()),
        ];

        for (input, expected) in cases {
            let parsed = OsRelease::parse(input);
            assert_eq!(parsed, expected, "Failed for input: {:?}", input);
        }
    }
}
