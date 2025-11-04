// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fs;
use std::path::{Path, PathBuf};

use crate::Error;

/// Relevant content from an `/etc/os-release` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OsRelease {
    /// The `ID` parameter identifying the distribution.
    pub id: String,
    /// The `ID_LIKE` parameter, a space-separated list of distribution IDs this
    /// one is similar to (if any).
    pub id_like: Vec<String>,
    /// The "pretty" name of the distribution.
    pub pretty: String,
}

impl Default for OsRelease {
    fn default() -> Self {
        // Match the defaults from os-release(5).
        Self {
            id: "linux".to_string(),
            id_like: Vec::new(),
            pretty: "Linux".to_string(),
        }
    }
}

impl OsRelease {
    /// Attempt to find and parse the `/etc/os-release` (or
    /// `/usr/lib/os-release`) file from the root filesystem at `root_dir`.
    pub fn scan<P: AsRef<Path>>(root_dir: P) -> std::io::Result<Option<Self>> {
        let etc_path = root_dir.as_ref().join("etc/os-release");
        if etc_path.exists() {
            return Self::from_path(&etc_path).map(Some);
        }
        let usr_path = root_dir.as_ref().join("usr/lib/os-release");
        if usr_path.exists() {
            return Self::from_path(&usr_path).map(Some);
        }
        Ok(None)
    }

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
            } else if let Some(value) = line.strip_prefix("ID_LIKE=") {
                out.id_like = value
                    .trim_matches('"')
                    .split_whitespace()
                    .map(|s| s.to_string())
                    .collect();
            } else if let Some(value) = line.strip_prefix("PRETTY_NAME=") {
                out.pretty = value.trim_matches('"').to_string();
            }
        }
        out
    }

    /// Combined distribution identifiers.
    pub fn ids(&self) -> Vec<&str> {
        let mut ids = vec![self.id.as_str()];
        ids.extend(self.id_like.iter().map(|s| s.as_str()));
        ids
    }
}

/// A matching kernel and initial RAM disk pair.
#[derive(Debug, PartialEq, Eq)]
pub struct KernelPair {
    /// Path to the kernel image.
    pub path: PathBuf,
    /// Path to the initial RAM disk.
    pub initrd: PathBuf,
}

impl KernelPair {
    /// Find the latest kernel and initramfs pair -- if any -- in the `/boot`
    /// directory of the root filesystem at `root_dir`.
    ///
    /// This uses the same approach as ZFSBootMenu, and should be compatible.
    pub fn scan<P: AsRef<Path>>(root_dir: P) -> Result<Self, Error> {
        let boot_dir = root_dir.as_ref().join("boot");

        // Match the kernel prefixes supported by ZFSBootMenu.
        const KERNEL_PREFIXES: [&str; 5] = ["vmlinuz", "vmlinux", "linux", "linuz", "kernel"];

        let mut kernel_candidates = Vec::new();
        for entry in boot_dir.read_dir()? {
            let path = entry?.path();
            // Be slightly more picky than ZBM and ignore non-UTF8 file names.
            let basename = match path.file_name().and_then(|name| name.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            for prefix in KERNEL_PREFIXES {
                if let Some(suffix) = basename.strip_prefix(prefix) {
                    let version = suffix.strip_prefix("-").unwrap_or(suffix).to_string();
                    kernel_candidates.push((path, basename, version));
                    break; // No boot directory should have multiple kernel prefixes.
                }
            }
        }

        if kernel_candidates.is_empty() {
            return Err(Error::KernelNotFound);
        }

        // Sort kernels by version using "natural" ordering and pick the latest one.
        kernel_candidates.sort_by(|a, b| natord::compare(&b.2, &a.2));
        let (kernel_path, kernel_name, kernel_version) =
            kernel_candidates.into_iter().next().unwrap();

        // We generally follow ZFSBootMenu here and support the following cases for
        // matching an initramfs to a kernels:
        //
        // * initramfs-${label}${extension}
        // * initramfs${extension}-${label}
        // * initrd-${label}${extension}
        // * initrd${extension}-${label}
        //
        // Where the "label" is the full kernel name or just the trailing version
        // number or other identifier.
        const INITRAMFS_PREFIXES: [&str; 2] = ["initramfs", "initrd"];
        const EXTENSIONS: [&str; 16] = [
            "",
            ".img",
            ".gz",
            ".img.gz",
            ".bz2",
            ".img.bz2",
            ".xz",
            ".img.xz",
            ".lzma",
            ".img.lzma",
            ".lz4",
            ".img.lz4",
            ".lzo",
            ".img.lzo",
            ".zstd",
            ".img.zstd",
        ];

        for label in &[kernel_name, kernel_version] {
            for prefix in INITRAMFS_PREFIXES {
                for ext in EXTENSIONS {
                    let initrd = boot_dir.join(format!("{}-{}{}", prefix, label, ext));
                    if initrd.exists() {
                        return Ok(Self::new(kernel_path.to_owned(), initrd));
                    }
                    let initrd = boot_dir.join(format!("{}{}-{}", prefix, ext, label));
                    if initrd.exists() {
                        return Ok(Self::new(kernel_path.to_owned(), initrd));
                    }
                }
            }
        }

        Err(Error::KernelNotFound)
    }

    /// Create a new `KernelPair` from a matching kernel binary and initramfs.
    fn new(path: PathBuf, initrd: PathBuf) -> Self {
        KernelPair { path, initrd }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

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
                    id_like: vec!["debian".to_string()],
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
                    id_like: Vec::new(),
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
                    id_like: Vec::new(),
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
                    id_like: Vec::new(),
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
                    id_like: Vec::new(),
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
                    id_like: Vec::new(),
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

    #[test]
    fn test_os_release_scan() {
        let tmpdir = TempDir::new().unwrap();

        // 1. No os-release(5) file.
        let result = OsRelease::scan(tmpdir.path()).unwrap();
        assert!(result.is_none());

        // 2. Found /usr/lib/os-release.
        fs::create_dir_all(tmpdir.path().join("usr/lib")).unwrap();
        fs::write(tmpdir.path().join("usr/lib/os-release"), "ID=usrlib\n").unwrap();
        assert_eq!(
            OsRelease::scan(tmpdir.path()).unwrap(),
            Some(OsRelease {
                id: "usrlib".to_string(),
                ..Default::default()
            })
        );

        // 3. /etc/os-release takes precendence.
        fs::create_dir(tmpdir.path().join("etc")).unwrap();
        fs::write(tmpdir.path().join("etc/os-release"), "ID=etc\n").unwrap();
        assert_eq!(
            OsRelease::scan(tmpdir.path()).unwrap(),
            Some(OsRelease {
                id: "etc".to_string(),
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_find_kernel_and_initramfs_patterns() {
        struct TestCase {
            files: &'static [&'static str],
            kernel: &'static str,
            initramfs: &'static str,
        }

        let cases = [
            TestCase {
                // This is what I see in /boot on Pop OS, which is presumably
                // similar to Ubuntu.
                //
                // This tests version selection and precedence.
                files: &[
                    "vmlinuz-6.16.3-76061603-generic",
                    "initrd.img-6.16.3-76061603-generic",
                    "vmlinuz-6.6.6-76060606-generic.dpkg-bak",
                    "initrd.img-6.6.6-76060606-generic.dpkg-bak",
                    "vmlinuz",
                    "initrd.img",
                    "vmlinuz.old",
                    "initrd.img.old",
                ],
                kernel: "vmlinuz-6.16.3-76061603-generic",
                initramfs: "initrd.img-6.16.3-76061603-generic",
            },
            TestCase {
                // Arch Linux example.
                files: &[
                    "vmlinuz-linux",
                    "initramfs-linux.img",
                    "initramfs-linux-fallback.img",
                ],
                kernel: "vmlinuz-linux",
                initramfs: "initramfs-linux.img",
            },
            TestCase {
                // Tests the 'vmlinux' kernel naming scheme and compression
                // suffixes for the initramfs.
                files: &["vmlinux-6.1.12", "initrd-6.1.12.gz"],
                kernel: "vmlinux-6.1.12",
                initramfs: "initrd-6.1.12.gz",
            },
            TestCase {
                // Tests the 'linux' kernel naming scheme, dual extension
                // suffixes for the initramfs, and the version suffix format for
                // the initramfs.
                files: &["linux-lts", "initrd.img.bz2-lts"],
                kernel: "linux-lts",
                initramfs: "initrd.img.bz2-lts",
            },
            TestCase {
                // Tests the 'linuz' and 'initramfs' naming schemes.
                files: &["linuz-4.19.0", "initramfs-4.19.0.img.xz"],
                kernel: "linuz-4.19.0",
                initramfs: "initramfs-4.19.0.img.xz",
            },
            TestCase {
                // Tests the 'kernel' kernel naming scheme.
                files: &["kernel-5.4.0", "initrd.img-5.4.0"],
                kernel: "kernel-5.4.0",
                initramfs: "initrd.img-5.4.0",
            },
            TestCase {
                // No hyphens.
                files: &["vmlinuz5.15.0", "initramfs-vmlinuz5.15.0"],
                kernel: "vmlinuz5.15.0",
                initramfs: "initramfs-vmlinuz5.15.0",
            },
            TestCase {
                // No version.
                files: &["vmlinuz", "initramfs-vmlinuz.img"],
                kernel: "vmlinuz",
                initramfs: "initramfs-vmlinuz.img",
            },
        ];

        for (i, case) in cases.iter().enumerate() {
            let tmpdir = TempDir::new().unwrap();
            let boot_dir = tmpdir.path().join("boot");
            fs::create_dir_all(&boot_dir).unwrap();
            for file in case.files {
                fs::File::create(boot_dir.join(file)).unwrap();
            }

            let result = KernelPair::scan(tmpdir.path()).unwrap();
            let expected = KernelPair {
                path: boot_dir.join(case.kernel),
                initrd: boot_dir.join(case.initramfs),
            };
            assert_eq!(result, expected, "case {}", i);
        }
    }
}
