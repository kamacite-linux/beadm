// SPDX-License-Identifier: MPL-2.0

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::fs;
use std::process::Command;

use crate::{Error, be::scan};

/// Check if `kexec(8)` can be used to load a kernel and initramfs pair.
pub fn has_kexec() -> Result<(), Error> {
    if let Err(e) = Command::new("kexec").arg("--version").output() {
        return Err(Error::KexecNotAvailable(e));
    }

    // Check if kexec has been disabled at the kernel level.
    if let Ok(contents) = fs::read_to_string("/proc/sys/kernel/kexec_load_disabled") {
        if contents.trim() != "0" {
            return Err(Error::KexecNotAvailable(std::io::Error::other(
                "kexec syscalls disabled for this kernel",
            )));
        }
    }

    Ok(())
}

/// Load a kernel and initramfs into kexec.
pub fn kexec_load(kernel: &scan::KernelPair, cmdline: &str) -> Result<(), Error> {
    // TODO: Device tree support.
    let mut cmd = Command::new("kexec");
    cmd.arg("-l")
        .arg(kernel.path.as_os_str())
        .arg("--initrd")
        .arg(kernel.initrd.as_os_str())
        .arg("--command-line")
        .arg(cmdline);

    let output = cmd.output().map_err(|e| Error::KexecFailed(e))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::KexecFailed(std::io::Error::other(stderr.trim())));
    }

    Ok(())
}
