use std::ffi::{CStr, OsString};
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;

use anyhow::{Result, bail};

/// The account the helper runs as, the only one it lets in.
#[derive(Clone)]
pub struct Account {
    pub name: String,
    pub home: PathBuf,
    pub shell: PathBuf,
}

impl Account {
    pub fn current() -> Result<Self> {
        let uid = unsafe { libc::getuid() };
        let mut entry: libc::passwd = unsafe { std::mem::zeroed() };
        let mut found: *mut libc::passwd = std::ptr::null_mut();
        let mut buf = vec![0u8; 16 * 1024];
        let rc = unsafe {
            libc::getpwuid_r(
                uid,
                &mut entry,
                buf.as_mut_ptr().cast(),
                buf.len(),
                &mut found,
            )
        };
        if rc != 0 || found.is_null() {
            bail!("uid {uid} has no account entry");
        }
        let field = |p: *const libc::c_char| unsafe { CStr::from_ptr(p) }.to_bytes().to_vec();
        let shell = field(entry.pw_shell);
        Ok(Self {
            name: String::from_utf8(field(entry.pw_name))?,
            home: PathBuf::from(OsString::from_vec(field(entry.pw_dir))),
            // sshd's fallback for an account with no shell set.
            shell: if shell.is_empty() {
                PathBuf::from("/bin/sh")
            } else {
                PathBuf::from(OsString::from_vec(shell))
            },
        })
    }
}
