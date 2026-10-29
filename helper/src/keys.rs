//! Keys as OpenSSH writes them: the public line of one of this machine's own,
//! and the key in a line the deployment hands over.

use anyhow::Result;
use russh::keys::{PrivateKey, PublicKey};

/// The OpenSSH line of a key's public half.
pub fn public_line(key: &PrivateKey) -> Result<String> {
    Ok(key.public_key().to_openssh()?)
}

/// The key in an `authorized_keys` or `known_hosts` line. Options and host
/// patterns before it are skipped, not applied: the helper allows one thing,
/// a command or SFTP as its own account, whatever a line says.
pub fn key_in_line(line: &str) -> Option<PublicKey> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let words: Vec<&str> = line.split_whitespace().collect();
    (0..words.len()).find_map(|i| PublicKey::from_openssh(&words[i..].join(" ")).ok())
}
