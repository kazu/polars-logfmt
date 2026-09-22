use anyhow::{Result, anyhow};
use ssh2::{Channel, Session};
use std::net::TcpStream;
use std::path::Path;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshSource {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub line_filter: Option<String>,
}

impl SshSource {
    pub fn new(source: &str) -> Self {
        parse_ssh_source(source).expect("invalid ssh source")
    }

    pub fn try_new(source: &str) -> Result<Self> {
        parse_ssh_source(source)
    }

    pub fn contain_line(mut self, needle: impl Into<String>) -> Self {
        self.line_filter = Some(needle.into());
        self
    }

    pub fn line_filter(&self) -> Option<&str> {
        self.line_filter.as_deref()
    }
}

pub struct SshStream {
    pub _sess: Session,
    pub channel: Option<Channel>,
}

pub fn parse_ssh_source(source: &str) -> Result<SshSource> {
    let url = Url::parse(source).map_err(|e| anyhow!("invalid source url: {e}"))?;
    if url.scheme() != "ssh" {
        return Err(anyhow!("source scheme must be ssh"));
    }
    let user = url.username();
    if user.is_empty() {
        return Err(anyhow!("source must include user"));
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("source must include host"))?;
    let port = url.port().unwrap_or(22);
    let path = url.path();
    if path == "/" || path.is_empty() {
        return Err(anyhow!("source must include path"));
    }

    Ok(SshSource {
        user: user.to_string(),
        host: host.to_string(),
        port,
        path: path.to_string(),
        line_filter: None,
    })
}

pub fn connect_ssh(
    source: &SshSource,
    key_path: Option<&str>,
    password: Option<&str>,
    cmd: &str,
) -> Result<SshStream> {
    let tcp = TcpStream::connect((source.host.as_str(), source.port))?;
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    if let Some(key_path) = key_path {
        let key_path = Path::new(key_path);
        sess.userauth_pubkey_file(&source.user, None, key_path, password)?;
    } else if let Some(password) = password {
        sess.userauth_password(&source.user, password)?;
    } else {
        sess.userauth_agent(&source.user)?;
    }

    if !sess.authenticated() {
        return Err(anyhow!("ssh authentication failed"));
    }

    let mut channel = sess.channel_session()?;
    channel.exec(cmd)?;

    Ok(SshStream {
        _sess: sess,
        channel: Some(channel),
    })
}
