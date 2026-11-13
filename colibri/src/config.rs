//! Settings: one table says what there is, the config file and the command
//! line fill it, and a flag overrides the file.

use std::{collections::HashMap, net::IpAddr, str::FromStr};

type Error = Box<dyn std::error::Error>;

const DEFAULT_PATH: &str = "colibri.json";

/// Every setting: its key in the config file, its flag, what the flag takes,
/// the value when neither names it, and what it is. A setting is added here
/// and read in [`Config::from_args`], nowhere else.
const SETTINGS: &[(&str, &str, &str, Option<&str>, &str)] = &[
    ("ip",            "--ip",            "IP",        None,           "Public IP to advertise in gossip (required unless --stun is given)"),
    ("port",          "--port",          "PORT",      Some("8000"),   "Gossip UDP port"),
    ("tvu_port",      "--tvu-port",      "PORT",      Some("8200"),   "TVU port where shreds arrive"),
    ("repair_port",   "--repair-port",   "PORT",      Some("8210"),   "UDP port for repair responses"),
    ("stun",          "--stun",          "HOST:PORT", None,           "STUN server; advertise each socket's real external NAT mapping instead of --ip:port. \
                                                                       Needed behind a port-rewriting NAT; omit on a host with a real public IP"),
    ("entrypoints",   "--entrypoint",    "ADDR",      None,           "Solana entrypoint (repeatable; an array in the file). Flags replace the file's list. \
                                                                       Default: the five mainnet-beta entrypoints"),
    ("shred_version", "--shred-version", "VER",       None,           "Shred version (default: fetched from an entrypoint)"),
    ("rpc",           "--rpc",           "URL",       Some("http://api.mainnet-beta.solana.com"), "RPC endpoint for stake and leader-schedule data"),
    ("tier1_fanout",  "--tier1-fanout",  "N",         Some("200"),    "Stake table size for repair peer scoring"),
    ("grpc_port",     "--grpc-port",     "PORT",      Some("8888"),   "gRPC listen port"),
    ("auth_token",    "--auth-token",    "TOKEN",     None,           "Bearer token required of gRPC subscribers"),
    ("tls_cert",      "--tls-cert",      "PATH",      None,           "TLS certificate PEM (TLS is on when --tls-key is given too)"),
    ("tls_key",       "--tls-key",       "PATH",      None,           "TLS private key PEM"),
    ("keypair",       "--keypair",       "PATH",      None,           "Keypair JSON file, loaded or created, for a gossip identity that survives restarts"),
    ("depth",         "--depth",         "N",         Some("500"),    "Backfill depth from the tip when no consumer reports from-slot"),
    ("window",        "--window",        "N",         Some("64"),     "Slots repaired in parallel"),
    ("top_peers",     "--top-peers",     "N",         Some("128"),    "Distinct peers repair requests are spread across"),
    ("repair_rate",   "--repair-rate",   "N",         Some("0"),      "Most repair requests sent per second, 0 = no limit"),
    ("oracle_rpc",    "--oracle-rpc",    "URL",       None,           "Enable the RPC oracle cross-check (samples completed slots via getBlock)"),
    ("stall_secs",    "--stall-secs",    "N",         Some("60"),     "Exit after N s without one shred, for the supervisor to restart it; 0 = never"),
];

/// Used when neither the config file nor the command line names any. One
/// dead entrypoint stalls gossip, so the default is all five, not one.
const MAINNET_ENTRYPOINTS: [&str; 5] = [
    "entrypoint.mainnet-beta.solana.com:8001",
    "entrypoint2.mainnet-beta.solana.com:8001",
    "entrypoint3.mainnet-beta.solana.com:8001",
    "entrypoint4.mainnet-beta.solana.com:8001",
    "entrypoint5.mainnet-beta.solana.com:8001",
];

pub struct Config {
    pub ip:            IpAddr,
    pub port:          u16,
    pub tvu_port:      u16,
    pub repair_port:   u16,
    pub stun_server:   Option<String>,
    pub entrypoints:   Vec<String>,
    pub shred_version: Option<u16>,
    pub rpc_url:       String,
    pub tier1_fanout:  usize,
    pub grpc_port:     u16,
    pub auth_token:    Option<String>,
    pub tls_cert:      Option<String>,
    pub tls_key:       Option<String>,
    pub keypair_path:  Option<String>,
    pub depth:         u64,
    pub window:        usize,
    pub top_peers:     usize,
    pub repair_rate:   u64,
    pub oracle_rpc:    Option<String>,
    /// Seconds without one shred after which the node exits, for its
    /// supervisor to start it again; 0 never.
    pub stall_secs:    u64,
}

pub fn usage() -> String {
    let mut text = format!("Usage: colibri [OPTIONS]\n\nOptions:\n  {:<26}JSON config; keys are the flags below without dashes, underscores for hyphens,\n  {:<26}`entrypoints` as an array. ./{DEFAULT_PATH} is loaded if present. Flags override the file.\n", "--config <PATH>", "");
    for (_, flag, takes, default, what) in SETTINGS {
        let what = what.split_whitespace().collect::<Vec<_>>().join(" ");
        let default = default.map(|d| format!(" (default: {d})")).unwrap_or_default();
        text += &format!("  {:<26}{what}{default}\n", format!("{flag} <{takes}>"));
    }
    text + &format!("  {:<26}Print this help\n", "--help")
}

/// What the file and the flags said, as text, by key.
struct Said(HashMap<&'static str, Vec<String>>);

impl Said {
    /// The setting as `T`: what was said last, else the table's default.
    fn get<T: FromStr>(&self, key: &'static str) -> Result<Option<T>, Error> {
        let default = SETTINGS.iter().find(|s| s.0 == key).unwrap_or_else(|| panic!("{key} is not a setting")).3;
        let Some(text) = self.0.get(key).and_then(|v| v.last()).map(String::as_str).or(default) else { return Ok(None) };
        text.parse().map(Some).map_err(|_| format!("{key}: {text:?} is not a valid value").into())
    }

    /// A setting that has a default.
    fn need<T: FromStr>(&self, key: &'static str) -> Result<T, Error> {
        Ok(self.get(key)?.unwrap_or_else(|| panic!("{key} has no default")))
    }
}

/// The settings of a config file: strings, integers, and arrays of strings.
fn read_file(path: &str) -> Result<HashMap<&'static str, Vec<String>>, Error> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("config {path}: {e}"))?;
    let json: serde_json::Value = serde_json::from_str(&text).map_err(|e| format!("config {path}: {e}"))?;
    let object = json.as_object().ok_or_else(|| format!("config {path}: top level must be an object"))?;
    let scalar = |key: &str, v: &serde_json::Value| match v {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) if n.is_u64() => Ok(n.to_string()),
        _ => Err(format!("config {path}: `{key}` must be a string or a whole number")),
    };
    let mut said = HashMap::new();
    for (key, value) in object {
        let Some(setting) = SETTINGS.iter().find(|s| s.0 == key) else {
            let known: Vec<_> = SETTINGS.iter().map(|s| s.0).collect();
            return Err(format!("config {path}: unknown key `{key}` (known: {})", known.join(", ")).into());
        };
        let values = match value.as_array() {
            Some(items) => items.iter().map(|v| scalar(key, v)).collect::<Result<Vec<_>, _>>()?,
            None => vec![scalar(key, value)?],
        };
        said.insert(setting.0, values);
    }
    Ok(said)
}

impl Config {
    /// From `args` (the program's name first) and the config file they name,
    /// or `./colibri.json` when there is one. `Ok(None)`: help was asked for.
    pub fn from_args(args: &[String]) -> Result<Option<Config>, Error> {
        let mut flags: HashMap<&'static str, Vec<String>> = HashMap::new();
        let mut path = None;
        let mut rest = args.iter().skip(1);
        while let Some(arg) = rest.next() {
            if arg == "--help" || arg == "-h" { return Ok(None); }
            let mut value = || rest.next().cloned().ok_or_else(|| format!("{arg} needs a value"));
            if arg == "--config" { path = Some(value()?); continue; }
            let setting = SETTINGS.iter().find(|s| s.1 == arg).ok_or_else(|| format!("unknown argument: {arg}"))?;
            flags.entry(setting.0).or_default().push(value()?);
        }
        let path = path.or_else(|| std::path::Path::new(DEFAULT_PATH).exists().then(|| DEFAULT_PATH.to_string()));
        let mut said = match &path {
            Some(path) => { eprintln!("[colibri] config:        {path}"); read_file(path)? }
            None => HashMap::new(),
        };
        // A flag replaces what the file said of its key, a list included.
        said.extend(flags);
        let said = Said(said);

        let stun_server = said.get::<String>("stun")?;
        Ok(Some(Config {
            // STUN finds the advertised IP from the socket's own mapping, so
            // --ip is required only where there is no STUN server to ask.
            ip: match said.get("ip")? {
                Some(ip) => ip,
                None if stun_server.is_some() => IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                None => return Err("--ip <PUBLIC_IP> is required (or pass --stun)".into()),
            },
            port:          said.need("port")?,
            tvu_port:      said.need("tvu_port")?,
            repair_port:   said.need("repair_port")?,
            stun_server,
            entrypoints:   said.0.get("entrypoints").cloned().filter(|e| !e.is_empty())
                .unwrap_or_else(|| MAINNET_ENTRYPOINTS.iter().map(|s| s.to_string()).collect()),
            shred_version: said.get("shred_version")?,
            rpc_url:       said.need("rpc")?,
            tier1_fanout:  said.need("tier1_fanout")?,
            grpc_port:     said.need("grpc_port")?,
            auth_token:    said.get("auth_token")?,
            tls_cert:      said.get("tls_cert")?,
            tls_key:       said.get("tls_key")?,
            keypair_path:  said.get("keypair")?,
            depth:         said.need("depth")?,
            window:        said.need("window")?,
            top_peers:     said.need("top_peers")?,
            repair_rate:   said.need("repair_rate")?,
            oracle_rpc:    said.get("oracle_rpc")?,
            stall_secs:    said.need("stall_secs")?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(file: Option<&str>, flags: &[&str]) -> Result<Option<Config>, Error> {
        let mut args = vec!["colibri".to_string()];
        if let Some(text) = file {
            let path = std::env::temp_dir().join(format!("colibri-config-test-{}-{:p}.json", std::process::id(), text));
            std::fs::write(&path, text).unwrap();
            args.extend(["--config".to_string(), path.to_string_lossy().into_owned()]);
        }
        args.extend(flags.iter().map(|s| s.to_string()));
        Config::from_args(&args)
    }

    #[test]
    fn defaults_then_the_file_then_the_flags() {
        let c = config(None, &["--stun", "stun.example:3478"]).unwrap().unwrap();
        assert_eq!((c.port, c.grpc_port, c.depth, c.window, c.stall_secs, c.entrypoints.len()), (8000, 8888, 500, 64, 60, 5));
        assert!(c.ip.is_unspecified() && c.auth_token.is_none() && c.shred_version.is_none());

        let file = r#"{"ip": "203.0.113.10", "port": 9000, "depth": 20, "auth_token": "t", "entrypoints": ["a:1", "b:2"], "shred_version": 50093}"#;
        let c = config(Some(file), &[]).unwrap().unwrap();
        assert_eq!((c.ip.to_string().as_str(), c.port, c.depth, c.auth_token.as_deref(), c.shred_version), ("203.0.113.10", 9000, 20, Some("t"), Some(50093)));
        assert_eq!(c.entrypoints, ["a:1", "b:2"]);

        // A flag wins over the file; entrypoint flags replace the file's list.
        let c = config(Some(file), &["--port", "9100", "--entrypoint", "c:3"]).unwrap().unwrap();
        assert_eq!((c.port, c.depth), (9100, 20));
        assert_eq!(c.entrypoints, ["c:3"]);
    }

    #[test]
    fn what_is_wrong_is_said() {
        let said = |r: Result<Option<Config>, Error>| r.err().expect("an error").to_string();
        assert!(said(config(None, &[])).contains("--ip"));
        assert!(said(config(None, &["--nope", "1"])).contains("unknown argument: --nope"));
        assert!(said(config(None, &["--ip", "1.2.3.4", "--port"])).contains("--port needs a value"));
        assert!(said(config(None, &["--ip", "1.2.3.4", "--port", "70000"])).contains("port"));
        assert!(said(config(Some(r#"{"prot": 1}"#), &[])).contains("unknown key `prot`"));
        assert!(said(config(Some(r#"{"port": true}"#), &[])).contains("`port` must be"));
        assert!(config(None, &["--help"]).unwrap().is_none());
    }

    #[test]
    fn every_setting_is_in_the_help_and_has_a_readable_default() {
        let help = usage();
        for (key, flag, _, default, _) in SETTINGS {
            assert!(help.contains(flag), "{flag}");
            assert_eq!(*flag, format!("--{}", key.replace('_', "-").trim_end_matches(if *key == "entrypoints" { "s" } else { "\0" })));
            if let Some(d) = default { assert!(help.contains(&format!("(default: {d})"))); }
        }
    }
}
