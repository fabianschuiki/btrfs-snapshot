// Copyright (c) 2021 Fabian Schuiki
/// A simple tool to create rotating btrfs subvolume snapshots.

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Timelike as _};
use clap::Arg;
use humantime::format_duration;
use indexmap::{IndexMap, IndexSet};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

fn main() -> Result<()> {
    pretty_env_logger::init();

    // Parse the command line arguments.
    let matches = clap::app_from_crate!("\n")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Path to the configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dry-run")
                .short("n")
                .long("dry-run")
                .help("Show btrfs operations without executing"),
        )
        .arg(
            Arg::with_name("only-rotate")
                .short("r")
                .long("rotate")
                .help("Only rotate snapshots"),
        )
        .arg(
            Arg::with_name("only-take")
                .short("t")
                .long("take")
                .help("Only take snapshots"),
        )
        .arg(
            Arg::with_name("only-snapshot")
                .short("s")
                .long("snapshot")
                .value_name("NAME")
                .help("Only operate on specific snapshots")
                .multiple(true)
                .takes_value(true),
        )
        .get_matches();

    // Determine what to do.
    let default = !matches.is_present("only-rotate") && !matches.is_present("only-take");
    let do_rotate = default || matches.is_present("only-rotate");
    let do_take = default || matches.is_present("only-take");

    // Locate and read the configuration file.
    let config_path = matches
        .value_of("config")
        .unwrap_or("/etc/btrfs-snapshot.toml");
    let config = read_config(config_path)
        .with_context(|| format!("Failed to read config from {}", config_path))?;
    trace!("{:#?}", config);

    // Do the work.
    let mut state = State::default();
    state.dry_run = matches.is_present("dry-run");
    for snapshot in config.snapshots.values() {
        if let Some(mut snaps) = matches.values_of("only-snapshot") {
            if snaps.find(|&x| x == snapshot.name).is_none() {
                continue;
            }
        }
        if do_take {
            state.take_snapshot(snapshot)?;
        }
        if do_rotate {
            state.rotate_snapshot(snapshot)?;
        }
    }
    state.unmount()?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    /// The common configuration bits for snapshots.
    #[serde(flatten)]
    generic: SnapshotConfig,
    /// The per-snapshot configuration.
    #[serde(default)]
    snapshots: IndexMap<String, SnapshotConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotConfig {
    /// The name of the snapshot config.
    #[serde(skip)]
    name: String,
    /// The mount point of the btrfs volume.
    mount_point: Option<PathBuf>,
    /// The format to use for snapshot names.
    format: Option<String>,
    /// The subvolume to snapshot.
    subvolume: Option<PathBuf>,
    /// The directory where snapshots are stored.
    snapshot_dir: Option<PathBuf>,
    /// A list of spacing between snapshots for snapshots of a given age.
    spacings: Option<IndexMap<humantime_serde::Serde<Duration>, humantime_serde::Serde<Duration>>>,
}

/// Read a configuration file.
fn read_config(path: &str) -> Result<Config> {
    debug!("Loading config {}", path);
    let mut buf = String::new();
    File::open(path)?.read_to_string(&mut buf)?;
    let mut cfg: Config = toml::de::from_str(&buf)?;
    if cfg.generic.spacings.is_none() {
        cfg.generic.spacings = Some(Default::default());
    }

    // Copy details from the generic config into the snapshots.
    let mut snapshots = std::mem::take(&mut cfg.snapshots);
    for (name, s) in &mut snapshots {
        s.name = name.clone();
        if s.mount_point.is_none() {
            s.mount_point = cfg.generic.mount_point.clone();
        }
        if s.format.is_none() {
            s.format = cfg.generic.format.clone();
        }
        if s.subvolume.is_none() {
            s.subvolume = cfg.generic.subvolume.clone();
        }
        if s.snapshot_dir.is_none() {
            s.snapshot_dir = cfg.generic.snapshot_dir.clone();
        }
        if s.spacings.is_none() {
            s.spacings = cfg.generic.spacings.clone();
        }

        // Check that we have enough information.
        if s.mount_point.is_none() {
            bail!("Snapshot {} has no `mount_point` config", name);
        }
        if s.format.is_none() {
            bail!("Snapshot {} has no `format` config", name);
        }
        if s.subvolume.is_none() {
            bail!("Snapshot {} has no `subvolume` config", name);
        }
        if s.snapshot_dir.is_none() {
            bail!("Snapshot {} has no `snapshot_dir` config", name);
        }
    }
    cfg.snapshots = snapshots;

    Ok(cfg)
}

#[derive(Default)]
struct State<'a> {
    /// Whether to only print btrfs commands rather than executing them.
    dry_run: bool,
    /// The disks mounted explicitly by us.
    manual_mounts: IndexSet<&'a Path>,
}

impl<'a> State<'a> {
    fn take_snapshot(&mut self, snapshot: &'a SnapshotConfig) -> Result<()> {
        debug!("Take snapshot of {}", snapshot.name);
        self.mount_if_needed(snapshot.mount_point.as_ref().unwrap())?;

        // Construct the snapshot directory.
        let format = snapshot.format.as_ref().unwrap();
        let mut path = snapshot.snapshot_dir.clone().unwrap();
        path.push(chrono::Local::now().format(format).to_string());
        println!("Taking snapshot {}", path.display());

        // Take the snapshot.
        self.maybe_run(
            Command::new("btrfs")
                .arg("subvolume")
                .arg("snapshot")
                .arg("-r")
                .arg(snapshot.subvolume.as_ref().unwrap())
                .arg(&path),
        )
        .with_context(|| format!("Taking snapshot {} failed", path.display()))?;

        Ok(())
    }

    fn rotate_snapshot(&mut self, snapshot: &'a SnapshotConfig) -> Result<()> {
        debug!("Rotate snapshots for {}", snapshot.name);
        self.mount_if_needed(snapshot.mount_point.as_ref().unwrap())?;

        // Create an array of snapshot spacings.
        let mut spacings: Vec<_> = snapshot
            .spacings
            .as_ref()
            .unwrap()
            .iter()
            .map(|(age, spacing)| (age.into_inner(), spacing.into_inner()))
            .collect();
        spacings.sort_by_key(|&(age, _)| age);
        trace!("Spacings: {:?}", spacings);

        // Parse the snapshots into proper dates.
        let now = chrono::Local::now().with_nanosecond(0).unwrap();
        let format = snapshot.format.as_ref().unwrap();
        let mut entries = Vec::new();
        for file in std::fs::read_dir(snapshot.snapshot_dir.as_ref().unwrap())? {
            let file = file?.path();
            let name = match file.file_name().and_then(|x| x.to_str()) {
                Some(x) => x,
                None => continue,
            };
            let date = match DateTime::parse_from_str(name, format) {
                Ok(x) => x,
                Err(_) => {
                    warn!(
                        "Ignoring snapshot {} because name does not match format `{}`",
                        file.display(),
                        format
                    );
                    continue;
                }
            };
            let age = now.signed_duration_since(date).to_std()?;
            let rule = spacings
                .iter()
                .enumerate()
                .filter(|(_, &(a, _))| a <= age)
                .max_by_key(|(_, &(a, _))| a)
                .map(|(i, _)| i);
            entries.push((date, file, rule));
        }

        // Sort the entries by descending date.
        entries.sort_by_key(|&(d, ..)| d);
        entries.reverse();

        // Iterate through the entries newest to oldest and mark the ones that
        // are too close to the previous entry.
        let mut delete = IndexSet::new();
        for (rule, &(target_age, target_spacing)) in spacings.iter().enumerate() {
            trace!(
                "Purging for rule {}, until age {}, spacing {}",
                rule,
                format_duration(target_age),
                format_duration(target_spacing)
            );
            let mut it = entries.iter().zip(entries.iter().skip(1));
            let mut newest = match it.next() {
                Some((x, _)) => x,
                None => return Ok(()),
            };
            trace!("  Initial {}", newest.0);
            for (current, older) in it {
                if current.2 > Some(rule) {
                    break;
                }
                let applies = current.2 == Some(rule);
                let spacing = std::cmp::max(
                    (newest.0).signed_duration_since(current.0).to_std()?,
                    (current.0).signed_duration_since(older.0).to_std()?,
                );
                trace!(
                    "  {} {}, rule {:?}, spacing {}",
                    if applies { "Considering" } else { "Skipping" },
                    current.0,
                    current.2,
                    format_duration(spacing)
                );

                // Drop the snapshot if not adequately spaced.
                if spacing < target_spacing {
                    if current.2 == Some(rule) {
                        delete.insert(&current.1);
                        debug!("  Dropping {}", current.0);
                        debug!("    Favoring: {}", newest.0);
                        debug!("    Spacing:  {}", format_duration(spacing));
                        debug!("    Intended: {}", format_duration(target_spacing));
                    }
                } else {
                    newest = current;
                }
            }
        }

        // Delete the marked snapshots.
        for file in delete {
            println!("Dropping snapshot {}", file.display());
            self.maybe_run(
                Command::new("btrfs")
                    .arg("subvolume")
                    .arg("delete")
                    .arg(file),
            )
            .with_context(|| format!("Deleting snapshot {} failed", file.display()))?;
        }

        Ok(())
    }

    /// Mount a disk if it is not yet mounted.
    fn mount_if_needed(&mut self, mount_point: &'a Path) -> Result<()> {
        // No need to mount twice.
        if self.manual_mounts.contains(mount_point) {
            return Ok(());
        }

        // Check if the disk is not already mounted.
        let re = Regex::new(r"(?m)^.+? on (.+?) type").unwrap();
        let mounts = run(&mut Command::new("mount")).context("Checking mounts failed")?;
        for cap in re.captures_iter(&mounts) {
            if Path::new(&cap[1]) == mount_point {
                trace!("Already mounted {}", &cap[1]);
                return Ok(());
            }
        }

        // Actually mount the disk.
        debug!("Mounting {}", mount_point.display());
        run(&mut Command::new("mount").arg(mount_point))
            .with_context(|| format!("Mounting {} failed", mount_point.display()))?;
        self.manual_mounts.insert(mount_point);
        Ok(())
    }

    /// Unmount all the manually mounted disks.
    fn unmount(&mut self) -> Result<()> {
        for mount_point in std::mem::take(&mut self.manual_mounts) {
            debug!("Unmounting {}", mount_point.display());
            run(&mut Command::new("umount").arg(mount_point))
                .with_context(|| format!("Unmounting {} failed", mount_point.display()))?;
        }
        Ok(())
    }

    fn maybe_run(&self, cmd: &mut Command) -> Result<String> {
        if self.dry_run {
            println!("{:?}", cmd);
            Ok(String::new())
        } else {
            run(cmd)
        }
    }
}

/// Execute a `Command` and return its stdout on exit code 0, or a flurry of
/// appropriate error messages if anything goes wrong.
fn run(cmd: &mut Command) -> Result<String> {
    let output = cmd
        .output()
        .with_context(|| format!("Failed to execute {:?}", cmd))?;
    if !output.status.success() {
        let code = output.status.code().unwrap_or(0);
        return Err(anyhow!(std::str::from_utf8(&output.stderr)
            .unwrap_or("<stderr not utf-8>")
            .trim()
            .to_owned()))
        .with_context(|| format!("Command {:?} failed with exit code {}", cmd, code));
    }
    String::from_utf8(output.stdout)
        .with_context(|| format!("Command {:?} stdout is non-utf8", cmd))
}
