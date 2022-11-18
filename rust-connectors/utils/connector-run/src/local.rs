use std::process::Command;

use clap::Parser;
use fluvio_connectors_common::config::ConnectorConfig;

use crate::{build_args, build_envs};

#[derive(Debug, Parser)]
pub struct LocalOpt {
    /// path to the connector config
    #[clap(short = 'c', long)]
    config: String,

    #[clap(short = 'a', long)]
    /// arguments passed to Docker container, example: --docker-arg=--rm --docker-arg=--network --docker-arg=bridge
    docker_arg: Vec<String>,
}

impl LocalOpt {
    pub fn execute(self) -> anyhow::Result<()> {
        let config = ConnectorConfig::from_file(self.config)?;

        let args = build_args(&config)?;
        let type_ = &config.type_;
        let image = format!("infinyon/fluvio-connect-{}:{}", type_, config.version);

        let home = std::env::var("HOME").expect("no home found");

        let mut fluvio_config_path = std::path::PathBuf::from(home);
        fluvio_config_path.push(".fluvio");
        fluvio_config_path.push("config");
        let fluvio_config_mount = format!(
            "type=bind,source={},target=/home/fluvio/.fluvio/config,readonly",
            fluvio_config_path.as_os_str().to_string_lossy()
        );

        let mut env_args: Vec<String> = vec![];
        for (key, value) in &build_envs(&config)? {
            env_args.push(format!("{}={}", key, value));
        }

        let mut binding = Command::new("docker");
        let mut command = binding
            .arg("run")
            .args(self.docker_arg.as_slice())
            .arg("--rm")
            .arg("-i")
            .arg("--mount")
            .arg(fluvio_config_mount);
        for arg in &env_args {
            command = command.arg("--env").arg(arg);
        }
        command = command.arg(image).args(args);
        println!("Spawning with command: {command:?}");
        let mut child = command.spawn()?;

        child.wait().expect("failed while waiting for child");

        Ok(())
    }
}
