mod apply;
mod delete;
mod local;
mod print;

use anyhow::Context;
use itertools::Itertools;
use std::collections::HashMap;

use apply::ApplyOpt;
use clap::Parser;
use delete::DeleteOpt;
use fluvio_connectors_common::config::{ConnectorConfig, ManagedConnectorParameterValue};
use k8_types::{
    app::deployment::DeploymentSpec,
    core::pod::{
        ConfigMapVolumeSource, ContainerSpec, ImagePullPolicy, KeyToPath, PodSecurityContext,
        PodSpec, VolumeMount, VolumeSpec,
    },
    Env, LabelProvider, LabelSelector, TemplateMeta, TemplateSpec,
};
use local::LocalOpt;
use print::PrintOpt;

const DEFAULT_CONNECTOR_NAME: &str = "fluvio-connector";

#[tokio::main]
async fn main() {
    let config = RunOpt::from_args();
    config.execute().await.expect("failed to execute");
}

#[derive(Debug, Parser)]
pub enum RunOpt {
    /// Apply k8 deployment in current namespace
    Apply(ApplyOpt),
    /// Delete k8 deployment in current namespace
    Delete(DeleteOpt),
    /// Print k8 deployment
    Print(PrintOpt),
    /// Run connector locally with docker
    Local(LocalOpt),
}

impl RunOpt {
    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::Apply(apply) => apply.execute().await?,
            Self::Delete(delete) => delete.execute().await?,
            Self::Local(local) => local.execute()?,
            Self::Print(print) => print.execute()?,
        };

        Ok(())
    }
}

fn convert_to_k8_deployment(config: &ConnectorConfig) -> anyhow::Result<DeploymentSpec> {
    // Volume:
    // - configMap:
    //     defaultMode: 420
    //     items:
    //     - key: fluvioClientConfig
    //       path: config
    //     name: fluvio-config-map
    //   name: fluvio-config-volume
    let config_map_volume_spec = VolumeSpec {
        name: "fluvio-config-volume".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: Some("fluvio-config-map".to_string()),
            items: Some(vec![KeyToPath {
                key: "fluvioClientConfig".to_string(),
                path: "config".to_string(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let args = build_args(config)?;

    let type_ = &config.type_;
    let image = format!("infinyon/fluvio-connect-{}:{}", type_, config.version);

    let volume_mounts = vec![VolumeMount {
        name: "fluvio-config-volume".to_string(),
        mount_path: "/home/fluvio/.fluvio".to_string(),
        ..Default::default()
    }];

    let volumes = vec![config_map_volume_spec];

    let secrets = &config.secrets;
    let env: Vec<Env> = secrets
        .keys()
        .zip(secrets.values())
        .flat_map(|(key, value)| [Env::key_value(key, &(**value).to_string())])
        .collect::<Vec<_>>();

    let template = TemplateSpec {
        metadata: Some(TemplateMeta::default().set_labels(vec![
            ("app", DEFAULT_CONNECTOR_NAME),
            ("connectorName", &config.name),
        ])),
        spec: PodSpec {
            termination_grace_period_seconds: Some(10),
            security_context: Some(PodSecurityContext {
                fs_group: Some(1000),
                ..Default::default()
            }),
            containers: vec![ContainerSpec {
                name: DEFAULT_CONNECTOR_NAME.to_owned(),
                image: Some(image),
                image_pull_policy: Some(ImagePullPolicy::Never),
                env,
                volume_mounts,
                args,
                ..Default::default()
            }],
            volumes,
            ..Default::default()
        },
    };

    let mut match_labels = HashMap::new();
    match_labels.insert("app".to_owned(), DEFAULT_CONNECTOR_NAME.to_owned());
    match_labels.insert("connectorName".to_owned(), config.name.clone());

    Ok(DeploymentSpec {
        template,
        selector: LabelSelector { match_labels },
        ..Default::default()
    })
}

fn build_args(config: &ConnectorConfig) -> anyhow::Result<Vec<String>> {
    let parameters = &config.parameters;

    let parameters: Vec<String> = parameters
        .keys()
        .zip(parameters.values())
        .flat_map(|(key, values)| match &values {
            ManagedConnectorParameterValue::String(value) => {
                vec![format!("--{}={}", key.replace('_', "-"), value)]
            }
            ManagedConnectorParameterValue::Vec(values) => {
                let mut args = Vec::new();
                for value in values.iter() {
                    args.push(format!("--{}={}", key.replace('_', "-"), value))
                }
                args
            }
            ManagedConnectorParameterValue::Map(map) => {
                let mut args = Vec::new();
                for (sub_key, value) in map.iter() {
                    args.push(format!(
                        "--{}={}:{}",
                        key.replace('_', "-"),
                        sub_key.replace('_', "-"),
                        value
                    ));
                }
                args
            }
        })
        .collect::<Vec<_>>();

    // Prefixing the args with a "--" passed to the container is needed for an unclear reason.
    let mut args = vec!["--".to_string(), format!("--fluvio-topic={}", config.topic)];
    args.extend(parameters);

    if let Some(ref transform_params) = config.transforms {
        let transforms: Result<Vec<String>, serde_json::Error> = transform_params
            .iter()
            .map(serde_json::to_string)
            .map_ok(|transform| format!("--transform={}", transform))
            .collect();
        args.extend(transforms.context("unable serialize TransformParameters into arguments")?);
    }

    Ok(args)
}
