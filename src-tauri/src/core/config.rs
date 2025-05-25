use std::{collections::HashMap, sync::Mutex};

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap_servers: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    clusters: HashMap<String, ClusterConfig>,
    default_cluster: String,
}
impl Config {
    pub fn clusters(&self) -> Vec<ClusterConfig> {
        self.clusters.to_owned().into_values().collect()
    }
    pub fn load() -> Self {
        let clusters = HashMap::from([
            (
                "docker".to_owned(),
                ClusterConfig {
                    name: "docker".to_owned(),
                    bootstrap_servers: vec![
                        "localhost:29092".to_owned(),
                        "localhost:39092".to_owned(),
                        "localhost:49092".to_owned(),
                    ],
                },
            ),
            (
                "dev".to_owned(),
                ClusterConfig {
                    name: "dev".to_owned(),
                    bootstrap_servers: vec!["192.168.225.10:9092".to_owned()],
                },
            ),
            (
                "prod".to_owned(),
                ClusterConfig {
                    name: "prod".to_owned(),
                    bootstrap_servers: vec!["10.10.18.89:9093".to_owned()],
                },
            ),
            (
                "local".to_owned(),
                ClusterConfig {
                    name: "local".to_owned(),
                    bootstrap_servers: vec!["localhost:9092".to_owned()],
                },
            ),
        ]);

        Self {
            clusters,
            default_cluster: "docker".to_owned(),
        }
    }

    pub fn set_default_cluster(&mut self, cluster: &str) -> Result<ClusterConfig, String> {
        if !self.clusters.contains_key(cluster) {
            return Err(format!(
                "Cluster key '{cluster}' does not exist in the config"
            ));
        }

        self.default_cluster = cluster.to_owned();

        Ok(self.clusters.get(cluster).unwrap().to_owned())
    }
    pub fn default_cluster_config(&self) -> ClusterConfig {
        let default_cluster = ClusterConfig {
            name: "default".to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
        };

        let cluster_config = self
            .clusters
            .get(&self.default_cluster)
            .unwrap_or(&default_cluster);

        cluster_config.to_owned()
    }
}

pub struct ApplicationState {
    pub config: Mutex<Config>,
    pub active_consumers: Mutex<HashMap<String, tokio::sync::mpsc::Sender<()>>>,
}

impl ApplicationState {
    pub fn load() -> Self {
        ApplicationState {
            config: Mutex::new(Config::load()),
            active_consumers: Mutex::new(HashMap::new()),
        }
    }
}
