/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * documentdb_gateway_core/src/configuration/pg_configuration.rs
 *
 *-------------------------------------------------------------------------
 */

use std::{collections::HashMap, path::Path, sync::Arc, time::SystemTime};

use arc_swap::ArcSwap;
use bson::{rawbson, RawBson};
use serde::Deserialize;
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant},
};

use crate::{
    configuration::{
        dynamic::{parse_cluster_version, ClusterVersion, POSTGRES_RECOVERY_KEY},
        DynamicConfiguration, SetupConfiguration,
    },
    error::{DocumentDBError, Result},
    postgres::{conn_mgmt::PoolManager, PgDocument},
};

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct HostConfig {
    #[serde(default)]
    is_primary: String,
    #[serde(default)]
    send_shutdown_responses: String,
}

/// Inner struct that holds the dependencies needed for loading configurations.
#[derive(Debug, Clone)]
struct PgConfigurationInner {
    dynamic_config_path: String,
    settings_prefixes: Vec<String>,
    pool_manager: Arc<PoolManager>,
    instance_kind: String,
    enable_pg_file_settings_refresh: bool,
}

impl PgConfigurationInner {
    /// Loads configurations from the database and config file using the provided connection.
    async fn load_configurations(&self) -> Result<HashMap<String, String>> {
        let mut configs = HashMap::new();

        match Self::load_host_config(&self.dynamic_config_path).await {
            Ok(host_config) => {
                configs.insert(
                    "IsPrimary".to_owned(),
                    host_config.is_primary.to_lowercase(),
                );
                configs.insert(
                    "SendShutdownResponses".to_owned(),
                    host_config.send_shutdown_responses.to_lowercase(),
                );
            }
            Err(e) => tracing::warn!("Host Config file not able to be loaded: {e}"),
        }

        // pg_settings is the source of the dynamic GUC map (max_connections and
        // the rest). If it cannot be read, DO NOT continue with a partial map:
        // returning it would let refresh_configuration() swap in a config missing
        // max_connections, silently flip it to the 25 default, change the per-user
        // pool-cache key, and then hard-fail every request with "Connection pool
        // missing for user". Propagate the error instead so refresh_configuration()
        // skips the swap and keeps the last-known-good full configuration.
        let conn = self
            .pool_manager
            .system_requests_connection()
            .await
            .map_err(|e| {
                tracing::warn!(
                    "Failed to get connection for pg_settings; keeping last-known-good configuration: {e}"
                );
                e
            })?;
        let pg_config_rows = conn
            .query(self.pool_manager.query_catalog().pg_settings(), &[], &[])
            .await
            .map_err(|e| {
                tracing::warn!(
                    "Failed to query pg_settings; keeping last-known-good configuration: {e}"
                );
                DocumentDBError::from(e)
            })?;
        if pg_config_rows.is_empty() {
            return Err(DocumentDBError::internal_error(
                "pg_settings returned no rows; keeping last-known-good configuration".to_owned(),
            ));
        }

        // Fetch most up-to-date switch-related values from pg_file_settings for settings that are set there. pg_settings may have stale
        // values if pg_reload_conf() failed or if 030-user-supplied-server-parameters.conf was updated after the gateway started. Then
        // upsert them into the HashSet. This ensures that we have the most accurate settings without relying on pg_reload_conf() succeeding.
        let pg_file_settings_query = self.pool_manager.query_catalog().pg_file_settings();
        let pg_file_settings_rows =
            if !self.enable_pg_file_settings_refresh || pg_file_settings_query.is_empty() {
                Vec::new()
            } else {
                match self.pool_manager.system_requests_connection().await {
                    Ok(conn) => conn
                        .query(pg_file_settings_query, &[], &[])
                        .await
                        .unwrap_or_else(|e| {
                            tracing::warn!("Failed to query pg_file_settings: {e}");
                            Vec::new()
                        }),
                    Err(e) => {
                        tracing::warn!("Failed to get connection for pg_file_settings: {e}");
                        Vec::new()
                    }
                }
            };

        let all_config_rows: Vec<_> = pg_config_rows
            .into_iter()
            .chain(pg_file_settings_rows)
            .collect();

        for pg_config in all_config_rows {
            let mut key = pg_config.get::<_, String>(0);

            for settings_prefix in &self.settings_prefixes {
                if key.starts_with(settings_prefix) {
                    key = key[settings_prefix.len()..].to_string();
                    break;
                }
            }

            let mut value: String = pg_config.get(1);
            if value == "on" || value.eq_ignore_ascii_case("true") {
                "true".clone_into(&mut value);
            } else if value == "off" || value.eq_ignore_ascii_case("false") {
                "false".clone_into(&mut value);
            }
            configs.insert(key.clone(), value);
        }

        let pg_is_in_recovery_row = match self.pool_manager.system_requests_connection().await {
            Ok(conn) => conn
                .query(
                    self.pool_manager.query_catalog().pg_is_in_recovery(),
                    &[],
                    &[],
                )
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!("Failed to query pg_is_in_recovery: {e}");
                    Vec::new()
                }),
            Err(e) => {
                tracing::warn!("Failed to get connection for pg_is_in_recovery: {e}");
                Vec::new()
            }
        };

        let in_recovery: bool = pg_is_in_recovery_row.first().is_some_and(|row| row.get(0));
        configs.insert(POSTGRES_RECOVERY_KEY.to_owned(), in_recovery.to_string());

        tracing::info!("Dynamic configurations loaded: {configs:?}");
        Ok(configs)
    }

    async fn load_host_config(dynamic_config_path: &str) -> Result<HostConfig> {
        let config: HostConfig = serde_json::from_str(
            &tokio::fs::read_to_string(dynamic_config_path).await?,
        )
        .map_err(|e| DocumentDBError::internal_error(format!("Failed to read config file: {e}")))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct PgConfiguration {
    inner: PgConfigurationInner,
    values: ArcSwap<HashMap<String, String>>,
    last_update_at: ArcSwap<Instant>,
    topology_bson: ArcSwap<RawBson>,
    cluster_version: ArcSwap<Option<ClusterVersion>>,
    refresh_task: Option<JoinHandle<()>>,
    watch_task: Option<JoinHandle<()>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WatchedFileState {
    exists: bool,
    modified_at: Option<SystemTime>,
    len: Option<u64>,
}

impl PgConfiguration {
    fn start_dynamic_configuration_refresh_thread(
        configuration: Arc<Self>,
        refresh_interval: u32,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(u64::from(refresh_interval)));
            interval.tick().await;

            loop {
                interval.tick().await;

                Self::reload_configuration(&configuration).await;
            }
        })
    }

    async fn reload_configuration(configuration: &Self) {
        if let Err(e) = configuration.refresh_configuration().await {
            tracing::error!("Config reload failed! {e}");
        }
    }

    async fn get_file_state(path: &Path) -> WatchedFileState {
        match tokio::fs::metadata(path).await {
            Ok(metadata) => WatchedFileState {
                exists: true,
                modified_at: metadata.modified().ok(),
                len: Some(metadata.len()),
            },
            Err(_) => WatchedFileState {
                exists: false,
                modified_at: None,
                len: None,
            },
        }
    }

    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn new(
        setup_configuration: &dyn SetupConfiguration,
        pool_manager: Arc<PoolManager>,
        settings_prefixes: Vec<String>,
    ) -> Result<Arc<Self>> {
        let inner = PgConfigurationInner {
            dynamic_config_path: setup_configuration.dynamic_configuration_file(),
            settings_prefixes,
            pool_manager,
            instance_kind: setup_configuration.instance_kind().to_owned(),
            enable_pg_file_settings_refresh: setup_configuration
                .enable_pg_file_settings_refresh()
                .unwrap_or(false),
        };

        let values = ArcSwap::from_pointee(inner.load_configurations().await?);
        let last_update_at = ArcSwap::from_pointee(Instant::now());
        let topology_bson = ArcSwap::from_pointee(
            Self::load_topology(&inner.pool_manager, &inner.instance_kind).await,
        );
        let cluster_version =
            ArcSwap::from_pointee(parse_cluster_version(&topology_bson.load_full()));

        let mut configuration = Arc::new(Self {
            inner,
            values,
            last_update_at,
            topology_bson,
            cluster_version,
            refresh_task: None,
            watch_task: None,
        });

        let refresh_interval = setup_configuration.dynamic_configuration_refresh_interval_secs();
        let watch_interval_ms = setup_configuration.host_configuration_watch_interval_ms();

        let refresh_task = Self::start_dynamic_configuration_refresh_thread(
            Arc::clone(&configuration),
            refresh_interval,
        );
        let watch_task = Self::start_config_watcher(Arc::clone(&configuration), watch_interval_ms);

        if let Some(config) = Arc::get_mut(&mut configuration) {
            config.refresh_task = Some(refresh_task);
            config.watch_task = Some(watch_task);
        }

        Ok(configuration)
    }

    pub fn last_update_at(&self) -> Instant {
        *self.last_update_at.load_full()
    }

    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn refresh_configuration(&self) -> Result<()> {
        let new_config = match self.inner.load_configurations().await {
            Ok(config) => config,
            Err(e) => {
                tracing::error!("Failed to reload configuration: {e}");
                return Err(e);
            }
        };

        self.values.store(Arc::new(new_config));
        let new_topology =
            Self::load_topology(&self.inner.pool_manager, &self.inner.instance_kind).await;
        let parsed_version = parse_cluster_version(&new_topology);
        self.topology_bson.store(Arc::new(new_topology));
        self.cluster_version.store(Arc::new(parsed_version));
        self.last_update_at.store(Arc::new(Instant::now()));

        Ok(())
    }

    fn start_config_watcher(configuration: Arc<Self>, watch_interval_ms: u64) -> JoinHandle<()> {
        let dynamic_config_path = configuration.inner.dynamic_config_path.clone();
        let file_path = Path::new(&dynamic_config_path).to_path_buf();
        let poll_interval = Duration::from_millis(watch_interval_ms);

        tracing::info!(
            "Config file polling watcher enabled on: {} ({}ms)",
            dynamic_config_path,
            poll_interval.as_millis()
        );

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            let mut previous_state = Self::get_file_state(&file_path).await;

            loop {
                interval.tick().await;
                let current_state = Self::get_file_state(&file_path).await;

                if current_state != previous_state {
                    tracing::info!(
                        "Config file state changed for {}. Reloading dynamic configuration.",
                        file_path.display()
                    );
                    Self::reload_configuration(&configuration).await;
                    previous_state = current_state;
                }
            }
        })
    }

    async fn load_topology(pool_manager: &PoolManager, instance_kind: &str) -> RawBson {
        let extension_versions_query = pool_manager.query_catalog().extension_versions();
        if extension_versions_query.is_empty() {
            return rawbson!({});
        }

        let results = match async {
            let conn = pool_manager.system_requests_connection().await?;
            conn.query(extension_versions_query, &[], &[])
                .await
                .map_err(DocumentDBError::from)
        }
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to load topology versions: {e}");
                return rawbson!({});
            }
        };

        let Some(result) = results.first() else {
            tracing::error!("No results returned for extension versions query");
            return rawbson!({});
        };

        let doc: std::result::Result<PgDocument, _> = result.try_get(0);
        match doc {
            Ok(doc) => {
                tracing::info!("Topology acquired: {doc:?}");
                match doc.0.get("internal") {
                    Ok(Some(value)) => rawbson!({
                        "documentdb_versions": value.to_raw_bson(),
                        "kind": instance_kind
                    }),
                    _ => rawbson!({}),
                }
            }
            Err(e) => {
                tracing::error!("Failed to parse extension versions: {e}");
                rawbson!({})
            }
        }
    }
}

impl DynamicConfiguration for PgConfiguration {
    fn get_str(&self, key: &str) -> Option<String> {
        self.values.load_full().get(key).cloned()
    }
    fn get_bool(&self, key: &str, default: bool) -> bool {
        self.values
            .load_full()
            .get(key)
            .map_or(default, |v| v.parse::<bool>().unwrap_or(default))
    }
    fn get_i32(&self, key: &str, default: i32) -> i32 {
        self.values
            .load_full()
            .get(key)
            .map_or(default, |v| v.parse::<i32>().unwrap_or(default))
    }
    fn get_u64(&self, key: &str, default: u64) -> u64 {
        self.values
            .load_full()
            .get(key)
            .map_or(default, |v| v.parse::<u64>().unwrap_or(default))
    }
    fn equals_value(&self, key: &str, value: &str) -> bool {
        self.values.load_full().get(key).is_some_and(|v| v == value)
    }

    fn topology(&self) -> RawBson {
        self.topology_bson.load_full().as_ref().clone()
    }

    fn cluster_version(&self) -> Option<ClusterVersion> {
        *self.cluster_version.load_full().as_ref()
    }

    fn enable_developer_explain(&self) -> bool {
        self.get_bool("enableDeveloperExplain", false)
    }

    fn max_connections(&self) -> usize {
        let max_connections = self.get_i32("max_connections", -1);
        match max_connections {
            n if n < 0 => {
                // theoretically we can't end up here, since Postgres always provide values
                tracing::error!("GUC max_connections is not setup correctly");
                25usize
            }
            n => usize::try_from(n).unwrap_or(25),
        }
    }

    fn allow_transaction_snapshot(&self) -> bool {
        self.get_bool("mongoAllowTransactionSnapshot", false)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Drop for PgConfiguration {
    fn drop(&mut self) {
        if let Some(refresh_task) = &self.refresh_task {
            refresh_task.abort();
        }

        if let Some(watch_task) = &self.watch_task {
            watch_task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        configuration::DocumentDBSetupConfiguration,
        postgres::{
            conn_mgmt::{
                ConnectionPool, PgPoolSettings, AUTHENTICATION_MAX_CONNECTIONS,
                SYSTEM_REQUESTS_MAX_CONNECTIONS,
            },
            create_query_catalog,
        },
    };

    /// Base setup configuration overridden with a guaranteed-closed local
    /// Postgres endpoint (127.0.0.1:1), so connection attempts fail fast and
    /// deterministically instead of depending on whatever Postgres endpoint
    /// happens to be unreachable in the test environment.
    fn test_setup_configuration() -> DocumentDBSetupConfiguration {
        DocumentDBSetupConfiguration {
            postgres_host_name: Some("127.0.0.1".to_owned()),
            postgres_port: Some(1),
            ..crate::testing::test_setup_configuration()
        }
    }

    /// Builds a real `PoolManager`. The pools are lazy (deadpool), so no live
    /// Postgres is required, but pool creation must run inside a Tokio runtime.
    fn test_pool_manager() -> Arc<PoolManager> {
        let setup_config = test_setup_configuration();
        let query_catalog = create_query_catalog();
        let user = setup_config.postgres_system_user().to_owned();

        let system_requests_pool = ConnectionPool::new_with_user(
            &setup_config,
            &query_catalog,
            &user,
            None,
            &format!("{}-SystemRequests", setup_config.application_name()),
            PgPoolSettings::system_pool_settings(SYSTEM_REQUESTS_MAX_CONNECTIONS),
        )
        .expect("failed to create system requests pool");

        let authentication_pool = ConnectionPool::new_with_user(
            &setup_config,
            &query_catalog,
            &user,
            None,
            &format!("{}-PreAuthRequests", setup_config.application_name()),
            PgPoolSettings::system_pool_settings(AUTHENTICATION_MAX_CONNECTIONS),
        )
        .expect("failed to create authentication pool");

        Arc::new(PoolManager::new(
            query_catalog,
            Box::new(setup_config),
            system_requests_pool,
            authentication_pool,
        ))
    }

    #[tokio::test]
    async fn load_configurations_errors_when_pg_settings_unavailable() {
        tokio::task::yield_now().await; // lets the lazy pools build inside the runtime
        let inner = PgConfigurationInner {
            dynamic_config_path: String::new(),
            settings_prefixes: Vec::new(),
            pool_manager: test_pool_manager(),
            instance_kind: String::new(),
            enable_pg_file_settings_refresh: false,
        };
        assert!(
            inner.load_configurations().await.is_err(),
            "expected Err when pg_settings is unavailable, so refresh keeps last-known-good config"
        );
    }

    /// Constructs a `PgConfiguration` directly (bypassing `new()`, which
    /// would itself fail against the unreachable pool manager) so we can
    /// seed known-good `values` and then exercise `refresh_configuration()`.
    fn test_configuration(values: HashMap<String, String>) -> PgConfiguration {
        PgConfiguration {
            inner: PgConfigurationInner {
                dynamic_config_path: String::new(),
                settings_prefixes: Vec::new(),
                pool_manager: test_pool_manager(),
                instance_kind: String::new(),
                enable_pg_file_settings_refresh: false,
            },
            values: ArcSwap::from_pointee(values),
            last_update_at: ArcSwap::from_pointee(Instant::now()),
            topology_bson: ArcSwap::from_pointee(rawbson!({})),
            cluster_version: ArcSwap::from_pointee(None),
            refresh_task: None,
            watch_task: None,
        }
    }

    #[tokio::test]
    async fn refresh_configuration_keeps_last_known_good_values_on_error() {
        tokio::task::yield_now().await; // lets the lazy pools build inside the runtime

        let mut initial_values = HashMap::new();
        initial_values.insert("max_connections".to_owned(), "100".to_owned());
        let config = test_configuration(initial_values);

        let last_update_before = config.last_update_at();

        let result = config.refresh_configuration().await;

        assert!(
            result.is_err(),
            "expected refresh_configuration() to fail against an unreachable Postgres endpoint"
        );
        assert_eq!(
            config.values.load().get("max_connections").map(String::as_str),
            Some("100"),
            "values must not be swapped when the refresh fails, keeping the last-known-good configuration"
        );
        assert_eq!(
            config.last_update_at(),
            last_update_before,
            "last_update_at must not change when the refresh fails"
        );
    }
}
