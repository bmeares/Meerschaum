//! The jobs backend of `mrsm start api`: every job operation consults the catalog and routes to
//! the local backend or the job's executor, never falling back to the local backend.

use std::collections::BTreeMap;

use super::{
    DelegatingJobs, JobPlacement, LogSanitizer, Placement, Record, StreamPublication,
    stream_publication, text,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::Map;
use meerschaum_core::SuccessTuple;
use meerschaum_server::backends::{CreateJob, JobsBackend, LogEvent, LogMonitor};
use serde_json::Value as Json;
use uuid::Uuid;

#[async_trait]
impl JobsBackend for DelegatingJobs {
    fn supports_bound_commands(&self) -> bool {
        true
    }

    async fn ready(&self, name: &str) -> Result<(), String> {
        match self.placement(name).await? {
            Placement::Remote(record) => {
                if record.placement == JobPlacement::HostApi {
                    return self.controller.host_jobs().map(|_| ());
                }
                self.controller.prepare_id(record.team_id).await.map(|_| ())
            }
            Placement::Hidden => Err("This job was not found.".into()),
            Placement::Local => self.local.ready(name).await,
        }
    }

    async fn try_list(&self) -> Result<IndexMap<String, Json>, String> {
        let records = self.catalog.records()?;
        let mut jobs = self.local.try_list().await?;
        for name in records.keys() {
            jobs.shift_remove(name);
        }
        for record in records
            .values()
            .filter(|r| r.placement == JobPlacement::HostApi)
        {
            jobs.shift_remove(&record.remote_name);
        }
        let mut teams: BTreeMap<Uuid, Vec<Record>> = BTreeMap::new();
        for record in self
            .controller
            .job_views(records.into_values().collect())
            .await?
        {
            if record.placement == JobPlacement::HostApi {
                let metadata = self.remote_metadata(&record).await;
                jobs.insert(record.name.clone(), metadata);
                continue;
            }
            teams.entry(record.team_id).or_default().push(record);
        }
        for (team, records) in teams {
            let remote = match self.controller.prepare_id(team).await {
                Ok(api) => api.try_get_jobs().await.ok(),
                Err(_) => None,
            };
            for record in records {
                let metadata = remote
                    .as_ref()
                    .and_then(|jobs| jobs.get(&record.remote_name))
                    .and_then(|m| serde_json::to_value(m).ok());
                if record.callback.is_none()
                    && metadata.as_ref().is_some_and(|m| {
                        m["status"] == "stopped" && m["completion_pending"] != true
                    })
                {
                    let _ = self.catalog.observe_stopped(&record);
                }
                jobs.insert(record.name.clone(), record.metadata(metadata.as_ref()));
            }
        }
        Ok(jobs)
    }

    async fn list(&self) -> IndexMap<String, Json> {
        self.try_list().await.unwrap_or_default()
    }

    async fn try_list_definitions(&self) -> Result<IndexMap<String, Json>, String> {
        let records = self.catalog.records()?;
        let mut jobs = self.local.try_list_definitions().await?;
        for name in records.keys() {
            jobs.shift_remove(name);
        }
        for record in records
            .values()
            .filter(|r| r.placement == JobPlacement::HostApi)
        {
            jobs.shift_remove(&record.remote_name);
        }
        for record in self
            .controller
            .job_views(records.into_values().collect())
            .await?
        {
            jobs.insert(record.name.clone(), record.metadata(None));
        }
        Ok(jobs)
    }

    async fn list_definitions(&self) -> IndexMap<String, Json> {
        self.try_list_definitions().await.unwrap_or_default()
    }

    async fn try_get(&self, name: &str) -> Result<Option<Json>, String> {
        match self.placement(name).await? {
            Placement::Remote(record) => Ok(Some(self.remote_metadata(&record).await)),
            Placement::Hidden => Ok(None),
            Placement::Local => self.local.try_get(name).await,
        }
    }

    async fn get(&self, name: &str) -> Option<Json> {
        self.try_get(name).await.ok().flatten()
    }

    async fn exists(&self, name: &str) -> bool {
        match self.placement(name).await {
            Ok(Placement::Remote(_)) => true,
            Ok(Placement::Hidden) => false,
            Ok(Placement::Local) => self.local.exists(name).await,
            // A corrupted catalog must not authorize replacing an unknown existing job.
            Err(_) => true,
        }
    }

    async fn create(&self, name: &str, sysargs: Vec<String>, properties: Option<Map>) -> CreateJob {
        self.create_canonical(name, sysargs, properties, None).await
    }

    async fn promote(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "promote").await {
            Some(result) => result,
            None => self.local.promote(name).await,
        }
    }
    async fn delete(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "delete").await {
            Some(result) => result,
            None => self.local.delete(name).await,
        }
    }
    async fn start(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "start").await {
            Some(result) => result,
            None => self.local.start(name).await,
        }
    }
    async fn stop(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "stop").await {
            Some(result) => result,
            None => self.local.stop(name).await,
        }
    }
    async fn pause(&self, name: &str) -> SuccessTuple {
        match self.mutate(name, "pause").await {
            Some(result) => result,
            None => self.local.pause(name).await,
        }
    }

    async fn logs(&self, name: &str) -> Option<String> {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) => {
                if record.placement == JobPlacement::HostApi {
                    let backend = self.controller.host_jobs().ok()?.backend();
                    let logs = meerschaum_server::executor::context::scope(
                        None,
                        backend.logs(&record.remote_name),
                    )
                    .await?;
                    return Some(LogSanitizer::default().push(&logs));
                }
                let api = self.controller.prepare_id(record.team_id).await.ok()?;
                let logs = api.get_logs(&record.remote_name).await.ok()?;
                Some(LogSanitizer::default().push(&logs))
            }
            Ok(Placement::Local) => self.local.logs(name).await,
            _ => None,
        }
    }

    async fn stop_time(&self, name: &str) -> Option<DateTime<Utc>> {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) => {
                let _ = self.remote_metadata(&record).await;
                self.catalog
                    .get(name)
                    .ok()
                    .flatten()?
                    .completed_at
                    .and_then(|t| DateTime::from_timestamp(t, 0))
            }
            Ok(Placement::Local) => self.local.stop_time(name).await,
            _ => None,
        }
    }

    async fn completion_time(&self, name: &str) -> Option<DateTime<Utc>> {
        match self.placement(name).await {
            Ok(Placement::Remote(_)) => self.stop_time(name).await,
            Ok(Placement::Local) => self.local.completion_time(name).await,
            _ => None,
        }
    }

    async fn is_blocking_on_stdin(&self, name: &str) -> bool {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) if record.placement == JobPlacement::HostApi => {
                match self.controller.host_jobs() {
                    Ok(host) => {
                        meerschaum_server::executor::context::scope(
                            None,
                            host.backend().is_blocking_on_stdin(&record.remote_name),
                        )
                        .await
                    }
                    Err(_) => false,
                }
            }
            Ok(Placement::Remote(record)) => match self.controller.prepare_id(record.team_id).await
            {
                Ok(api) => api
                    .try_get_job_is_blocking_on_stdin(&record.remote_name)
                    .await
                    .unwrap_or(false),
                Err(_) => false,
            },
            Ok(Placement::Local) => self.local.is_blocking_on_stdin(name).await,
            _ => false,
        }
    }

    async fn prompt_kwargs(&self, name: &str) -> Map {
        match self.placement(name).await {
            Ok(Placement::Remote(record)) if record.placement == JobPlacement::HostApi => {
                match self.controller.host_jobs() {
                    Ok(host) => {
                        meerschaum_server::executor::context::scope(
                            None,
                            host.backend().prompt_kwargs(&record.remote_name),
                        )
                        .await
                    }
                    Err(_) => Map::new(),
                }
            }
            Ok(Placement::Remote(record)) => match self.controller.prepare_id(record.team_id).await
            {
                Ok(api) => api
                    .try_get_job_prompt_kwargs(&record.remote_name)
                    .await
                    .unwrap_or_default(),
                Err(_) => Map::new(),
            },
            Ok(Placement::Local) => self.local.prompt_kwargs(name).await,
            _ => Map::new(),
        }
    }

    async fn monitor_logs(&self, name: &str) -> Option<LogMonitor> {
        let record = match self.placement(name).await {
            Ok(Placement::Remote(record)) => record,
            Ok(Placement::Local) => return self.local.monitor_logs(name).await,
            _ => return None,
        };
        if record.placement == JobPlacement::HostApi {
            let backend = self.controller.host_jobs().ok()?.backend();
            let monitor = meerschaum_server::executor::context::scope(
                None,
                backend.monitor_logs(&record.remote_name),
            )
            .await?;
            let LogMonitor { mut events, input } = monitor;
            let (sender, received) = tokio::sync::mpsc::channel(16);
            let catalog = self.catalog.clone();
            tokio::spawn(async move {
                let mut sanitizer = LogSanitizer::default();
                while let Some(event) = events.recv().await {
                    let event = match event {
                        LogEvent::Line(line) => LogEvent::Line(sanitizer.push(&line)),
                        LogEvent::Stopped(result) => {
                            let _ = catalog.observe_stopped(&record);
                            LogEvent::Stopped(SuccessTuple {
                                success: result.success,
                                message: text(&result.message, 16 * 1024),
                            })
                        }
                        event => event,
                    };
                    let stopped = matches!(event, LogEvent::Stopped(_));
                    if sender.send(event).await.is_err() || stopped {
                        break;
                    }
                }
            });
            return Some(LogMonitor {
                events: received,
                input,
            });
        }
        let api = self.controller.prepare_id(record.team_id).await.ok()?;
        api.clear_job_metadata_cache();
        let initial = api.get_job_metadata(&record.remote_name).await.ok()?;
        let launch = initial
            .get("daemon")?
            .get("properties")?
            .get("process")?
            .get("launch_id")?
            .as_str()?
            .to_string();
        let mut channel = api.open_log_channel(&record.remote_name).await.ok()?;
        let (events, received) = tokio::sync::mpsc::channel(16);
        let (input, mut inputs) = tokio::sync::mpsc::channel::<String>(4);
        let catalog = self.catalog.clone();
        tokio::spawn(async move {
            use meerschaum_connector_api::client::LogMessage;
            let mut sanitizer = LogSanitizer::default();
            loop {
                tokio::select! {
                    () = events.closed() => break,
                    input = inputs.recv() => match input {
                        Some(input) => if channel.send_input(input).await.is_err() { break; },
                        None => break,
                    },
                    message = channel.next() => {
                        let event = match message {
                            Ok(Some(LogMessage::Line(line))) => LogEvent::Line(sanitizer.push(&line)),
                            Ok(Some(LogMessage::InputRequested)) => LogEvent::StdinRequested,
                            Ok(Some(LogMessage::Stopped(_))) | Ok(None) => {
                                // The worker or stream can stop before the native supervisor
                                // publishes. Only the attached launch's committed result ends it.
                                let mut published = None;
                                loop {
                                    api.clear_job_metadata_cache();
                                    let Ok(meta) = api.get_job_metadata(&record.remote_name).await else { break; };
                                    let Ok(meta) = serde_json::to_value(meta) else { break; };
                                    match stream_publication(&meta, &launch) {
                                        StreamPublication::Published(result) => { published = Some(result); break; }
                                        StreamPublication::Unavailable => break,
                                        StreamPublication::Pending => tokio::select! {
                                            () = events.closed() => break,
                                            () = tokio::time::sleep(std::time::Duration::from_millis(25)) => {},
                                        },
                                    }
                                }
                                match published { Some(result) => LogEvent::Stopped(result), None => break }
                            }
                            _ => break,
                        };
                        let stopped = matches!(event, LogEvent::Stopped(_));
                        if stopped { let _ = catalog.observe_stopped(&record); }
                        if events.send(event).await.is_err() || stopped { break; }
                    }
                }
            }
            channel.close().await;
        });
        Some(LogMonitor {
            events: received,
            input,
        })
    }

    async fn team_jobs(&self, team: &str) -> Vec<String> {
        let Ok(records) = self.catalog.records() else {
            return Vec::new();
        };
        let mut names = self.local.team_jobs(team).await;
        names.retain(|name| {
            !records.contains_key(name)
                && !records.values().any(|record| {
                    record.placement == JobPlacement::HostApi && record.remote_name == *name
                })
        });
        let id = if meerschaum_core::access::same_team(Some(team), None) {
            Some(Uuid::nil())
        } else {
            self.controller.team_id(team).await.ok().flatten()
        };
        let Some(id) = id else {
            return names;
        };
        let records = records.into_values().filter(|r| r.team_id == id).collect();
        if let Ok(visible) = self.controller.job_views(records).await {
            names.extend(visible.into_iter().map(|r| r.name));
        }
        names
    }

    async fn retag_team(&self, old: &str, new: Option<&str>) -> Vec<String> {
        let Ok(records) = self.catalog.records() else {
            return Vec::new();
        };
        let mut names = self.local.retag_team(old, new).await;
        names.retain(|name| {
            !records.values().any(|record| {
                record.placement == JobPlacement::HostApi && record.remote_name == *name
            })
        });
        // Retired executor history remains bound to its original UUID, never made Everyone's.
        if let Some(new) = new
            && let Ok(Some(id)) = self.controller.team_id(new).await
            && let Ok(changed) = self.catalog.retag_id(id, new)
        {
            names.extend(changed);
        }
        names
    }
}
