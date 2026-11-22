//! Canonical per-source connector-register hooks, including Host/Everyone placement.

use super::*;
use meerschaum_server::executor::{
    context,
    registration::{Outcome, Registration, Sources},
};
use meerschaum_server::mcp::ConnectorRegister;
use std::collections::BTreeSet;

impl Controller {
    pub(crate) async fn connector_registration(
        self: &Arc<Self>,
        pipe: &meerschaum_core::Pipe,
    ) -> Result<ConnectorRegister, String> {
        let caller = context::current()
            .filter(|caller| !caller.no_auth && !caller.full_admin() && caller.auth.is_some())
            .ok_or("Connector registration lost its authenticated original source.")?;
        caller
            .require(&["pipes:register"], &[])
            .map_err(|error| error.detail_text())?;
        context::registration_scope(pipe.connector_keys().to_string(), Box::pin(async {
            let selected=self.selected(None).await?;
            let config=self.config()?;
            let sources=if let Some(team)=&selected {
                let id=team.id.as_deref().and_then(|id|Uuid::parse_str(id).ok()).ok_or("Registration needs a registered remote Team.")?;
                let grants:BTreeSet<_>=team.granted_plugins().into_iter().filter(|name|meerschaum_core::access::can_use_plugin(&caller.principal,name) && meerschaum_server::executor::source_ceiling::permits_name(&caller,name)).collect();
                if grants.is_empty() { return Err("No approved plugin sources provide this registration hook.".into()); }
                let mut catalog=self.discover_plugin_catalog(id,grants).await?.catalog().clone();
                for plugin in catalog.plugins.values_mut() { plugin.metadata=None; }
                Sources::Archives{catalog}
            } else {
                let caller=caller.clone();
                let directories=config.paths().plugins_dirs.clone();
                let hashes=tokio::task::spawn_blocking(move || {
                    let (sources,_)=crate::executor::host_jobs::plugin_sources(&caller,&directories)?;
                    crate::executor::registration::source_hashes(&sources)
                }).await.map_err(|_|"Registration source preparation was interrupted.")??;
                Sources::Files{hashes}
            };
            let registration=Arc::new(Registration{deadline:chrono::Utc::now().timestamp()+60,
                pipe_ref:pipe.pipe_ref(),parameters:pipe.raw_parameters().clone(),sources});
            Box::pin(registration.authorize(&self.state()?, &caller)).await.map_err(|error|error.detail_text())?;
            let argv=registration.sysargs();
            let request=context::connector_registration_scope(registration.clone(), Box::pin(async {
                match &selected {
                    Some(team)=>Box::pin(self.run(team, argv)).await,
                    None=>Box::pin(self.run_host_api(argv)).await,
                }
            }));
            let (result,_)=tokio::time::timeout(std::time::Duration::from_secs(40),request).await
                .map_err(|_|"The confined connector registration timed out; its bounded source lease is being retired.")??;
            if !result.success { return Err(result.message); }
            if result.message.len()>1024*1024 { return Err("The confined registration result exceeds its byte limit.".into()); }
            if let Sources::Files{hashes}=&registration.sources {
                let caller=caller.clone(); let directories=config.paths().plugins_dirs.clone(); let expected=hashes.clone();
                tokio::task::spawn_blocking(move || {
                    let (sources,_)=crate::executor::host_jobs::plugin_sources(&caller,&directories)?;
                    if crate::executor::registration::source_hashes(&sources)?!=expected {
                        return Err("Registration source content changed before metadata commit.".into());
                    }
                    Ok::<_,String>(())
                }).await.map_err(|_|"Registration source verification was interrupted.")??;
            }
            Box::pin(registration.authorize(&self.state()?, &caller)).await.map_err(|error|error.detail_text())?;
            let outcome:Outcome=serde_json::from_str(&result.message).map_err(|_|"The confined registration result is invalid.")?;
            Ok(match outcome {
                Outcome::Undefined=>ConnectorRegister::Undefined,
                Outcome::Parameters{parameters}=>ConnectorRegister::Parameters(parameters),
                Outcome::Invalid{representation}=>ConnectorRegister::Invalid(representation),
            })
        })).await
    }
}
