# Parameter consumer audit (wave 5 core)

This production-code inventory includes parameter fields and accessors, target fields and accessors, and raw serialized-map
lookups. Some hits concern non-Pipe structs; they are included so the audit can be checked.
Line numbers are a snapshot and may move during concurrent work.

All core accessors now share resolved parameters; template loaders and state serialization
explicitly retain raw data. Kernel receives dtype contexts, not raw Pipe metadata.

Requests for other crates:

- **engine**: `graph.rs` definition hashing and implicit-reference extraction must use
  `raw_parameters()`. `sync.rs` template detection, loader merges and replacement must also
  start with raw parameters, then cache the result with `resolve_parameters_with`. Otherwise
  lazy fallback can consume a reference before the instance-backed resolver sees it.
- **sql / connector**: prepare an instance-backed `TemplateResolver` at sync/fetch entry points
  and call `resolve_parameters_with` before binding `SqlPipe`; provide flavor/schema quoting.
  Self templates work automatically, but core cannot fetch registered cross-pipe state.
  Registration/edit persistence in `instance/pipes.rs` must use `raw_parameters()`.
- **api / valkey / worker / server / compose / py / cli**: use raw parameters for metadata
  persistence, forwarding unresolved definitions, and merging loaded metadata; keep resolved
  parameters for execution. Resolver adapters' `raw_parameters` methods must call the new raw
  accessor. Supply registered references/config via the explicit resolver hook per run.
- **cli / sql**: import `meerschaum_core::version::version_ge`; SQL can retain its existing
  public function as a forwarding wrapper. The old SQL function was left untouched because
  it is outside this task's ownership.

```text
crates/meerschaum-worker/src/sync.rs:59:    pipe.parameters()
crates/meerschaum-worker/src/sync.rs:86:    let ctx = dtypes_context(instance, pipe, pipe.parameters())
crates/meerschaum-worker/src/sync.rs:124:/// under the pipe's own (v4: `apply_patch_to_config(params, self.parameters)`).
crates/meerschaum-worker/src/sync.rs:138:                let merged = apply_patch_maps(&outcome.parameters, pipe.parameters());
crates/meerschaum-worker/src/sync.rs:144:                outcome.parameters
crates/meerschaum-worker/src/connector.rs:426:            parameters: match m.get("parameters") {
crates/meerschaum-worker/src/callback.rs:108:    if pipe.parameters().is_empty() {
crates/meerschaum-worker/src/callback.rs:109:        // `Pipe.parameters` reads the registered parameters unless set in memory.
crates/meerschaum-worker/src/callback.rs:111:            if let Some(CValue::Map(params)) = attrs.get("parameters") {
crates/meerschaum-worker/src/callback.rs:116:    let params = pipe.parameters().clone();
crates/meerschaum-worker/src/callback.rs:145:            .param("dtypes")
crates/meerschaum-valkey/src/instance.rs:24:        let source_key = match pipe.parameters().get("valkey").and_then(|v| v.get("key")) {
crates/meerschaum-jobs/src/systemd.rs:151:            "[Unit]\nDescription=Run the job '{name}'\n\n[Service]\nExecStart={python} -m meerschaum {sysargs}\nKillSignal=SIGTERM\nRestart=always\nRestartPreventExitStatus=0\nSyslogIdentifier={service_name}\n{environment}\n\n[Install]\nWantedBy=default.target\n",
crates/meerschaum-connector/src/pipe_ops.rs:212:    let ctx = match dtypes_context(conn, pipe, pipe.parameters()).await {
crates/meerschaum-valkey/src/pipes.rs:24:/// `pipe.target` on a Valkey instance (no per-flavor truncation).
crates/meerschaum-valkey/src/pipes.rs:26:    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
crates/meerschaum-valkey/src/pipes.rs:38:/// `pipe.parameters.get('valkey', {}).get('dtypes', {})`.
crates/meerschaum-valkey/src/pipes.rs:87:        return Ok(pipe.parameters().clone());
crates/meerschaum-valkey/src/pipes.rs:92:        .unwrap_or_else(|| pipe.parameters().clone()))
crates/meerschaum-valkey/src/pipes.rs:146:        let parameters_str = dumps_parameters(pipe.parameters());
crates/meerschaum-valkey/src/pipes.rs:235:        match self.set(&key, &dumps_parameters(pipe.parameters())).await {
crates/meerschaum-valkey/src/pipes.rs:299:        let mut parameters = pipe.parameters().clone();
crates/meerschaum-valkey/src/pipes.rs:380:        let parameters = pipe.parameters();
crates/meerschaum-valkey/src/pipes.rs:480:        let upsert = pipe.parameters().get("upsert").is_some_and(|v| v.truthy());
crates/meerschaum-valkey/src/pipes.rs:481:        let static_ = pipe.parameters().get("static").is_some_and(|v| v.truthy());
crates/meerschaum-valkey/src/pipes.rs:485:        let configured = pipe_ops::configured_dtypes(pipe.parameters());
crates/meerschaum-valkey/src/pipes.rs:486:        let mut vk_dtypes = valkey_dtypes(pipe.parameters());
crates/meerschaum-valkey/src/pipes.rs:545:            let ctx = pipe_ops::dtypes_context(self, &pipe, pipe.parameters()).await?;
crates/meerschaum-valkey/src/pipes.rs:550:                autoincrement: pipe_ops::bool_param(pipe.parameters(), "autoincrement", false),
crates/meerschaum-valkey/src/pipes.rs:551:                autotime: pipe_ops::bool_param(pipe.parameters(), "autotime", false),
crates/meerschaum-valkey/src/pipes.rs:712:        Ok(valkey_dtypes(pipe.parameters())
crates/meerschaum-valkey/src/pipes.rs:774:        let dt_typ = pipe_ops::configured_dtypes(pipe.parameters())
crates/meerschaum-jobs/src/daemon.rs:883:    let args = props.get("target")?.get("args")?.as_list()?;
crates/meerschaum-core/src/template.rs:10://! - `{{ Pipe(...).columns['datetime'] }}`, `.columns`, `.dtypes`, `.target`, `.indices`: an
crates/meerschaum-core/src/template.rs:12://! - `{{ self.metric }}`, `{{ self.parameters['x']['y'] }}` when resolving a pipe's own
crates/meerschaum-core/src/template.rs:13://!   parameters (`self.parameters` reads the raw, unresolved parameters).
crates/meerschaum-core/src/template.rs:191:    /// quoted, schema-qualified name (`sql_item_name`) on SQL instances and `pipe.target`
crates/meerschaum-core/src/template.rs:192:    /// elsewhere; the default is `pipe.target` without truncation.
crates/meerschaum-core/src/template.rs:194:        pipe.target(None).map_err(|e| e.to_string())
crates/meerschaum-core/src/template.rs:209:    /// `pipe.attributes['parameters']`: the raw parameters (used for `{{ self.parameters }}`).
crates/meerschaum-core/src/template.rs:310:/// `pipe.parameters` property returns them.
crates/meerschaum-core/src/template.rs:324:    out.set_parameters(resolved.parameters);
crates/meerschaum-core/src/template.rs:341:    let (pipe, chain) = match (&template.target, self_pipe) {
crates/meerschaum-core/src/template.rs:343:            let chain_src = match &template.target {
crates/meerschaum-core/src/template.rs:533:        } = t.target
crates/meerschaum-core/src/template.rs:648:            .parameters(
crates/meerschaum-core/src/template.rs:690:            .parameters(
crates/meerschaum-core/src/template.rs:699:        let out = replace_pipes_syntax("{{ self.parameters['a']['b'] }}", Some(&me), &r);
crates/meerschaum-core/src/template.rs:711:            "SELECT * FROM {{ Pipe('sql:main', 'a') }} JOIN {{ mrsm.Pipe('sql:main', 'b', 'x', instance='sql:other').target }}",
crates/meerschaum-core/src/pipe/resolve.rs:125:            .parameters(
crates/meerschaum-core/src/pipe/resolve.rs:135:            .parameters(
crates/meerschaum-core/src/pipe/resolve.rs:152:            out.parameters.get("columns"),
crates/meerschaum-core/src/pipe/resolve.rs:156:            out.parameters.get("sql"),
crates/meerschaum-core/src/pipe/chunk.rs:127:            self.parameters()
crates/meerschaum-core/src/pipe/chunk.rs:150:            .parameters()
crates/meerschaum-core/src/pipe/chunk.rs:187:            .parameters()
crates/meerschaum-core/src/pipe/chunk.rs:262:            .parameters()
crates/meerschaum-core/src/pipe/chunk.rs:280:            .parameters(
crates/meerschaum-py/src/sql_api.rs:827:        let before = p.parameters().clone();
crates/meerschaum-py/src/sql_api.rs:854:        let patch = changed_keys(&before, p.parameters());
crates/meerschaum-core/src/pipe/mod.rs:605:        m.insert("parameters".into(), Value::Map(self.parameters.clone()));
crates/meerschaum-core/src/pipe/mod.rs:622:            return &self.parameters;
crates/meerschaum-core/src/pipe/mod.rs:626:            let config = Value::Map(self.parameters.clone())
crates/meerschaum-core/src/pipe/mod.rs:632:                .map(|r| r.parameters)
crates/meerschaum-core/src/pipe/mod.rs:633:                .unwrap_or_else(|_| self.parameters.clone())
crates/meerschaum-core/src/pipe/mod.rs:639:        &self.parameters
crates/meerschaum-core/src/pipe/mod.rs:651:        let _ = self.resolved_cache.set(result.parameters);
crates/meerschaum-core/src/pipe/mod.rs:658:        &mut self.parameters
crates/meerschaum-core/src/pipe/mod.rs:661:    /// `pipe.parameters = {...}`.
crates/meerschaum-core/src/pipe/mod.rs:664:        self.parameters = parameters;
crates/meerschaum-core/src/pipe/mod.rs:670:        self.parameters = apply_patch_maps(&self.parameters, &patch);
crates/meerschaum-core/src/pipe/mod.rs:679:    /// A top-level parameter (`pipe.parameters.get(key)`).
crates/meerschaum-core/src/pipe/mod.rs:681:        self.parameters().get(key)
crates/meerschaum-core/src/pipe/mod.rs:686:        match self.param("columns") {
crates/meerschaum-core/src/pipe/mod.rs:720:        if self.parameters().contains_key("indexes") {
crates/meerschaum-core/src/pipe/mod.rs:733:        let configured = match self.param(self.indices_key()) {
crates/meerschaum-core/src/pipe/mod.rs:768:        self.param("tags")
crates/meerschaum-core/src/pipe/mod.rs:799:        let configured = self.param("dtypes").cloned().unwrap_or(Value::Null);
crates/meerschaum-core/src/pipe/mod.rs:801:        let raw_columns = self.param("columns");
crates/meerschaum-core/src/pipe/mod.rs:845:        self.param(key).map_or(default, Value::truthy)
crates/meerschaum-core/src/pipe/mod.rs:922:    /// `pipe.target` without side effects. `sql_flavor` is the instance connector's flavor when
crates/meerschaum-core/src/pipe/mod.rs:933:        if let Some(t) = self.param("target").filter(|v| v.truthy()) {
crates/meerschaum-core/src/pipe/mod.rs:940:            if let Some(v) = self.param(k) {
crates/meerschaum-core/src/pipe/mod.rs:959:    /// `pipe.target` with Python's side effect: a non-default target (e.g. from
crates/meerschaum-core/src/pipe/mod.rs:963:        if !is_default && self.param("target").filter(|v| v.truthy()).is_none() {
crates/meerschaum-core/src/pipe/mod.rs:969:    /// `pipe.target = name`.
crates/meerschaum-core/src/pipe/mod.rs:983:            .param("precision")
crates/meerschaum-core/src/pipe/mod.rs:993:                .param("columns")
crates/meerschaum-core/src/pipe/mod.rs:1068:        let existing = self.param("precision").cloned();
crates/meerschaum-core/src/pipe/mod.rs:1115:            "parameters" => Value::Map(self.parameters().clone()),
crates/meerschaum-core/src/pipe/mod.rs:1130:            "parameters" => Value::Map(self.parameters().clone()),
crates/meerschaum-core/src/pipe/mod.rs:1135:            "target" => Value::Str(self.target(None).ok()?),
crates/meerschaum-core/src/pipe/mod.rs:1158:        let key = if self.parameters().contains_key(plural) {
crates/meerschaum-core/src/pipe/mod.rs:1163:        let refs = self.param(key).cloned().unwrap_or(Value::Null);
crates/meerschaum-core/src/pipe/mod.rs:1265:        assert_eq!(p.target(None).unwrap(), "sql_main_weather_us");
crates/meerschaum-core/src/pipe/mod.rs:1291:            .parameters(params.as_map().unwrap().clone())
crates/meerschaum-py/src/pipe_bridge.rs:18:    /// `pipe.parameters` (references, `{{ Pipe() }}` and `MRSM{}` resolved): data methods.
crates/meerschaum-py/src/pipe_bridge.rs:87:        .parameters(parameters)
crates/meerschaum-web/src/pages.rs:171:        let parameters = &attributes["parameters"];
crates/meerschaum-py/src/build_info.rs:75:    dict.set_item("target", info.target)?;
crates/meerschaum-py/src/build_info.rs:106:        assert!(!info.target.is_empty());
crates/meerschaum-engine/src/python.rs:172:            pipe.target(None).map_err(|e| e.to_string())
crates/meerschaum-web/src/sessions.rs:41:            .parameters(parameters)
crates/meerschaum-py/src/core_api.rs:212:        if !pipe.parameters().is_empty() {
crates/meerschaum-py/src/core_api.rs:215:                core_to_py(self.py, &Value::Map(pipe.parameters().clone()))?,
crates/meerschaum-py/src/core_api.rs:271:            return pipe.parameters().clone();
crates/meerschaum-engine/src/engine.rs:521:        Some(Value::Map(params)) => rec.parameters = params.clone(),
crates/meerschaum-engine/src/engine.rs:523:            rec.parameters
crates/meerschaum-engine/src/engine.rs:614:                        if let Some(Value::Map(p)) = attrs.get("parameters") {
crates/meerschaum-engine/src/engine.rs:615:                            rec.parameters = p.clone();
crates/meerschaum-engine/src/engine.rs:1128:                let config = StreamConfig::from_parameters(pipe.parameters());
crates/meerschaum-cli/src/native/engine.rs:65:            pipe.target(None).map_err(|e| e.to_string())
crates/meerschaum-cli/src/native/engine.rs:101:            let ctx = pipe_ops::dtypes_context(self.0.as_ref(), pipe, pipe.parameters())
crates/meerschaum-engine/src/native.rs:270:            .map(|n| (n.pipe_ref.clone(), n.record.parameters.clone()))
crates/meerschaum-compose/src/subactions.rs:423:/// A resolver over the instance for `pipe.parameters` (`{{ Pipe() }}` references load the
crates/meerschaum-compose/src/subactions.rs:443:            out.set_parameters(apply_patch_maps(&params, pipe.parameters()));
crates/meerschaum-compose/src/subactions.rs:453:/// `pipe.parameters` (the property): the registered parameters patched with the local ones,
crates/meerschaum-compose/src/subactions.rs:456:    let local = pipe.parameters().clone();
crates/meerschaum-compose/src/subactions.rs:478:            resolved.parameters
crates/meerschaum-compose/src/subactions.rs:560:        let local_params = pipe.parameters().clone();
crates/meerschaum-compose/src/subactions.rs:656:                .map(|r| apply_patch_maps(&r, tagged_pipe.parameters()))
crates/meerschaum-compose/src/subactions.rs:657:                .unwrap_or_else(|| tagged_pipe.parameters().clone());
crates/meerschaum-compose/src/subactions.rs:985:        let remote_params = attrs.get("parameters").cloned();
crates/meerschaum-compose/src/subactions.rs:1129:            let local_params = pipe.parameters().clone();
crates/meerschaum-cli/src/native/verify.rs:35:            sp.pipe.parameters(),
crates/meerschaum-cli/src/native/verify.rs:144:    let verify = sp.pipe.parameters().get("verify")?.as_map()?.clone();
crates/meerschaum-cli/src/native/verify.rs:163:            .parameters()
crates/meerschaum-engine/src/instance_state.rs:49:        .parameters(kind.parameters())
crates/meerschaum-engine/src/instance_state.rs:188:    let dtypes: IndexMap<String, String> = match kind.parameters().get("dtypes") {
crates/meerschaum-compose/src/lint.rs:33:    /// Where the finding is (`pipes[1].parameters.dtype`).
crates/meerschaum-compose/src/lint.rs:248:        if let Some(Value::Map(params)) = meta.get("parameters") {
crates/meerschaum-compose/src/lint.rs:253:                        location: format!("{label}.parameters.{key}"),
crates/meerschaum-compose/src/lint.rs:264:                        location: format!("{label}.parameters.tags"),
crates/meerschaum-compose/src/lint.rs:284:                if let TemplateTarget::Pipe { call: Err(e), .. } = &t.target {
crates/meerschaum-engine/src/selection.rs:148:        let pipe_tags: Vec<String> = match record.parameters.get("tags") {
crates/meerschaum-sql/src/instance/mod.rs:386:    /// `sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))`.
crates/meerschaum-sql/src/instance/mod.rs:389:            &pipe.target,
crates/meerschaum-sql/src/instance/geopackage.rs:367:        let table = sp.target.clone();
crates/meerschaum-sql/src/instance/geopackage.rs:452:        let Some(geom_col) = self.geopackage_geometry_column(&sp.target).await else {
crates/meerschaum-engine/src/sync.rs:168:            out.set_parameters(apply_patch_maps(params, pipe.parameters()));
crates/meerschaum-engine/src/sync.rs:188:/// `pipe.parameters` (the property): `{{ Pipe(...) }}` templates resolved.
crates/meerschaum-engine/src/sync.rs:191:    if !has_templates(&Value::Map(pipe.parameters().clone())) {
crates/meerschaum-engine/src/sync.rs:195:    let resolved = replace_in_parameters(pipe.parameters(), Some(pipe), &resolver, &mut warnings);
crates/meerschaum-engine/src/sync.rs:222:        if pipe.param("precision").is_some_and(|p| p.truthy()) {
crates/meerschaum-engine/src/sync.rs:446:    // `pipe.parameters`: the registered parameters patched with the in-memory ones.
crates/meerschaum-engine/src/sync.rs:450:            if let Some(Value::Map(registered)) = attrs.get("parameters") {
crates/meerschaum-engine/src/sync.rs:451:                let merged = apply_patch_maps(registered, pipe.parameters());
crates/meerschaum-engine/src/sync.rs:502:            let dtypes = match pipe_ops::get_dtypes(instance, &pipe, pipe.parameters(), true).await
crates/meerschaum-engine/src/sync.rs:577:    let mut ctx = match pipe_ops::dtypes_context(instance, &pipe, pipe.parameters()).await {
crates/meerschaum-engine/src/sync.rs:627:            .param("dtypes")
crates/meerschaum-engine/src/graph.rs:417:    let minutes = backtrack_minutes(pipe.parameters(), options.default_backtrack_minutes);
crates/meerschaum-engine/src/graph.rs:427:        .parameters()
crates/meerschaum-engine/src/graph.rs:480:            let definition_hash = definition_hash(pipe.parameters());
crates/meerschaum-engine/src/graph.rs:513:                let mut params = pipe.parameters().clone();
crates/meerschaum-cli/src/native/manage.rs:60:    if pipe.parameters().is_empty() {
crates/meerschaum-cli/src/native/manage.rs:108:            let existing = pipe.parameters().clone();
crates/meerschaum-cli/src/native/manage.rs:203:        let patched = apply_patch_maps(sp.pipe.parameters(), params);
crates/meerschaum-cli/src/native/manage.rs:234:            Err(_) => pipe.target(None).unwrap_or_default(),
crates/meerschaum-cli/src/native/manage.rs:236:        None => pipe.target(None).unwrap_or_default(),
crates/meerschaum-cli/src/native/manage.rs:613:            Some(sp.pipe.parameters().clone()),
crates/meerschaum-cli/src/native/pipes.rs:96:        b = b.parameters(p);
crates/meerschaum-cli/src/native/pipes.rs:245:        if let Some(p) = attrs.get("parameters").and_then(Value::as_map) {
crates/meerschaum-cli/src/native/pipes.rs:247:            for (k, v) in sp.pipe.parameters() {
crates/meerschaum-cli/src/native/pipes.rs:288:    m.insert("parameters".into(), Value::Map(pipe.parameters().clone()));
crates/meerschaum-plugin/src/host.rs:265:    json!({"pipe": {"keys":pipe.keys(), "parameters":pipe.parameters(), "temporary":pipe.temporary}})
crates/meerschaum-engine/src/registry.rs:113:    /// schema-qualified name (`sql_item_name`) on SQL instances and `pipe.target` elsewhere;
crates/meerschaum-engine/src/registry.rs:116:        pipe.target(None).map_err(|e| e.to_string())
crates/meerschaum-engine/src/registry.rs:233:            None => pipe.target(None).map_err(|e| e.to_string()),
crates/meerschaum-cli/src/native/sync.rs:137:            let ctx = match pipe_ops::dtypes_context(conn.as_ref(), &sp.pipe, sp.pipe.parameters())
crates/meerschaum-sql/src/instance/inplace.rs:146:        let target = sp.target.clone();
crates/meerschaum-compose/src/ops.rs:129:                let rec_tags: Vec<String> = match r.parameters.get("tags") {
crates/meerschaum-compose/src/ops.rs:139:                    .parameters(r.parameters.clone())
crates/meerschaum-compose/src/ops.rs:150:            .map(|r| r.parameters.clone())
crates/meerschaum-compose/src/ops.rs:160:            pipe.parameters().clone(),
crates/meerschaum-compose/src/ops.rs:163:            r.parameters = pipe.parameters().clone();
crates/meerschaum-cli/src/native/show.rs:46:        Value::Map(sp.pipe.parameters().clone()),
crates/meerschaum-cli/src/native/show.rs:386:            pipe.parameters(),
crates/meerschaum-engine/src/node.rs:157:        self.parameters = parameters;
crates/meerschaum-engine/src/node.rs:176:            .parameters(self.parameters.clone())
crates/meerschaum-engine/src/node.rs:236:                Some(Value::Map(params)) => record.parameters = params.clone(),
crates/meerschaum-engine/src/node.rs:239:                        .parameters
crates/meerschaum-engine/src/node.rs:283:            recs[1].parameters.get("tags"),
crates/meerschaum-engine/src/node.rs:293:        assert!(recs[0].parameters.contains_key("columns"));
crates/meerschaum-sql/src/pipe.rs:81:    /// `pipe.target` as the instance resolves it (truncated to the flavor's identifier limit).
crates/meerschaum-sql/src/pipe.rs:98:    /// Bind a pipe to a SQL flavor: `pipe.target` with the flavor's truncation.
crates/meerschaum-sql/src/pipe.rs:100:        let target = pipe.target(Some(flavor.as_str()))?;
crates/meerschaum-sql/src/pipe.rs:126:    /// `pipe.parameters.get('hypertable', True)`.
crates/meerschaum-sql/src/pipe.rs:128:        self.param("hypertable").is_none_or(Value::truthy)
crates/meerschaum-sql/src/pipe.rs:131:    /// `pipe.parameters.get('hypercore', True)`.
crates/meerschaum-sql/src/pipe.rs:133:        self.param("hypercore").is_none_or(Value::truthy)
crates/meerschaum-sql/src/pipe.rs:186:            .parameters(parameters)
crates/meerschaum-sql/src/pipe.rs:202:        assert_eq!(p.target, "plugin_x_m");
crates/meerschaum-sql/src/instance/fetch.rs:78:                "Could not determine a SQL definition for {pipe}.\n    Set the key `query` in `pipe.parameters` to a valid SQL query."
crates/meerschaum-sql/src/queries/update.rs:281:    let target_table_name = sql_item_name_opt(args.target, flavor, schema);
crates/meerschaum-engine/src/executor.rs:860:                        .map(|n| RetryPolicy::from_parameters(&n.record.parameters))
crates/meerschaum-sql/src/queries/metadef.rs:23:    let definition = truthy_str(pipe.param("fetch").and_then(|f| f.get("definition")))
crates/meerschaum-sql/src/queries/metadef.rs:24:        .or_else(|| truthy_str(pipe.param("definition")))
crates/meerschaum-sql/src/queries/metadef.rs:25:        .or_else(|| truthy_str(pipe.param("query")))
crates/meerschaum-sql/src/queries/metadef.rs:26:        .or_else(|| truthy_str(pipe.param("sql")))?;
crates/meerschaum-sql/src/queries/metadef.rs:75:    /// `parent.target`.
crates/meerschaum-sql/src/queries/metadef.rs:369:            let parent_target = parent.target.as_str();
crates/meerschaum-sql/src/instance/pipes.rs:79:            self.json_param(pipe.parameters()),
crates/meerschaum-sql/src/instance/pipes.rs:124:            "SELECT {table}.pipe_id, {table}.connector_keys, {table}.metric_key, {table}.location_key, {table}.parameters \nFROM {table} \nWHERE {table}.pipe_id = {}",
crates/meerschaum-sql/src/instance/pipes.rs:151:            Value::Map(parse_parameters(doc.get("parameters"))),
crates/meerschaum-sql/src/instance/pipes.rs:162:        Ok(attrs.get("parameters").map(|v| v.to_map()))
crates/meerschaum-sql/src/instance/pipes.rs:177:            pipe.parameters().clone()
crates/meerschaum-sql/src/instance/pipes.rs:183:            apply_patch_maps(&original, pipe.parameters())
crates/meerschaum-sql/src/instance/pipes.rs:329:                    format!("CAST(CAST({table}.parameters AS JSONB) -> 'tags' AS JSONB)");
crates/meerschaum-sql/src/instance/pipes.rs:351:                        format!("CAST({table}.parameters AS CHAR)")
crates/meerschaum-sql/src/instance/pipes.rs:353:                    _ => format!("CAST({table}.parameters AS VARCHAR)"),
crates/meerschaum-sql/src/instance/pipes.rs:398:            "SELECT {table}.pipe_id, {table}.connector_keys, {table}.metric_key, {table}.location_key, {table}.parameters \nFROM {table} \nWHERE {where_str} ORDER BY {table}.connector_keys ASC, {table}.metric_key ASC, {loc_order}"
crates/meerschaum-sql/src/instance/pipes.rs:412:                    extra: Some(Value::Map(parse_parameters(doc.get("parameters")))),
crates/meerschaum-engine/src/runs.rs:283:        params.insert("target".into(), Value::Str(self.target()));
crates/meerschaum-engine/src/runs.rs:309:            let params = p.parameters();
crates/meerschaum-engine/src/runs.rs:310:            assert_eq!(params["target"], Value::Str(p.target()));
crates/meerschaum-engine/src/runs.rs:322:        let dtypes = EnginePipe::Ledger.parameters()["dtypes"].clone();
crates/meerschaum-engine/src/runs.rs:333:        let dtypes = EnginePipe::Nodes.parameters()["dtypes"].clone();
crates/meerschaum-sql/src/instance/indices.rs:46:            .get_table_cols_indices(&sp.target, self.get_pipe_schema(sp).as_deref())
crates/meerschaum-sql/src/instance/indices.rs:62:                        .sqlite_master_has(&geopackage::rtree_name(&sp.target, &col))
crates/meerschaum-sql/src/instance/indices.rs:65:                    queries.insert(key, geopackage::get_rtree_create_queries(&sp.target, &col));
crates/meerschaum-sql/src/instance/indices.rs:90:        let exists = self.table_exists(&sp.target, schema.as_deref()).await;
crates/meerschaum-sql/src/instance/indices.rs:95:            .get_table_cols_indices(&sp.target, schema.as_deref())
crates/meerschaum-sql/src/instance/indices.rs:106:        let temp_table = format!("_{}_temp_migration", sp.target);
crates/meerschaum-sql/src/instance/indices.rs:120:                    .sqlite_master_has(&geopackage::rtree_name(&sp.target, &col))
crates/meerschaum-sql/src/instance/indices.rs:125:                        geopackage::get_rtree_drop_queries(&sp.target, &col, extensions),
crates/meerschaum-sql/src/queries/compress.rs:80:    let settings = match pipe.param("compress") {
crates/meerschaum-sql/src/queries/compress.rs:134:    // Reads `pipe.parameters['dtypes']` directly (no alias resolution, no inference).
crates/meerschaum-sql/src/queries/compress.rs:135:    let dt_typ = match pipe.param("dtypes").and_then(|d| d.get(&dt_col)) {
crates/meerschaum-sql/src/queries/compress.rs:293:        &pipe.target,
crates/meerschaum-sql/src/queries/compress.rs:311:        pipe.target.replace('\'', "''")
crates/meerschaum-sql/src/queries/compress.rs:514:                    pipe.target.replace('\'', "''")
crates/meerschaum-sql/src/queries/compress.rs:524:            pipe.target.replace('\'', "''")
crates/meerschaum-sql/src/instance/partition.rs:148:            partition_scheme_name(self.flavor, &sp.target),
crates/meerschaum-sql/src/instance/partition.rs:318:            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-sql/src/instance/partition.rs:403:            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-sql/src/instance/partition.rs:409:            .param("verify")
crates/meerschaum-sql/src/instance/partition.rs:527:        crate::queries::partition::get_initial_partition_bounds(self.flavor, &sp.target, &ranges)
crates/meerschaum-server/src/pipe_methods.rs:24:    let dtypes = pipe_ops::get_dtypes(conn.as_ref(), pipe, pipe.parameters(), true)
crates/meerschaum-server/src/pipe_methods.rs:57:        if pipe.param("precision").is_some_and(|p| p.truthy()) {
crates/meerschaum-server/src/pipe_methods.rs:400:    let ctx = match pipe_ops::dtypes_context(conn.as_ref(), pipe, pipe.parameters()).await {
crates/meerschaum-sql/src/instance/sync.rs:160:            // `Pipe.parameters` reads the registered parameters unless set in memory.
crates/meerschaum-sql/src/instance/sync.rs:161:            if pipe.parameters().is_empty() {
crates/meerschaum-sql/src/instance/sync.rs:204:                .param("dtypes")
crates/meerschaum-sql/src/instance/sync.rs:228:        if result.success && pipe.param("compress").is_some_and(|v| v.truthy()) {
crates/meerschaum-sql/src/instance/sync.rs:381:        // PARITY: `autoincrement not in pipe.parameters` tests the boolean against the keys, so
crates/meerschaum-sql/src/instance/sync.rs:463:                    &sp.target,
crates/meerschaum-sql/src/instance/sync.rs:485:                let max_id_query = get_max_id_query(&sp.target, pk, flavor, schema.as_deref());
crates/meerschaum-sql/src/instance/sync.rs:488:                    &sp.target,
crates/meerschaum-sql/src/instance/sync.rs:776:            &sp.target,
crates/meerschaum-sql/src/instance/sync.rs:831:        .parameters(parameters)
crates/meerschaum-sql/src/instance/sync.rs:909:        let mut args = UpdateQueriesArgs::new(&sp.target, &temp_target, join_cols, flavor);
crates/meerschaum-sql/src/instance/sync.rs:1048:                &sp.target,
crates/meerschaum-sql/src/instance/sync.rs:1116:        if !self.table_exists(&sp.target, schema.as_deref()).await || sp.is_static() {
crates/meerschaum-sql/src/instance/sync.rs:1121:            .get_table_cols_types(&sp.target, schema.as_deref())
crates/meerschaum-sql/src/instance/sync.rs:1175:        if !self.table_exists(&sp.target, schema.as_deref()).await || sp.is_static() {
crates/meerschaum-sql/src/instance/sync.rs:1240:        let target = sp.target.clone();
crates/meerschaum-server/src/mcp/tools.rs:115:/// `pipe.target`.
crates/meerschaum-server/src/mcp/tools.rs:117:    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
crates/meerschaum-server/src/mcp/tools.rs:163:        b = b.parameters(p);
crates/meerschaum-server/src/mcp/tools.rs:231:        meerschaum_connector::pipe_ops::get_dtypes(conn.as_ref(), pipe, pipe.parameters(), true)
crates/meerschaum-server/src/mcp/tools.rs:522:        .parameters()
crates/meerschaum-server/src/mcp/tools.rs:547:        meerschaum_connector::pipe_ops::get_dtypes(conn.as_ref(), pipe, pipe.parameters(), true)
crates/meerschaum-server/src/mcp/tools.rs:772:    if pipe.parameters().is_empty() {
crates/meerschaum-server/src/mcp/tools.rs:804:        let merged = meerschaum_core::value::apply_patch_maps(pipe.parameters(), &parameters);
crates/meerschaum-sql/src/queries/mod.rs:58:        // `pipe.parameters.get('schema', self.schema)`: an explicit `None` wins.
crates/meerschaum-sql/src/queries/mod.rs:59:        match pipe.param("schema") {
crates/meerschaum-sql/src/instance/data.rs:271:        if !self.table_exists(&sp.target, schema.as_deref()).await {
crates/meerschaum-sql/src/instance/data.rs:275:            self.get_reflected_columns_types(&sp.target, schema.as_deref())
crates/meerschaum-sql/src/instance/data.rs:278:            self.get_table_cols_types(&sp.target, schema.as_deref())
crates/meerschaum-sql/src/instance/data.rs:288:                .table_exists(&sp.target, self.get_pipe_schema(sp).as_deref())
crates/meerschaum-sql/src/instance/data.rs:321:            .get_table_cols_indices(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-sql/src/instance/data.rs:329:            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-sql/src/instance/data.rs:353:            if sp.param("precision").is_some_and(|p| p.truthy()) {
crates/meerschaum-sql/src/instance/data.rs:1011:                .param("fetch")
crates/meerschaum-sql/src/instance/data.rs:1177:        if self.table_exists(&sp.target, schema.as_deref()).await {
crates/meerschaum-sql/src/instance/data.rs:1190:            let _ = self.drop_geopackage_metadata(&sp.target).await;
crates/meerschaum-sql/src/instance/data.rs:1426:        let dedup_table = self.get_temporary_target(&sp.target, Some(&session_id), Some("dedup"));
crates/meerschaum-sql/src/instance/data.rs:1427:        let temp_old_table = self.get_temporary_target(&sp.target, Some(&session_id), Some("old"));
crates/meerschaum-sql/src/instance/data.rs:1445:            get_rename_table_queries(&sp.target, &temp_old_table, flavor, schema.as_deref(), None)
crates/meerschaum-sql/src/instance/data.rs:1449:            get_rename_table_queries(&dedup_table, &sp.target, flavor, None, schema.as_deref())
crates/meerschaum-sql/src/queries/partition.rs:251:        let full = sql_item_name(&pipe.target, flavor, schema.as_deref()).replace('\'', "''");
crates/meerschaum-sql/src/queries/partition.rs:259:        let clean_target = pipe.target.replace('\'', "''");
crates/meerschaum-sql/src/queries/partition.rs:265:        let func_name = partition_function_name(flavor, &pipe.target).replace('\'', "''");
crates/meerschaum-sql/src/queries/partition.rs:276:    let clean_target = pipe.target.replace('\'', "''");
crates/meerschaum-sql/src/queries/partition.rs:289:    let parent_name = sql_item_name(&pipe.target, ctx.flavor, schema.as_deref());
crates/meerschaum-sql/src/queries/partition.rs:304:    let parent_name = sql_item_name(&pipe.target, flavor, schema.as_deref());
crates/meerschaum-sql/src/queries/partition.rs:309:                &partition_name(flavor, &pipe.target, *lo),
crates/meerschaum-sql/src/queries/partition.rs:333:    let clean_target = pipe.target.replace('\'', "''");
crates/meerschaum-sql/src/queries/partition.rs:408:    let parent_name = sql_item_name(&pipe.target, flavor, ctx.get_pipe_schema(pipe).as_deref());
crates/meerschaum-sql/src/queries/partition.rs:414:        let part_name = sql_item_name(&partition_name(flavor, &pipe.target, lo), flavor, None);
crates/meerschaum-sql/src/queries/partition.rs:441:    let func_name = partition_function_name(flavor, &pipe.target);
crates/meerschaum-sql/src/queries/partition.rs:442:    let scheme_name = partition_scheme_name(flavor, &pipe.target);
crates/meerschaum-sql/src/queries/partition.rs:468:    let func_name = partition_function_name(Flavor::Mssql, &pipe.target).replace('\'', "''");
crates/meerschaum-sql/src/queries/partition.rs:512:    let func_item = sql_item_name(&partition_function_name(flavor, &pipe.target), flavor, None);
crates/meerschaum-sql/src/queries/partition.rs:513:    let scheme_item = sql_item_name(&partition_scheme_name(flavor, &pipe.target), flavor, None);
crates/meerschaum-sql/src/queries/partition.rs:543:    let func_name = partition_function_name(flavor, &pipe.target);
crates/meerschaum-sql/src/queries/partition.rs:544:    let scheme_name = partition_scheme_name(flavor, &pipe.target);
crates/meerschaum-sql/src/queries/partition.rs:568:        &pipe.target,
crates/meerschaum-server/src/openapi.rs:1080:        assert!(j["paths"]["/pipes/keys"]["get"]["parameters"].is_array());
crates/meerschaum-server/src/openapi.rs:1081:        assert!(j["paths"]["/version"]["get"]["parameters"][0]["name"] == "curr_user");
crates/meerschaum-server/src/state.rs:274:            b = b.parameters(p);
crates/meerschaum-server/src/state.rs:393:        let merged = meerschaum_core::value::apply_patch_maps(&params, pipe.parameters());
crates/meerschaum-server/src/state.rs:401:    match attrs.get("parameters")? {
crates/meerschaum-server/src/state.rs:430:    local.insert("parameters".into(), Value::Map(pipe.parameters().clone()));
crates/meerschaum-sql/src/queries/index.rs:25:        .param("index_template")
crates/meerschaum-sql/src/queries/index.rs:37:                ("target", &pipe.target),
crates/meerschaum-sql/src/queries/index.rs:122:        .unwrap_or_else(|| format!("IX_{}_unique", pipe.target));
crates/meerschaum-sql/src/queries/index.rs:172:    let primary_key_constraint_name = q(&format!("PK_{}", pipe.target));
crates/meerschaum-sql/src/queries/index.rs:202:    let pipe_name = sql_item_name(&pipe.target, flavor, pipe_schema.as_deref());
crates/meerschaum-sql/src/queries/index.rs:263:                        let new_table = format!("_new_{}", pipe.target);
crates/meerschaum-sql/src/queries/index.rs:290:                                &pipe.target,
crates/meerschaum-sql/src/queries/index.rs:528:    let pipe_name = sql_item_name(&pipe.target, flavor, schema.as_deref());
crates/meerschaum-sql/src/queries/index.rs:529:    let pipe_name_no_schema = sql_item_name(&pipe.target, flavor, None);
crates/meerschaum-sql/src/queries/index.rs:540:        let temp_table = format!("_{}_temp_migration", pipe.target);
crates/meerschaum-api/src/client/instance.rs:39:        let fetch = match pipe.parameters().get("fetch") {
crates/meerschaum-server/src/routes/pipes.rs:177:    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
crates/meerschaum-server/src/routes/pipes.rs:423:                Value::Map(h.pipe.parameters().clone())
crates/meerschaum-server/src/routes/pipes.rs:988:            h.pipe.parameters(),
crates/meerschaum-sql/src/instance/maintenance.rs:79:            .table_exists(&sp.target, self.get_pipe_schema(sp).as_deref())
crates/meerschaum-sql/src/instance/maintenance.rs:116:        if !pipe.param("compress").is_some_and(|v| v.truthy()) {
crates/meerschaum-sql/src/instance/maintenance.rs:128:            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-sql/src/instance/maintenance.rs:184:            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-sql/src/instance/maintenance.rs:259:            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-sql/src/instance/maintenance.rs:296:            if let Some(col) = self.geopackage_geometry_column(&sp.target).await {
crates/meerschaum-sql/src/instance/maintenance.rs:297:                let rtree = crate::instance::geopackage::rtree_name(&sp.target, &col);
crates/meerschaum-sql/src/instance/maintenance.rs:300:                        crate::instance::geopackage::get_rtree_create_queries(&sp.target, &col);
crates/meerschaum-sql/src/instance/maintenance.rs:326:            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
crates/meerschaum-api/src/client/pipes.rs:190:        match pipe.parameters().get("instance_keys") {
crates/meerschaum-api/src/client/pipes.rs:225:            return pipe.parameters().clone();
crates/meerschaum-api/src/client/pipes.rs:233:                        .get("parameters")
crates/meerschaum-api/src/client/pipes.rs:242:                    return pipe.parameters().clone();
crates/meerschaum-api/src/client/pipes.rs:246:        meerschaum_core::value::apply_patch_maps(&remote, pipe.parameters())
crates/meerschaum-api/src/client/pipes.rs:327:            body: Body::Json(py_json(&Value::Map(pipe.parameters().clone()))),
crates/meerschaum-api/src/client/pipes.rs:737:            Value::Map(m) if m.is_empty() || matches!(m.get("parameters"), Some(Value::Map(_))) => {
```
