//! Minimal source and in-memory instance, actions and hooks; all data is synthetic.
use meerschaum_plugin::{
    Error, Frame, Plugin, Reply, Request, Result,
    arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    serde_json::{Value, json},
};
use std::sync::Arc;
use std::sync::Mutex;
static DATA: Mutex<Option<Frame>> = Mutex::new(None);
#[derive(Default)]
struct Demo;
impl Plugin for Demo {
    const NAME: &'static str = "native_demo";
    const VERSION: &'static str = "1.0.0";
    fn manifest(&self) -> Value {
        json!({"schema_version":meerschaum_plugin::MANIFEST_SCHEMA_VERSION,"meerschaum_version":"5.0.0-rc.18","python_version":"","fingerprint":null,
          "plugins":{"native_demo":{"name":"native_demo","version":"1.0.0","data_functions":["fetch","sync","register"],"actions":[{"name":"hello","doc":"Return a synthetic greeting."},{"name":"panic","doc":"Exercise panic containment."}],"arguments":[{"option_strings":["--native-demo"],"action":"store_true","help":"Enable demo."}],"pre_sync_hooks":["before"],"post_sync_hooks":["after"],"connectors":["native_demo"],"api_plugins":["routes"],"web_pages":[{"path":"/dash/native-demo","page_group":"Native demo","page_key":"native-demo","login_required":true,"function":"page","mode":"html"}]}},
          "connector_types":{"native_demo":{"type":"native_demo","plugin":"native_demo","class_name":"Demo","is_instance":true,"is_thread_safe":false,"methods":["fetch","register","sync_pipe","get_pipe_data"]}},"pre_sync_hooks":["before"],"post_sync_hooks":["after"]})
    }
    fn fetch(&self, request: &Request) -> Result<Option<Frame>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        // `context.kwargs.rows` (a sync's extra keyword arguments) sets the row count.
        let ids = match request.context["kwargs"]["rows"].as_i64() {
            Some(rows) => (1..=rows).map(Some).collect(),
            None => vec![Some(1), None, Some(3)],
        };
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))])
            .map_err(|e| Error::Message(e.to_string()))?;
        Ok(Some(
            Frame::from_record_batch(&batch).map_err(|e| Error::Message(e.to_string()))?,
        ))
    }
    fn sync(&self, _: &Request, data: Option<Frame>) -> Result<Value> {
        *DATA.lock().unwrap() = data;
        Ok(json!([true, "Success"]))
    }
    fn call(&self, r: &Request, data: Option<Frame>) -> Result<Reply> {
        match r.method.as_str() {
            "fetch" => self.fetch(r).map(Reply::data),
            "sync" | "sync_pipe" => self.sync(r, data).map(Reply::json),
            "register" => Ok(Reply::json(
                json!({"columns": {"datetime": "id"}, "dtypes": {"id": "int"}}),
            )),
            "get_pipe_data" => Ok(Reply::data(DATA.lock().unwrap().clone())),
            "get_pipe_rowcount" => Ok(Reply::json(json!(
                DATA.lock().unwrap().as_ref().map(Frame::height)
            ))),
            "test_connection" | "pipe_exists" => Ok(Reply::json(json!(true))),
            "action" if r.context["name"] == "panic" => panic!("intentional demo panic"),
            "action" => Ok(Reply::json(json!([true, "Hello from native_demo"]))),
            "pre_sync_hook" | "post_sync_hook" => {
                Ok(Reply::json(json!([true, r.context["kwargs"].to_string()])))
            }
            "register_pipe" | "drop_pipe" => Ok(Reply::json(json!([true, "Success"]))),
            "get_pipe_attributes" => Ok(Reply::json(json!({"pipe_id": 1, "parameters": {}}))),
            "get_pipe_id" => Ok(Reply::json(json!(1))),
            "get_sync_time" => Ok(Reply::json(json!(3))),
            "get_pipe_columns_types" => Ok(Reply::json(json!({"id":"BIGINT"}))),
            "fetch_pipes_keys" => Ok(Reply::json(json!([[
                "native_demo:source",
                "plugin_abi",
                null
            ]]))),
            "echo" => Ok(Reply::json(r.context.clone())),
            "routes" => Ok(Reply::json(json!([
                {"method": "GET", "path": "/native-demo/ping", "public": true},
                {"method": "POST", "path": "/native-demo/echo"}
            ]))),
            "route" => Ok(Reply::json(json!({
                "status": 200,
                "content_type": "application/json",
                "body": json!({"path": r.context["path"], "method": r.context["method"],
                    "body": r.context["body"], "user": r.context["user"]}).to_string()
            }))),
            "page" => Ok(Reply::json(json!({
                "html": format!("<p>Hello from the native demo page, {}.</p>", r.context["username"].as_str().unwrap_or("guest"))
            }))),
            _ => Err(Error::Unsupported(format!(
                "unsupported method {}",
                r.method
            ))),
        }
    }
}
meerschaum_plugin::export_plugin!(Demo);
