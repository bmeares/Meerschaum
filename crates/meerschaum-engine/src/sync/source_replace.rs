//! Bounded registered-source replacement before ordinary registration or dtype persistence.
use super::*;

pub(super) async fn run(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    instance: &dyn InstanceConnector,
    source: &dyn Connector,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    if pipe.temporary {
        return fail("Bounded source replacement requires a registered destination.");
    }
    match instance.get_pipe_id_checked(pipe).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return fail("Bounded source replacement requires an existing registered destination.");
        }
        Err(error) => return fail(error.to_string()),
    }
    if observer.is_some_and(|observer| !observer.supports_source_replace_summary()) {
        return fail("This observer does not support bounded source replacement summaries.");
    }
    let Some(transport) = source.source_replace_transport() else {
        return unsupported(pipe, source);
    };
    // These are reads only. The Main broker owns canonical registration, schema planning,
    // dtype persistence and the entire replacement transaction after preparation succeeds.
    let dtypes = match pipe_ops::get_dtypes(instance, pipe, pipe.parameters(), true).await {
        Ok(dtypes) => dtypes,
        Err(error) => return fail(error.to_string()),
    };
    let parse = |bound| parse_date_bound(env, pipe, &dtypes, bound);
    let end = match args.end.clone().map(parse).transpose() {
        Ok(end) => end,
        Err(error) => return fail(error),
    };
    let begin_arg = match &args.begin {
        SyncBegin::At(bound) => match parse(bound.clone()) {
            Ok(bound) => SyncBegin::At(bound),
            Err(error) => return fail(error),
        },
        other => other.clone(),
    };
    let begin = match determine_begin(
        env,
        instance,
        pipe,
        &dtypes,
        &begin_arg,
        end.as_ref(),
        args.check_existing,
    )
    .await
    {
        Ok(begin) => begin,
        Err(error) => return fetch_failed(pipe, &source.connector_keys(), &error),
    };
    let query = DataQuery {
        begin,
        end,
        params: args.params.clone(),
        ..Default::default()
    };
    let mut extra = args.extra.clone();
    extra.insert("enforce_dtypes".into(), Value::Bool(args.enforce_dtypes));
    let options = SyncOptions {
        replace: true,
        check_existing: false,
        chunksize: args.chunksize,
        begin: query.begin.clone(),
        end: query.end.clone(),
        params: query.params.clone(),
        extra,
    };
    let mut prepared = match instance
        .prepare_registered_source_replace(pipe, &transport, &query, &options)
        .await
    {
        Ok(Some(prepared)) => prepared,
        Ok(None) => return unsupported(pipe, source),
        Err(error) => return fetch_failed(pipe, &source.connector_keys(), &error),
    };
    let summary = prepared.summary().clone();
    if let Some(observer) = observer
        && let Err(error) = observer.before_source_replace(pipe, &summary).await
    {
        let _ = prepared.abandon().await;
        return fail(error);
    }
    // Every retry retains this exact lease. A lost successful response may resolve only
    // through its completed receipt; it must never prepare another snapshot or repeat DELETE.
    let mut attempt = 1;
    let mut result = loop {
        let result = match meerschaum_connector::batch_delivery::destination_write(
            env.delivery.as_ref(),
            prepared.commit(),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => fail(error.to_string()),
        };
        attempt += 1;
        if result.success || env.issued_batch() || !args.force || attempt > args.retries {
            break result;
        }
        tokio::time::sleep(Duration::from_secs_f64(args.min_seconds.max(0.0))).await;
    };
    if !result.success {
        let _ = prepared.abandon().await;
        return result;
    }
    if let Some(observer) = observer
        && let Err(error) = observer.after_commit(pipe, &result).await
    {
        if env.issued_batch() {
            result.success = false;
            result.message = error;
            return result;
        }
        return fail(error);
    }
    result.end = args.end.as_ref().and_then(date_bound_to_bound);
    if let SyncBegin::At(begin) = &args.begin {
        result.begin = date_bound_to_bound(begin);
    }
    if observer.is_none()
        && result.rows_written() + result.deleted > 0
        && let Some(axis) = node_axis(env, pipe)
    {
        let positions = [&summary.first, &summary.last]
            .into_iter()
            .flatten()
            .filter_map(|bound| date_bound_to_bound(bound).and_then(|bound| axis.pos(&bound)))
            .collect();
        result.changed_intervals = replaced_intervals(
            &axis,
            positions,
            summary.begin.as_ref(),
            summary.end.as_ref(),
        )
        .into_iter()
        .map(|interval| meerschaum_core::success::ChangedInterval {
            begin: interval.begin.and_then(|begin| axis.bound(begin)),
            end: interval.end.and_then(|end| axis.bound(end)),
        })
        .collect();
    }
    result
}

fn unsupported(pipe: &Pipe, source: &dyn Connector) -> SyncResult {
    fetch_failed(
        pipe,
        &source.connector_keys(),
        &ConnectorError::NotImplemented(
            source.fetch_replace_refusal().unwrap_or(
                "Bounded source replacement requires aliases of the same Main and sealed credential provider.",
            ).into(),
        ),
    )
}
