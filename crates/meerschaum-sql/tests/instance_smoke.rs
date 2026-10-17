mod common;
use common::*;
use meerschaum_connector::DataQuery;

flavor_tests!(smoke_drop_and_sync, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "drop_sync",
        None,
        r#"{"columns": {"datetime": "datetime", "id": "id"}}"#,
    );
    assert!(!c.conn.pipe_exists(&pipe).await.unwrap());
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("datetime", ndt(2021, 1, 1, 12, 0)),
            ("id", i(1)),
            ("val", i(1)),
        ])],
    )
    .await);
    assert!(c.conn.pipe_exists(&pipe).await.unwrap());
    let r = ok(c
        .sync(
            &pipe,
            vec![doc(vec![
                ("datetime", ndt(2021, 1, 1, 12, 1)),
                ("id", i(1)),
                ("val", i(1)),
            ])],
        )
        .await);
    eprintln!("{}", r.message);
    let df = c.get_data(&pipe, DataQuery::default()).await.unwrap();
    assert_eq!(df.height(), 2);
    let r = ok(c
        .sync(
            &pipe,
            vec![doc(vec![
                ("datetime", ndt(2021, 1, 1, 12, 1)),
                ("id", i(1)),
                ("val", i(2)),
            ])],
        )
        .await);
    assert_eq!(r.message, "Inserted 0, updated 1 rows.");
    let docs = c.docs(&pipe).await;
    eprintln!("{docs:?}");
    assert_eq!(as_int(&docs[1]["val"]), Some(2));
    c.delete(&pipe).await;
});
