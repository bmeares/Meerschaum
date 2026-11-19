//! Large, bounded declarations for exercising the real typed metadata transport.

pub fn source(non_bmp: bool) -> (String, usize) {
    let (description, count) = if non_bmp {
        ("é😀\"\\".repeat(1200), 171)
    } else {
        ("\"\\".repeat(4990), 103)
    };
    let description = serde_json::to_string(&description).unwrap();
    let kind = if non_bmp { "unicode" } else { "ascii" };
    (
        format!(
            r#"
from meerschaum.plugins import make_action
for index in range({count}):
    def payload_action(action=None, **kw):
        raise AssertionError('Discovery must never invoke an action')
    payload_action.__name__ = 'catalog_payload_{kind}_' + str(index).zfill(3)
    payload_action.__doc__ = {description}
    globals()[payload_action.__name__] = make_action(payload_action)
del payload_action
"#
        ),
        count,
    )
}
