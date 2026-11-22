//! Printing registered Pipelines: the definitions grid and the summary of a preview.

use super::*;

pub(super) fn show_grid(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    definitions: &[Definition],
    rows: &[Json],
) {
    let text = rt.with_formatter(|formatter| {
        use crate::format::{
            PipeKeys,
            rich::{
                table::{Column, SIMPLE, Table},
                text::Text,
            },
        };
        let mut table = Table::new();
        table.expand = true;
        table.box_chars = if formatter.settings.unicode {
            Some(SIMPLE)
        } else {
            None
        };
        for label in ["Pipeline", "Members", "Latest run"] {
            table.add_column(Column {
                header: Text::plain_text(label),
                ..Default::default()
            });
        }
        for (definition, row) in definitions.iter().zip(rows) {
            let mut label = formatter.pipeline_repr_text(&definition.name, &instance.keys());
            if !definition.description.is_empty() {
                label.append_text(&Text::plain_text(&format!("\n{}", definition.description)));
            }
            let mut members = Text::plain_text(&format!(
                "{} · {}\n",
                count_noun(definition.targets.len(), "member"),
                count_noun(
                    row["instance_count"].as_u64().unwrap_or_default() as usize,
                    "instance"
                ),
            ));
            if definition.targets.is_empty() {
                members.append_text(&Text::plain_text("No member pipes."));
            }
            for reference in &definition.targets {
                let parts: Vec<_> = reference.splitn(4, '|').collect();
                if parts.len() != 4 {
                    continue;
                }
                let keys = PipeKeys {
                    connector: parts[1].into(),
                    metric: parts[2].into(),
                    location: if parts[3] == "None" {
                        None
                    } else {
                        Some(parts[3].into())
                    },
                    instance: Some(parts[0].into()),
                };
                members.append_text(&formatter.pipe_repr_text(&keys));
                members.append_text(&Text::plain_text("\n"));
            }
            let status = row["latest_run"]["status"]
                .as_str()
                .unwrap_or("No runs yet");
            let (symbol, color) = match status {
                "running" => ("▶", "cyan"),
                "succeeded" => ("✔", "green"),
                "failed" => ("✘", "red"),
                _ => ("○", "dim"),
            };
            let status = format!(
                "{} {status}",
                if formatter.settings.unicode {
                    symbol
                } else {
                    "•"
                }
            );
            table.add_row(vec![
                Some(Box::new(label)),
                Some(Box::new(members)),
                Some(Box::new(Text::new(
                    &status,
                    if formatter.settings.ansi { color } else { "" },
                ))),
            ]);
        }
        formatter.console().print(&table)
    });
    rt.print(&text);
}

/// `"1 member"` / `"2 members"`.
fn count_noun(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

pub(super) fn show_summary(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    definition: &Definition,
    row: &Json,
    args: &ArgsMap,
    detail: bool,
) {
    let _ = args;
    let status = row["latest_run"]["status"]
        .as_str()
        .unwrap_or("No runs yet");
    let text = rt.with_formatter(|formatter| {
        use crate::format::rich::text::Text;
        let symbol = match (status, formatter.settings.unicode) {
            ("running", true) => "▶",
            ("succeeded", true) => "✔",
            ("failed", true) => "✘",
            ("running", false) => ">",
            ("succeeded", false) => "+",
            ("failed", false) => "x",
            (_, true) => "○",
            _ => "-",
        };
        let color = if formatter.settings.ansi {
            match status {
                "succeeded" => "green",
                "failed" => "red",
                "running" => "cyan",
                _ => "dim",
            }
        } else {
            ""
        };
        let mut text = Text::plain_text(&format!("{symbol} "));
        if !color.is_empty() {
            text.stylize(color, 0, None);
        }
        text.append_text(&formatter.pipeline_repr_text(&definition.name, &instance.keys()));
        text.append_text(&Text::plain_text(&format!(
            "  · {} · {} · ",
            count_noun(definition.targets.len(), "member"),
            count_noun(
                row["instance_count"].as_u64().unwrap_or_default() as usize,
                "instance"
            ),
        )));
        text.append_text(&Text::new(status, color));
        formatter.text_to_str(&text)
    });
    rt.print(&format!("{text}\n"));
    if !definition.description.is_empty() {
        rt.print(&format!("  {}\n", definition.description));
    }
    if detail {
        rt.print(&format!(
            "  ID: {}\n  Revision: {}\n  Parents: {} | Children: {}\n",
            definition.id, definition.revision, definition.parents, definition.children
        ));
        for reference in &definition.targets {
            rt.print(&format!("  • {reference}\n"));
        }
    }
    if definition.targets.is_empty() {
        rt.print("  No member pipes.\n");
    }
    if let Some(message) = row["status_error"].as_str() {
        rt.print(&format!("  Run history unavailable: {message}\n"));
    }
}
