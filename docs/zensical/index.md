---
hide:
  - navigation
  - toc
---

<div class="mrsm-hero" markdown>

<p class="mrsm-eyebrow">MEERSCHAUM 5 · DOCUMENTATION PREVIEW</p>

# Keep your data flowing.

Turn a query or a Python function into a **Pipe**. Sync new data, connect your transformations, and see what happened — from your terminal, Python, or the web.

[Build your first Pipe](get-started/){ .md-button .md-button--primary }
[Explore what's new](news/v5/){ .md-button }

<p class="mrsm-hero-note">Start with a local SQLite file. Add a database or a plugin when you need one.</p>

</div>

<div class="mrsm-demo" markdown>

<p class="mrsm-eyebrow">ONE PIPE. TWO SYNCS. NO DUPLICATES.</p>

<asciinema-player src="/assets/casts/v5-first-pipe.cast" size="small" preload="true" rows="15" title="Sync a Pipe, inspect the data, then sync again"></asciinema-player>

<p class="mrsm-caption">A real run of the <a href="get-started/bootstrap/">first-Pipe example</a>, recorded with 5.0.0rc15. Play it, then try it yourself.</p>

</div>

<div class="mrsm-card-grid" markdown>

<div class="mrsm-card" markdown>

### A small idea that grows

A Pipe remembers where its data belongs and how to fetch more. Keep the SQL and Python you already use.

[Meet Pipes →](reference/pipes/)

</div>

<div class="mrsm-card" markdown>

### From steps to a Pipeline

Connect transformations, preview the work, and run parents before their children. Save the whole workflow under a name.

[Build a Pipeline →](get-started/pipelines/)

</div>

<div class="mrsm-card" markdown>

### A clear view of your work

Browse data, edit a Pipe, inspect a Pipeline, and follow runs in the web console. Share access with your team.

[Open the console →](get-started/start-api/)

</div>

</div>

## Familiar tools. A faster foundation.

Meerschaum 5 brings a Rust core, Polars DataFrames, and native database connectors to the Python and command-line workflow. Use [pandas](reference/pipes/reading/#dataframe-library) too, or return plain Python dictionaries with no DataFrame dependency.

[See the measured v3 / v4 / v5 comparison →](reference/performance/)

## Take the next step

[Install](get-started/) → [Make a Pipe](get-started/bootstrap/) → [Sync new rows](get-started/incremental-sync/) → [Connect a Pipeline](get-started/pipelines/).

Already using Meerschaum? Start with [Migrating to v5](reference/migrating-to-v5/).
