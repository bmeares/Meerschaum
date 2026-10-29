# Automatic Runs

Clicking **Sync** on a Pipe or **Run** on a Pipeline starts a temporary worker. The console lists it under **Runs**, keeping one-off work out of persistent Jobs. A manually created one-shot Job stays in Jobs.

Choose **Keep as Job** after an automatic execution finishes to make it persistent. Repeating this request is safe.

## Output retention

| Record | Default retention |
|---|---|
| Successful automatic output | 24 hours |
| Failed or unknown-result output | 7 days |
| Archived summary | 30 days |

Active and paused work do not expire. After output expires, the summary keeps the result and run identity and labels the missing output. Cleanup runs at startup and periodically.

??? example "Change retention"

    Durations are in seconds:

    ```yaml
    api:
      execute:
        retention:
          success_seconds: 86400
          failure_seconds: 604800
          summary_seconds: 2592000
    ```

These settings govern temporary workers and their output, separately from [engine history](../pipelines/state.md). [Job access rules](access.md) apply to their logs.
