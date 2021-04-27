# Frequently Asked Questions

Below is a list of common questions and issues you may encounter. If you have suggestions, please join the [FAQ Suggestions discussion on the GitHub repository](https://github.com/bmeares/Meerschaum/discussions/46)!

## I can't open the Meerschaum shell!
You can invoke `mrsm` directly with `python -m meerschaum`. Check that your `PATH` includes packages installed by `pip`, such as `~/.local/bin`.

## How do I turn off the emoji and/or colors?
The color and emoji configuration settings are contained in the `system` config file under the `formatting` section, which you can access with `edit config system`.

Alternatively, you can install the `color` plugin which will toggle these values for you. The `color` command will toggle `unicode` and `ansi` settings by default, or you can specify `color unicode` or `color ansi` if you want to toggle specific values.
```
install plugins color
color
```
Note that disabling ANSI may not completely elimate ANSI characters.

## Can I use Meerschaum in my scripts?
Yes, although some commands like `bootstrap` and `edit` are interactive and not safe for scripting, most commands are scriptable.

When executing Meerschaum actions in a script, it's a good idea to add `--nopretty` and `--noask` flags, or `--yes` or `--force` to agree to confirmation dialogues.

The flag `--noask` will choose the default values for questions (e.g. skipping deleting to prevent data loss) and `--yes` of `--force` will choose `yes` to agree to all dialogues.

The `--nopretty` flag may not have 100% coverage, but in cases like `show pipes` or `show columns`, it will instead print JSON representations of the pipes and data (separated by newlines) rather than formatting them.

If you are planning on integrating Meerschaum into your Python scripts, you can access actions directly via the `meerschaum.actions.actions` dictionary. Please consult [the Python package documentation](https://docs.meerschaum.io) for more information. Also, you might want to consider making your script into a [Meerschaum action by writing an action plugin](/reference/plugins/types-of-plugins/#action-plugins).

## Connectors don't work for `<database flavor>`!
Although Connectors *should* work with any database flavor supported by `sqlalchemy` Engines, it is difficult to test against many database flavors. When bugs are encountered, please open an issue and describe your configuration!

In cases like Microsoft SQL server or Oracle SQL, make sure you have the appropriate drivers installed:

  - [Microsoft SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15)
  - [Oracle SQL](https://docs.oracle.com/en/database/oracle/oracle-database/19/cwlin/installing-odbc-drivers-for-linux-x86-64.html)


## How do I completely uninstall Meerschaum?
- Take down your stack and delete its data with `mrsm stack down -v`.
    - You might also want to delete `bmeares/meerschaum` and other images with `docker image rm`.
- Uninstall via `pip`: `pip uninstall meerschaum`.
- Delete the folder `~/.config/meerschaum/` (`%APPDATA%\Meerschaum\` on Windows).