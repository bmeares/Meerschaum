<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🏗️ Building a Pipe

Pipes fetch and sync data from [connectors](/reference/connectors/). For this example, we will be using the connector `plugin:noaa` from the [data plugin](/reference/plugins/types-of-plugins/#data-plugins) `noaa`. Type the commands below to bootstrap an example pipe.

See the [bootstrapping reference page](/reference/pipes/bootstrapping/) for further information.

1. Launch the Meerschaum shell.  

    ```bash
    mrsm
    ```

2. Install the `noaa` plugin.

    ```bash
    install plugin noaa
    ```

3. Bootstrap the pipe.  

    ```bash
    bootstrap pipe
    ```

    1. Choose `plugin:noaa` for the connector, `weather` for the metric, and skip the location.

    2. Enter `timestamp` for the datetime and `station` for the ID.

        > The plugin will ask for weather stations. Enter `KATL` for Atlanta.

    3. Answer `y` to sync the pipe.


!!! success "Success! Now what?"

    Well done, you've built your first pipe! Check out [all of the commands](/reference/actions) you have at your disposal.

    For now, here are some useful commands to manage your pipe:

    | Command                           | Description                                                                                        |
    |-----------------------------------|----------------------------------------------------------------------------------------------------|
    | `show pipes`                      | Print the pipes on this instance.                                                                  |
    | `show data`                       | Print a preview of the contents of your pipes.                                                     |
    | `sync pipes`                      | Sync new rows into your pipes.                                                                     |
    | `sync pipes -m weather --loop -d` | Continuously (`--loop`) sync new rows into weather pipes (`-m`), and run in the background (`-d`). |
    | `show jobs`                       | Print the background jobs (created with `-d`).                                                     |
    | `stop jobs`                       | Stop the running background jobs.                                                                  |

<asciinema-player src="/assets/casts/bootstrap-noaa.cast" size="small" preload="true"></asciinema-player>
