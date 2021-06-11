<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# Building a pipe

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

    3. Answer `y` to edit the definition.  

        > Vim or your editor will open the pipe's properties file. Press `:q` and `[Enter]` to quit editing.

    4. Answer `y` to sync the pipe.  

        > The plugin will ask for weather stations. Enter `KATL` for Atlanta.

4. Sync new data  

    ```bash
    sync pipes
    ```

5. Sync pipes with the metric `weather`

    ```bash
    sync pipes -m weather
    ```

6. Sync continuously

    ```bash
    sync pipes --loop
    ```

7. Sync continuously in the background

    ```bash
    sync pipes --loop -d
    show jobs
    ```

8. Monitor the background job

    ```bash
    show logs
    ```

9. Delete the background job

    ```bash
    delete jobs
    ```

<asciinema-player src="/assets/casts/bootstrap-noaa.cast" size="small" preload="true"></asciinema-player>
