<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# Starting the Stack

The Meerschaum stack is an integrated collection of services designed to help you start visualizing your data as quickly as possible.

To bring up the database and Grafana parts of the stack, run the following command:

```bash
mrsm stack up -d db grafana
```

<asciinema-player src="/assets/casts/stack.cast" autoplay="true" loop="true" size="small" preload="true" rows="10"></asciinema-player>

!!! note ""
    The `stack` command is a wrapper around a pre-configured [`docker-compose`](https://docs.docker.com/compose/) project. Don't worry if you don't have `docker-compose` installed; in case it's missing, Meerschaum will automatically install it within a virtual environment for its own use.
    

    Refer to the [`docker-compose` overview page](https://docs.docker.com/compose/reference/overview/) to see the available `stack` commands.

![Grafana pre-configured with Meerschaum](https://imgur.com/cYTfiFT.png){ align=right }

![Example Grafana Dashboard](grafana-dashboard.png){ align=left }

!!!info ""
    Grafana is included in the Meerschaum stack, pre-configured with the Meerschaum TimescaleDB database.
    
    Open a web browser and navigate to [http://localhost:3000](http://localhost:3000) where you can log into Grafana with username `admin`, password `admin`.

If you want to stop all the services in the stack, run the stack command with `down`:

```bash
mrsm stack down
```

To remove all services in the stack and delete all data, use the `-v` flag:

```bash
mrsm stack down -v
```

<asciinema-player src="/assets/casts/stack-down.cast" autoplay="true" loop="true" size="small" preload="true" rows="12"></asciinema-player>

!!! warning "Data Loss Warning"
    The `-v` flag in `stack down -v` will delete ALL volumes in the stack. That includes pipes' data!
    
    To delete a specific service's volume, run the command `docker volume rm`, e.g. to delete just Grafana's data:
    ```bash
    docker volume rm mrsm_grafana_storage
    ```
