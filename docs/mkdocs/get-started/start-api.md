# 🖥️ Meerschaum Web Console

You can use Meerschaum from its web interface. From within a `mrsm` shell, type to commands below to see how:

1. Start the Web API server.

    ```bash
    start api
    ```

    ??? tip "Change the port number"
        To change the port of the API server, add the flag `--port` or `-p`:

        ```bash
        start api --port 8001
        ```

2. Visit port 8000 in a web browser (e.g. [http://localhost:8000](http://localhost:8000)) and create an account or login to your instance.

    ??? tip "Register from the command-line"

        You can create users on your instance with the `register user` command.
        ```bash
        register user myuser
        ```

    <img src="/assets/screenshots/login-page.png"/>

3. On the left side of the dashboard are the available commands, like in the `mrsm` shell.

    <img src="/assets/screenshots/dash-bootstrap.png"/>

    !!! tip "Adding custom commands"
        You can add your own actions with [Meerschaum plugins](/reference/plugins/writing-plugins/#action-plugins).

    <img src="/assets/screenshots/dash-pipe-statistics.png"/>

    Also like the `mrsm` shell, the web console lets you control pipes from several instances (note the instances drop-down on the top right).

# Conclusion

That's all for now! Meerschaum has many other quality of life features, like [integrating with data science tools](/reference/data-analysis-tools/), [running background jobs](/reference/background-jobs/), [adding custom plugins](/reference/plugins/types-of-plugins/), and a whole lot more. Continue on to the [reference wiki](/reference/pipes/) if you'd like to keep reading. Thanks for trying out Meerschaum!
