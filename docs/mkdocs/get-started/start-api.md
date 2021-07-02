# Meerschaum Web Server

You can use Meerschaum from its web interface. From within a `mrsm` shell, type to commands below to see how:

## Web Interface

1. Register a new user (`myuser`) on your instance.

    ```bash
    register user myuser
    ```

2. Start the API server.

    ```bash
    start api
    ```

3. In a web browser, visit the IP address of your server followed by `:8000`, e.g. [http://localhost:8000](http://localhost:8000). Enter the username and password you just created, then click Login.

    ??? tip "Change the port number"
        To change the port of the API server, add the flag `--port` or `-p`:

        ```bash
        start api --port 8001
        ```

    <img src="/assets/screenshots/login-page.png"/>

4. On the left side of the dashboard are the available Meerschaum commands, just like in the `mrsm` shell.

    <img src="/assets/screenshots/dash.png"/>

    !!! tip "Adding custom commands"
        You can add your own actions with [Meerschaum plugins](/reference/plugins/writing-plugins/#action-plugins).

## Web API

1. Visit the address of the server mentioned above, and add `/docs`, e.g. [http://localhost:8000/docs](http://localhost:8000/docs):

    <img src="/assets/screenshots/api-docs.png"/>

2. Click the green Authorize button on the top left, and enter the credentials you made earlier:

    <img src="/assets/screenshots/api-login.png"/>

3. Scroll to the section for the endpoint `/pipes/{connector_keys}/{metric_key}/{location_key}/data` and enter the keys `plugin:noaa`, `weather`, and `ATL` for the pipe you [previouosly bootstrapped](/get-started/bootstrap/). You may also enter beginning and end datetimes if you like.

    <img src="/assets/screenshots/api-data-params.png"/>  

4. Click the blue Execute button and scroll down to see the JSON response.

    <img src="/assets/screenshots/api-data-response.png"/>


# Conclusion

That's all for now! Meerschaum has many other quality of life features, like [integrating with data science tools](/reference/data-analysis-tools/), [running background jobs](/reference/background-jobs/), [adding custom plugins](/reference/plugins/types-of-plugins/), and a whole lot more. Continue on to the [reference wiki](/reference/pipes/) if you'd like to keep reading. Thanks for trying out Meerschaum!
