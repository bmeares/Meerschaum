# 🌐 API Instance

The Meerschaum API instance (and [Web Console](/get-started/start-api/)) lets you manage [pipes](/reference/pipes/), [jobs](/reference/background-jobs/), and [connectors](/reference/connectors/) over the Internet. Start the API server with the [stack](/reference/stack/) or via `start api`; see [Deployment](#deployment) below for ways to run Meerschaum in production.

## Usage

Starting the Meerschaum API server lets you treat your host as the following:

- [API instance](/reference/connectors/#instances-and-repositories)  
  For managing pipes.
- [Plugin repository](/reference/plugins/repositories/)  
  For storing plugins.
- [Remote executor](/reference/background-jobs/#executors)  
  For running jobs.

### Adding Connectors

Add new `api` connectors with `bootstrap connector` or declaring an [environment connector](/reference/connectors/#environment-connectors):

```bash
# URI is in the form 'protocol://user:pass@host:port'
export MRSM_API_FOO='https://foo:bar@mrsm.example.com'
```

### Remote Actions

Add `-e` to client commands to [execute them remotely](/reference/background-jobs/#executors) on your API instance:

```
sync pipes -e api:main
```

When running the `mrsm` shell, the `executor` command allows you to temporarily run all the commands in your session remotely (like the `instance` command):

```
executor api:main
```

### API Documentation

You can visit your API instance's Swagger documentation by navigating to `/docs` or `/redoc`. Every route is listed there with the scopes it needs. Saved Pipelines, their runs and the console's actions have routes of their own, which answer `{"success": ..., "message": ..., ...}` with real status codes: see [Pipelines, Runs and Actions over HTTP](/reference/api-instance/routes/).

### Permissions and Rate Limits

Which routes a server serves, who may run actions and how often a client may call are settings under `api:permissions`: see [Permissions and Rate Limits](/reference/api-instance/permissions/). To publish only plugin routes and plugin pages (a public dashboard), use the [public preset](/reference/api-instance/permissions/#the-public-preset):

```
mrsm start api --public
```

## Deployment

It's common in production deployments to stand up a public-facing API instance. See below for deployment tips:

!!! danger "⚠️ Open registration plus terminals for non-admins is a shell for anyone"

    With the defaults, anyone who can reach the API can register an account (`api:permissions:registration:users`) and, as a logged-in user, open the web terminal (`api:permissions:actions:non_admin`): **a shell on the server for anyone who can reach the port**. Meerschaum 4 behaves the same, and v5 keeps it. Before exposing an API, start it with `--secure` (terminals and actions for administrators only) or `--private` (authentication on every endpoint), or set `api:permissions:registration:users: false` (no self-registration) or `api:permissions:actions:non_admin: false` (administrators only). `mrsm start api` prints a warning at startup when it binds a non-loopback address with both settings on; it cannot see a reverse proxy, so a loopback bind behind a proxy exposes the same terminal **without** the warning. (`--no-auth` on a non-loopback address also prints a warning: every route, `/actions` included, is then open to anyone who can connect.)


### Meerschaum Stack (easiest)

If you have [Docker](https://www.docker.com/get-started/) installed, the most straightforward way to deploy is via the [Meerschaum stack](/reference/stack/):

```
mrsm stack up -d
```

This starts an internally-managed, pre-configured Docker Compose project with an `api` service that runs on [http://localhost:8000](http://localhost:8000).

The environment in the `api` service is updated such that [connectors](/reference/connectors/) from your host are passed to the container.

!!! tip "Want to manage Docker yourself?"
    You can run the [`bmeares/meerschaum` Docker image](https://hub.docker.com/r/bmeares/meerschaum) if you'd like to add Meerschaum to another Docker Compose project. You may also edit the `stack` Docker Compose project with `mrsm edit config stack` under the key `docker-compose.yaml`.

### Bare Metal

If you don't want to deploy with Docker, you can run an API instance on your host directly:

!!! note inline end ""
    The `--production` flag stops serving the interactive API docs (`/docs`, `/redoc`) unless `api:endpoints:docs_in_production` is set.

```
mrsm start api --production
```

For example, this is a common pattern for [standing up a `systemd` service](/reference/background-jobs/) to start the API server:

```
mrsm start api --production --name api-instance -d
```

### TLS / SSL / HTTPS

The API server speaks plain HTTP. To serve HTTPS, put a reverse proxy that terminates TLS in front of it, such as Caddy or NGINX. See the [example NGINX configuration](/reference/api-instance/nginx/) for a proxy pass that adds HTTPS to your API instance via Let's Encrypt. Behind a proxy, set `api:server:forwarded_allow_ips` and `api:server:proxy_headers` so that the server sees each client's address (see [Permissions](/reference/api-instance/permissions/)).

`mrsm start api` stops with an error when given `--keyfile` or `--certfile`, or when `api:server` (or `api:uvicorn`) sets `ssl_keyfile` or `ssl_certfile`, rather than serving plain HTTP to clients that expect HTTPS.

### Docker

If you're building your own Docker images, consider basing off the [`bmeares/meerschaum` Docker image](https://hub.docker.com/r/bmeares/meerschaum), as is done in the [`mrsm compose` template repository](https://github.com/bmeares/mrsm-compose-template/blob/main/docker/Dockerfile):

```docker
FROM bmeares/meerschaum

COPY --chown=meerschaum:meerschaum ./ /app
WORKDIR /app
RUN mrsm compose init

ENTRYPOINT ["/app/docker/bootstrap.sh"]
```

This image comes pre-installed with common database drivers and package dependencies. Note this image is configured to run as a non-privileged user `meerschaum`.

Of course, you can always `pip install` Meerschaum into your own custom image:

```docker
FROM python

# Install dependencies into the virtual environment:
RUN pip install meerschaum && mrsm upgrade packages api -y

# or to install dependencies globally:
# RUN pip install meerschaum[api]

ENTRYPOINT ["python", "-m", "meerschaum"]
```

### AWS

Run the [`bmeares/meerschaum` Docker image](https://hub.docker.com/r/bmeares/meerschaum) with ECS or on an EC2 like you would any other Docker container. See the [Docker section above](#docker) if you want to build your own images.

### Helm Chart (k8s)

If you are running Kubernetes, consider the [Meerschaum Helm chart on Artifact Hub](https://artifacthub.io/packages/helm/meerschaum/meerschaum). It's configured to run the [`bmeares/meerschaum` Docker Image](https://hub.docker.com/r/bmeares/meerschaum) for k8s environments.
