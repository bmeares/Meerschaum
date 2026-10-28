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

On a new instance, the first account becomes an administrator. Create that account before opening the instance to visitors. Self-registration then closes unless you set `api:permissions:registration:users: true`. New users are Editors by default: they may work with pipes, but actions and the web terminal need the Operator role (`actions:execute` and `webterm:use`). Administrators manage users, roles and teams in the console's **Users & Teams** page.

`mrsm start api --secure` restricts actions and terminals to administrators; `--private` requires authentication on every endpoint. Enabling registration with Operator as the default role lets any visitor register and open a shell on this server. See [roles, teams and permissions](/reference/api-instance/permissions/#roles-and-teams) and [HTTPS](#tls-ssl-https).

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

Give `start api` a private key and its certificate, and the API server speaks HTTPS itself (HTTP/2 and HTTP/1.1):

```bash
mrsm start api --production \
  --keyfile /etc/meerschaum/tls/privkey.pem \
  --certfile /etc/meerschaum/tls/fullchain.pem
```

Or set the files in the config (`mrsm edit config api`), so every `start api` (and the background job of `start api -d`) uses them:

```yaml
api:
  server:
    ssl_keyfile: /etc/meerschaum/tls/privkey.pem
    ssl_certfile: /etc/meerschaum/tls/fullchain.pem
```

The server prints where it serves, for example `Serving HTTPS on https://mrsm.example.com:8000 (certificate for mrsm.example.com, valid until 2027-01-25).` Without a key and a certificate it speaks plain HTTP, as always.

- **The files** are PEM. The key may be unencrypted PKCS#8, PKCS#1 (RSA) or SEC1 (EC). The certificate file holds the server's certificate first, then its intermediates, as Let's Encrypt's `fullchain.pem` does. `start api` checks both before it starts and stops with the reason when something is off: a key without a certificate (or the reverse), a file it cannot read, swapped files, a key that does not belong to the certificate.
- **Renewals need no restart.** The server reads both files again every 30 seconds and on `SIGHUP` (`kill -HUP <pid>`), and new connections get the renewed certificate. When the new files do not load yet (a renewal caught between writing the key and the certificate), it keeps serving the previous certificate and says why if the problem persists. (So with HTTPS, `SIGHUP` reloads instead of stopping the server: stop it with Ctrl-C, `SIGTERM` or `mrsm stop jobs`.)
- **Plain HTTP on the HTTPS port** gets `/healthcheck` answered, so container healthchecks such as `curl --fail http://localhost:8000/healthcheck` keep working, and a redirect to `https://` for everything else.
- **Clients.** Point `api` connectors at the server with `https`, e.g. `MRSM_API_MAIN='https://user:pass@mrsm.example.com:8000'` or `protocol: https`. For a self-signed certificate, add its CA to the client's trust store, or set `verify: false` on the connector (no certificate check: for testing only).
- **The web console, the web terminal and MCP** work over HTTPS unchanged. The server's own callers on the same machine (the console's Python host, jobs that install plugins from this server) reach it through a separate plain-HTTP listener on `127.0.0.1` with a random port, which is never exposed.

!!! tip "Let's Encrypt"
    Let's Encrypt's private keys are readable only by root. Rather than running the API as root, copy the files where the server's user can read them whenever they are renewed, with a deploy hook (here for the user `meerschaum`):

    ```bash
    sudo mkdir -p /etc/meerschaum/tls
    sudo certbot certonly --standalone -d mrsm.example.com --deploy-hook \
      'install -m 640 -g meerschaum "$RENEWED_LINEAGE/privkey.pem" "$RENEWED_LINEAGE/fullchain.pem" /etc/meerschaum/tls/'
    ```

    The server picks up each renewal by itself. Ports below 1024 (such as 443) need privileges on Linux: serve on a higher port (`-p 8443`) or forward 443 to it.

In the [stack](/reference/stack/), mount the certificate directory into the `api` service (`mrsm edit config stack`, under `docker-compose.yaml`) and set `api:server:ssl_keyfile` / `ssl_certfile` to the paths inside the container; its healthcheck needs no change.

The 4.1 keys `api:uvicorn:ssl_keyfile` and `api:uvicorn:ssl_certfile` still work (with a notice to move them to `api:server`). `--keyfile '' --certfile ''` serves plain HTTP even when the config sets a key and a certificate.

A reverse proxy that terminates TLS, such as Caddy or NGINX, remains a fine choice, for example to serve several sites on port 443: see the [example NGINX configuration](/reference/api-instance/nginx/). Behind a proxy, set `api:server:forwarded_allow_ips` and `api:server:proxy_headers` so that the server sees each client's address (see [Permissions](/reference/api-instance/permissions/)).

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
