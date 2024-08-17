# 🌐 API Instance

The Meerschaum API instance (and [Web Console](/get-started/start-api/)) lets you manage [pipes](/reference/pipes/), [jobs](/reference/background-jobs/), and [connectors](/reference/connectors/) over the Internet. Start the API server with the [stack](/reference/stack/) or via `start api`; see [Deployment](#deployment) below for ways to run Meerschaum in production.

## Usage

Starting the Meerschaum API server lets you treat your host as the following:

- [API instance](/reference/connectors/#-instances-and-repositories)  
  For managing pipes.
- [Plugin repository](/reference/plugins/repositories/)  
  For storing plugins.
- [Remote executor](/reference/background-jobs/#-executors)  
  For running jobs.

## Deployment

It's common in production deployments to stand up a public-facing API instance. See below for deployment tips:


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
    The `--production` flag starts the API server with `gunicorn`, allowing for self-healing.

```
mrsm start api --production
```

For example, this is a common pattern for [standing up a `systemd` service](/reference/background-jobs/) to start the API server:

```
mrsm start api --production --name api-instance -d
```

### TLS / SSL / HTTPS

See the [example NGINX configuration](/reference/api-instance/nginx/) for a proxy pass to add HTTPS to your API instance via Let's Encrypt.

Otherwise, if you have a certificate on hand, you can run the API server with `--keyfile` and `--certfile`:

```
mrsm start api --keyfile /path/to/key --certfile /path/to/cert
```

### Docker

If you're building your own Docker images, consider basing off the [`bmeares/meerschaum` Docker image](https://hub.docker.com/r/bmeares/meerschaum), as is done in the [`mrsm compose` template repository](https://github.com/bmeares/mrsm-compose-template/blob/main/docker/Dockerfile):

```docker
FROM bmeares/meerschaum

RUN mrsm install plugin compose
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