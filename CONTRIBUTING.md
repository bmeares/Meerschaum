# Contributing

Meerschaum is free and open source software, built to make the lives of data scientists easier. It's my goal to direct the project towards the needs of my users, so the best way you can help steer the development of Meerschaum is to make your voice heard! If you have a need, chances are that someone else down the road will benefit from your present input.

Here are a couple ways you can help out! 💪

1. **Report bugs on the [issues tracker](https://github.com/bmeares/Meerschaum/issues).**

  No matter how large or small, your input is greatly appreciated and gives me a better understanding of the needs of real users.

2. **Join the [discussions board](https://github.com/bmeares/Meerschaum/discussions).**

  Feature requests, suggestions, and even showcases are welcome!

3. **Write and publish [plugins](https://meerschaum.io/reference/plugins/writing-plugins/).**

  If you publish a plugin to the public [`api:mrsm` repository](https://api.mrsm.io/dash/) and post in the [plugins showcase discussion](https://github.com/bmeares/Meerschaum/discussions/50), I will review and possibly include it on the [List of Plugins reference page](https://meerschaum.io/reference/plugins/list-of-plugins/).

## What About Pull Requests?

PRs are welcome! Due to the development pattern of Meerschaum, I likely may tweak PRs for a number of reasons. A good first step to making changes is to open an issue or start a discussion.

## Setting Up A Development Environment

For local development, you will need the following:

- Docker
- Git
- Python

First, clone the Meerschaum repo:

```bash
git clone https://github.com/bmeares/Meerschaum.git ~/Meerschaum
cd ~/Meerschaum
```

Build a local Docker image to install dependencies (note: this will overwrite `bmeares/meerschaum` on your machine):

```bash
./scripts/docker/buildx.sh
```

Start the development container, which will mount your cloned repository as the installed package in the container.


```bash
docker compose up -d
```

Start a new Bash shell in the container:

```bash
docker exec -it mrsm bash
```

You can now run `mrsm` commands in the Docker container. Changes you make to your cloned repository will be reflected in the container, so iterate to your heart's content!

The script `./scripts/setup.sh` will install dependencies for local development, and `./scripts/build.sh` will build a wheel and local documentation. This is only needed for local Python development.

## Sponsorship

Consider [sponsoring Meerschaum](https://github.com/sponsors/bmeares) as a way to prioritize your issues or feature requests. Your support is always greatly appreciated! I am available to implement specific features or to design and deploy your architecture using Meerschaum.
