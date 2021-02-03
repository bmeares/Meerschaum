# Quick Start
Meerschaum is customizable to best fit your situation, but if you're starting from scratch, the defaults work just fine. Read the guide below and follow the steps to install and use Meerschaum.

## **Step 1:** Requirements

To install Meerschaum, you need will need [Python 3.7+](https://www.python.org/) with [`pip`](https://pip.pypa.io/en/stable/installing/) installed (Python 3.4+ comes with `pip` pre-installed).

### **Step 1.5:** Server Stack Requirements

Meerschaum comes with the `stack` command which leverages [Docker Compose](https://docs.docker.com/compose/) to create all the necessary services in a full-stack Meerschaum installation ― services such as a database instance, API server, pre-configured Grafana instance, MQTT broker, and more.

The [stack](#starting-the-stack) is only needed for back-end server installations and is optional, so you can skip this step for any of the following reasons:

- You want a minimal install with SQLite instead of a dedicated database like TimescaleDB.
- You don't want to / can't install Docker.
  - You can install and run Meerschaum without any elevated privileges. More info on running [unprivileged Meerschaum here](#unprivileged-installation).
- You only need to use Meerschaum as a client for an existing Meerschaum instance.

If you plan on running the default, pre-configured Meerschaum database, make sure you have [Docker](https://www.docker.com/get-started) installed. Docker is used for the `stack` command, so if you have another database, you can skip installing Docker.

If you are running Linux, search your repositories for `docker.io` or run this script:

```bash
curl https://get.docker.com | sh
```

If you're on Windows or MacOS, install [Docker Desktop](https://www.docker.com/get-started).

## **Step 2:** Installation

Install Meerschaum from [PyPI](https://pypi.org/project/meerschaum/):

```bash
python -m pip install --upgrade meerschaum
```
!!! tip
    Meerschaum will auto-install packages in as you use them (into a virtual environment, to preserve your base environment). If you'd rather install all of its dependencies at installation, you can request the `full` version:
    ```bash
    python -m pip install --upgrade meerschaum[full]
    ```

That's it! You've got Meerschaum installed. Continue below for information on [bringing up a stack](#starting-the-stack) and creating your first pipes.

## **Step 3:** Starting the Stack

To bring up the stack, run the following command:

```bash
mrsm stack up [-d]
```

The `stack` command is an alias for [docker-compose](https://docs.docker.com/compose/), and any flags passed to `docker-compose` are wrapped in brackets (e.g. `[-d]`).

If you want to stop all the services in the stack, run the stack command with `down`:

```bash
mrsm stack down
```


