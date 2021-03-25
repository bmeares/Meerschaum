# Quick Start
Although you can customize Meerschaum to best fit your situation, if you're starting from scratch, the defaults work just fine. Read the guide below and follow the steps to install and use Meerschaum.

***TL;DR*** If you already have a Python 3.7+ environment set up, install Meerschaum via [`pip`](https://pip.pypa.io/en/stable/installing/):
```bash
pip install meerschaum
```

!!! question "Can I run Meerschaum without installing anything?"
    If you can't install Python or just want to try Meerschaum, consider downloading [Meerschaum Portable](meerschaum-portable).

## Requirements

To install Meerschaum, you need [Python 3.7+](https://www.python.org/) with [`pip`](https://pip.pypa.io/en/stable/installing/) installed.
> Python 3.4+ downloaded from [python.org](https://www.python.org/) comes with `pip` pre-installed, but on some operating systems like Ubuntu, you might have to install `python3-venv` and `python3-pip`.

### Server Stack Requirements

Meerschaum comes with the `stack` command which leverages [Docker Compose](https://docs.docker.com/compose/) to create all the necessary services in a full-stack Meerschaum installation ― services such as a database instance, API server, and pre-configured Grafana instance.

The [stack](starting-the-stack) is only needed for back-end server installations and is optional, so you can skip this step for any of the following reasons:

- You only need to use Meerschaum as a client for an existing Meerschaum instance.
- You want a minimal install with SQLite instead of a dedicated database like TimescaleDB.
- You're instaling Meerschaum on an IoT device (like a Raspberry Pi).
- You don't want to / can't install Docker.


If you plan on running the default, pre-configured Meerschaum database, make sure you have [Docker](https://www.docker.com/get-started) installed. Docker is used for the `stack` command, so if you have another database, you can skip installing Docker.

To install Docker, follow [this guide](https://docs.docker.com/engine/install/) or do the following:

=== "Windows / MacOS"
    If you're on Windows or MacOS, install [Docker Desktop](https://www.docker.com/get-started).

=== "Linux"
    If you are running Linux, search your repositories for `docker.io` or run this script:

    ```bash
    curl https://get.docker.com | sh
    ```
    
    Don't forget to add yourself to the `docker` user group and log out and back in.
    
    ```bash
    sudo usermod -aG docker myusername
    ```

## Install with `pip`

Install Meerschaum from [PyPI](https://pypi.org/project/meerschaum/):

```bash
python -m pip install --upgrade meerschaum
```

!!! tip
    Meerschaum will auto-install packages as you use them (into a virtual environment, to preserve your base environment). If you'd rather install all of its dependencies at installation, you can request the `full` version:
    ```bash
    python -m pip install --upgrade meerschaum[full]
    ```

That's it! You've got Meerschaum installed. Continue on for information on [bringing up a stack](starting-the-stack) and [creating your first pipes](bootstrapping-a-pipe).

