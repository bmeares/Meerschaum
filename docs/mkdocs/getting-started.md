# Getting Started

Meerschaum is customizable to best fit your situation, but if you're starting from scratch, the defaults work just fine. Read the [Quick Start](#quick-start) guide below and follow the steps to install and use Meerschaum.

## Quick Start

### Requirements

To install Meerschaum, you need will need [Python 3.7+](https://www.python.org/) with [pip](https://pip.pypa.io/en/stable/installing/) installed.

If you plan on running the default Meerschaum back-end database, make sure you have [Docker](https://www.docker.com/get-started) installed. Docker is used for the `stack` command, so if you have another database, you can skip installing Docker.

If you are running Linux, search your repositories for `docker.io` or run this script:

```bash
curl https://get.docker.com | sh
```

If you're on Windows or MacOS, install [Docker Desktop](https://www.docker.com/get-started).

### Installation

Install Meerschaum from [PyPI](https://pypi.org/project/meerschaum/):

```bash
python -m pip install --upgrade meerschaum
```

Meerschaum will auto-install packages in as you use them (into a virtual environment, to preserve your base environment). If you'd rather install all of its dependencies at installation, you can request the `full` version:

```bash
python -m pip install --upgrade meerschaum[full]
```

That's it! You've got Meerschaum installed. Continue below for information on [bringing up a stack](#starting-the-stack) and creating your first pipes.

### Starting the Stack



### Bootstrapping a Pipe

