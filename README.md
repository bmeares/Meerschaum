
# Build Pipes with Meerschaum
Meerschaum is a platform for quickly creating and managing time-series data streams called Pipes.

## Setup
### Requirements
Before getting started, make sure you have [Docker](https://www.docker.com/get-started) and Docker Compose installed. You can install Docker Compose with `pip`:
```
pip install docker-compose
```

### Installation
To install Meerschaum and all its features, install with `pip`:
```
pip install -U meerschaum[full]
```

To install the minimal installation without all the extra dependencies, install the base package:
```
pip install -U meerschaum
```
**Note: Don't forget to specify `-U` or `--user`!**
You must install Meerschaum as a user and not system-wide. This will cause problems on MacOS if you install without `-U` / `--user`.

### Quickstart
