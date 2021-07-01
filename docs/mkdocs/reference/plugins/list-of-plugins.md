# List of Meerschaum Plugins

This page contains a list of plugins available on the [public Meerschaum repository](https://api.mrsm.io/dash/plugins). To install a plugin, run the command `mrsm install plugin <name>`.

In case you would like to showcase your plugin or ask questions, please share in the [GitHub discussion thread](https://github.com/bmeares/Meerschaum/discussions/50)!

If you have [written a Meerschaum plugin](/reference/plugins/writing-plugins/) and would like it published to the public repository, run the following commands (replacing values in angle brackets):

```bash
### Register an account at instance `api:mrsm`.
mrsm register user <myusername> -i api:mrsm

### Register your plugin to repository `api:mrsm`.
mrsm register plugin <myplugin> -r api:mrsm
```

## Data Plugins

[Data plugins](/reference/plugins/types-of-plugins/#data-plugins) are used to incorporate third party data sources into Meerschaum. To use a data plugin, [bootstrap a new pipe](/reference/pipes/bootstrapping/) and choose the plugin as its [connector](/reference/connectors/#connectors).

### `apex`

The `apex` plugin connects to Apex Clearing's API so you may extract your transaction history. For example, this plugin may be used to extract transactions from M1 Finance.

[GitHub](https://github.com/bmeares/apex) | [Source](https://api.mrsm.io/plugins/apex)

### `noaa`

The `noaa` plugin reads weather station data from the [NOAA Weather API](https://www.weather.gov/documentation/services-web-api).

[GitHub](https://github.com/bmeares/noaa) | [Source](https://api.mrsm.io/plugins/noaa)

### `sense`

The `sense` plugin fetches environment readings from the Raspberry Pi Sense HAT.

[GitHub](https://github.com/bmeares/sense) | [Source](https://api.mrsm.io/plugins/sense)

## Action Plugins

[Action plugins](/reference/plugins/types-of-plugins/#action-plugins) add new commands to the Meerschaum system.

### `color`

The `color` plugin provides a shortcut to inverting the ANSI and Unicode configuration settings.

[Source](https://api.mrsm.io/plugins/color)

## API Plugins

[API plugins](/reference/plugins/types-of-plugins/#api-plugins) extend the web API's [`fastapi`](https://fastapi.tiangolo.com/) app.

### `sso`

The `sso` plugin is written to demonstrate how to integrate a Google sign-in and add additional endpoints. This plugin is also used as part of the back-end for this [wedding website](https://mazlinandaaron.com/).

[GitHub](https://github.com/bmeares/sso) | [Source](https://api.mrsm.io/plugins/sso)
