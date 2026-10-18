"""
The `mrsm` and `meerschaum` console scripts.

A top-level module so that it can run before the `meerschaum` package is imported: it tells the
package (`sys._meerschaum_cli`) that this process is the command line, which only needs the native
extension, so `meerschaum/__init__.py` skips importing the façade (about 0.12 s). Everywhere else
`import meerschaum` is eager. See `meerschaum._internal.binary` for the rest of the launch.
"""

import sys


def main() -> None:
    sys._meerschaum_cli = True
    from meerschaum.__main__ import main as run
    run()


if __name__ == '__main__':
    main()
