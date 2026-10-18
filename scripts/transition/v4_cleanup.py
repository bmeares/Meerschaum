"""Drop the server-side objects of the given flavors (and nothing else): `python v4_cleanup.py flavor ...`."""
import subprocess, sys, pathlib
HERE = pathlib.Path(__file__).resolve().parent
for f in sys.argv[1:]:
    if f != 'sqlite':
        subprocess.run([sys.executable, '-s', '-E', str(HERE / 'v4_admin.py'), 'drop', f])
