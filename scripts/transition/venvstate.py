import os, sys, json, pathlib
def snap(root):
    out = {}
    base = pathlib.Path(root) / 'venvs'
    for v in sorted(base.iterdir()) if base.exists() else []:
        sp = list(v.glob('lib/python*/site-packages'))
        names = sorted(x.name for x in sp[0].iterdir()) if sp else []
        out[v.name] = {'n': len(names), 'dist': [n for n in names if n.endswith('.dist-info')], 'mtime': sp[0].stat().st_mtime if sp else None,
                       'ino': v.stat().st_ino}
    return out
if __name__ == '__main__':
    print(json.dumps(snap(sys.argv[1]), indent=1))
