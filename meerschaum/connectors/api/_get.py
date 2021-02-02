#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrappers for requests.get
"""

def get(
        self,
        r_url : str,
        headers : dict = {},
        use_token : bool = True,
        debug : bool = False,
        **kw
    ):
    """
    Wrapper for requests.get
    """
    if debug: from meerschaum.utils.debug import dprint

    if use_token:
        if debug: dprint(f"Checking login token.")
        headers.update({ 'Authorization': f'Bearer {self.token}' })

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Sending GET request to {self.url + r_url}.")
        pprint(headers)
        pprint(kw)

    return self.session.get(
        self.url + r_url,
        headers = headers,
        **kw
    )

def wget(
        self,
        r_url : str,
        dest : str = None,
        *args,
        **kw
    ) -> 'pathlib.Path':
    """
    Mimic wget with requests
    """
    from meerschaum.utils.warnings import warn, error
    import os, pathlib, re
    try:
        response = self.get(r_url)
    except:
        error(f"Failed to download from '{r_url}'")
    try:
        d = response.headers['content-disposition']
        fname = re.findall("filename=(.+)", d)[0].strip('"')
    except:
        fname = r_url.split('/')[-1]

    if dest is None:
        dest = pathlib.Path(os.path.join(os.getcwd(), fname))
    elif isinstance(dest, str): dest = pathlib.Path(dest)

    with open(dest, 'wb') as f:
        f.write(response.content)

    return dest
