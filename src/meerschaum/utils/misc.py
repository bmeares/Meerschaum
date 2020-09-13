#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Miscellaneous functions go here
"""

def add_method_to_class(
        func : 'function',
        class_def : 'class', 
        method_name : str = None
    ) -> 'function':
    """
    Add function `func` to class `class_def`
    func - function :
        function to be added as a method of the class
    class_def - class :
        class we are modifying
    method_name - str (default None) :
        new name of the method. None will use func.__name__
    """
    from functools import wraps
    
    @wraps(func)
    def wrapper(self, *args, **kw):
        return func(*args, **kw)

    if method_name is None: method_name = func.__name__
    setattr(class_def, method_name, wrapper)
    return func

def choose_subaction(
        action : list = [''],
        options : dict = {},
        **kw
    ) -> tuple:
    """
    Given a dictionary of options and the standard Meerschaum actions list,
    check if choice is valid and execute chosen function, else show available
    options and return False
    
    action - list:
        subactions (e.g. `show pipes` -> ['pipes'])
    options - dict:
        Available options to execute
        option (key) -> function (value)
        Functions must accept **kw keyword arguments
        and return a tuple of success code and message
    """
    import inspect
    parent_action = inspect.stack()[1][3]
    if len(action) == 0: action = ['']
    choice = action[0]
    if choice not in options:
        print(f"Cannot {parent_action} '{choice}'. Choose one:")
        for option in options:
            print(f"  - {parent_action} {option}")
        return (False, f"Invalid choice '{choice}'")
    kw['action'] = action
    return options[choice](**kw)

def get_modules_from_package(
        package : 'package',
        names : bool = False,
        recursive : bool = False,
        debug : bool = False
    ):
    """
    Find and import all modules in a package.

    Returns: either list of modules or tuple of lists
    
    names = False (default) : modules
    names = True            : (__all__, modules)
    """
    from os.path import dirname, join, isfile, isdir, basename
    import glob, importlib

    if recursive: pattern = '*'
    else: pattern = '*.py'
    module_names = glob.glob(join(dirname(package.__file__), pattern), recursive=recursive)
    _all = [
        basename(f)[:-3] if isfile(f) else basename(f)
            for f in module_names
                if (isfile(f) or isdir(f))
                    and not f.endswith('__init__.py')
                    and not f.endswith('__pycache__')
    ]

    if debug: print(_all)
    modules = []
    for module_name in [package.__name__ + "." + mod_name for mod_name in _all]:
        ### there's probably a better way than a try: catch but it'll do for now
        try:
            modules.append(importlib.import_module(module_name))
        except Exception as e:
            if debug: print(e)
            pass
    if names:
        return _all, modules
    return modules

def import_children(
        package : 'package' = None,
        package_name : str = None,
        types : list = ['method', 'builtin', 'function', 'class'],
        debug : bool = False
    ) -> list:
    """
    Import all functions in a package to its __init__.
    package : package (default None)
        Package to import its functions into.
        If None (default), use parent
    
    package_name : str (default None)
        Name of package to import its functions into
        If None (default), use parent

    types : list
        types of members to return.
        Default : ['method', 'builtin', 'class', 'function']

    Returns: list of members
    """
    import sys, inspect
    
    ### if package_name and package are None, use parent
    if package is None and package_name is None:
        package_name = inspect.stack()[1][0].f_globals['__name__']

    ### populate package or package_name from other other
    if package is None:
        package = sys.modules[package_name]
    elif package_name is None:
        package_name = package.__name__

    ### Set attributes in sys module version of package.
    ### Kinda like setting a dictionary
    ###   functions[name] = func
    modules = get_modules_from_package(package, debug=debug)
    _all, members = [], []
    for module in modules:
        for ob in inspect.getmembers(module):
            for t in types:
                if getattr(inspect, 'is' + t)(ob[1]):
                    setattr(sys.modules[package_name], ob[0], ob[1])
                    _all.append(ob[0])
                    members.append(ob[1])
    
    if debug: print(_all)
    ### set __all__ for import *
    setattr(sys.modules[package_name], '__all__', _all)
    return members

def generate_password(
        length : int = 12
    ):
    """
    Generate a secure password of given length.
    """
    import secrets, string
    return ''.join((secrets.choice(string.ascii_letters) for i in range(length)))

def yes_no(
        question : str = '',
        options : list = ['y', 'n'],
        default : str = 'y',
        wrappers : tuple = ('[', ']'),
    ) -> bool:
    """
    Print a question and prompt the user with a yes / no input
    
    Returns bool (answer)
    """
    ending = f" {wrappers[0]}" + "/".join(
                [ o.upper() if o == default else o.lower() for o in options ]
                ) + f"{wrappers[1]} "
    print(question, end=ending, flush=True)
    answer = str(input()).lower()
    return answer == options[0].lower()

def reload_package(
        package : 'package',
        debug : bool = False,
        **kw
    ):
    """
    Recursively load a package's subpackages, even if they were not previously loaded
    """
    import os
    import types
    import importlib
    assert(hasattr(package, "__package__"))
    fn = package.__file__
    fn_dir = os.path.dirname(fn) + os.sep
    module_visit = {fn}
    del fn

    def reload_recursive_ex(module):
        importlib.reload(module)

        for module_child in get_modules_from_package(module, recursive=True):
            if isinstance(module_child, types.ModuleType) and hasattr(module_child, '__name__'):
                fn_child = getattr(module_child, "__file__", None)
                if (fn_child is not None) and fn_child.startswith(fn_dir):
                    if fn_child not in module_visit:
                        if debug: print("reloading:", fn_child, "from", module)
                        module_visit.add(fn_child)
                        reload_recursive_ex(module_child)

    return reload_recursive_ex(package)

def is_int(s):
    """
    Check if string is an int
    """
    try:
        float(s)
    except ValueError:
        return False
    else:
        return float(s).is_integer()

def get_options_functions():
    """
    Get options functions from parent module
    """
    import inspect
    parent_globals = inspect.stack()[1][0].f_globals
    parent_package = parent_globals['__name__']
    print(parent_package)

def string_to_dict(
        params_string : str
    ) -> dict:
    """
    Parse a string into a dictionary

    If the string begins with '{', parse as JSON. Else use simple parsing

    """

    import ast

    if str(params_string)[0] == '{':
        import json
        return json.loads(params_string)

    params_dict = dict()
    for param in params_string.split(","):
        values = param.split(":")
        try:
            key = ast.literal_eval(values[0])
        except:
            key = str(values[0])

        for value in values[1:]:
            try:
                params_dict[key] = ast.literal_eval(value)
            except:
                params_dict[key] = str(value)
    return params_dict

