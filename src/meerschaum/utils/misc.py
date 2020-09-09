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
    return options[choice](**kw)

def get_modules_from_package(
        package : 'package',
        names : bool = False,
        recursive : bool = False
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

    modules = []
    for module_name in [package.__name__ + "." + mod_name for mod_name in _all]:
        try:
            modules.append(importlib.import_module(module_name))
        except:
            pass
    if names:
        return _all, modules
    return modules

def import_children(
        package : 'package' = None,
        package_name : str = None,
        types : list = ['method', 'builtin', 'function', 'class']
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
    modules = get_modules_from_package(package)
    _all, members = [], []
    for module in modules:
        for ob in inspect.getmembers(module):
            for t in types:
                if getattr(inspect, 'is' + t)(ob[1]):
                    setattr(sys.modules[package_name], ob[0], ob[1])
                    _all.append(ob[0])
                    members.append(ob[1])
    
    ### set __all__
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
        default : str = 'y'
    ) -> bool:
    """
    Print a question and prompt the user with a yes / no input
    
    Returns bool (answer)
    """
    ending = " (" + "/".join([ o.upper() if o == default else o.lower() for o in options ]) + ") "
    print(question, end=ending, flush=True)
    answer = str(input()).lower()
    return answer == options[0].lower()


def reload_package(
        package : 'package',
        debug : bool = False,
        **kw
    ):
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

