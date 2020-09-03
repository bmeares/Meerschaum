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

