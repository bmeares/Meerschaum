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
