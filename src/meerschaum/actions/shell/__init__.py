#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module is the entry point for the interactive shell
"""

import cmd
from meerschaum.config import __doc__
from meerschaum.actions.arguments import parse_arguments
import inspect

class Shell(cmd.Cmd):
    #  added_methods = dict()
    prompt = "mrsm —> "
    intro = __doc__

    def precmd(self, line):
        """
        Pass line string to parent actions.
        Pass parsed arguments to custom actions
        """
        if line is None or len(line) == 0: return line

        args = parse_arguments(line.split())
        action = args['action'][0]
        ### delete the first action
        ### e.g. 'show actions' -> ['actions']
        del args['action'][0]

        try:
            func = getattr(self, 'do_' + action)
            func_param_kinds = inspect.signature(func).parameters.items()
        except AttributeError as ae:
            return line

        positional_only = True
        for param in func_param_kinds:
            ### if variable keyword arguments found,
            ### use meerschaum parser, else just pass
            ### the line string without parsing
            if str(param[1].kind) == "VAR_KEYWORD":
                positional_only = False
                break
        
        if positional_only: return line
        
        ### execute the meerschaum action
        ### and print the response message in case of failure
        response = func(**args)
        if not response[0]: print(reponse[1])
        return ""

    def do_exit(self, params):
        """
        Exit the shell
        """
        return True

    def do_quit(self, params):
        """
        Exit the shell
        """
        return True

    def do_EOF(self, line):
        """
        Exit the shell
        """
        return True

    def emptyline(self):
        pass

