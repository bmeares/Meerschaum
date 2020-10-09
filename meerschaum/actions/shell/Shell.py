#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module is the entry point for the interactive shell
"""

import cmd as cmd, sys, inspect
from meerschaum.config import __doc__, config as cf, get_config
from meerschaum.actions.arguments import parse_line
from meerschaum.utils.formatting import UNICODE, CHARSET, ANSI, colored
### readline is Unix-like only. Disable readline features for Windows
try:
    import readline
except ImportError:
    readline = None

patch = True

class Shell(cmd.Cmd):
    def __init__(self):
        """
        Customize the CLI from configuration
        """
        super().__init__()
        self.intro = get_config('system', 'shell', CHARSET, 'intro', patch=patch) + '\n' + __doc__
        self.prompt = get_config('system', 'shell', CHARSET, 'prompt', patch=patch)
        self.debug = False
        self.ruler = get_config('system', 'shell', CHARSET, 'ruler', patch=patch)
        self.close_message = get_config('system', 'shell', CHARSET, 'close_message', patch=patch)
        self.doc_header = get_config('system', 'shell', CHARSET, 'doc_header', patch=patch)
        self.undoc_header = get_config('system', 'shell', CHARSET, 'undoc_header', patch=patch)

        if ANSI:
            def apply_colors(attr, key):
                return colored(
                    attr,
                    *get_config('system', 'shell', 'ansi', key, 'color', patch=patch)
                )

            for attr_key in get_config('system', 'shell', 'ansi', patch=patch):
                self.__dict__[attr_key] = apply_colors(self.__dict__[attr_key], attr_key)

    def precmd(self, line):
        """
        Pass line string to parent actions.
        Pass parsed arguments to custom actions

        Overrides `default`: if action does not exist,
            assume the action is `bash`
        """
        if line is None or len(line) == 0:
            return line

        if line in {
            'exit',
            'quit',
            'EOF',
        }:
            return "exit"

        args = parse_line(line)

        ### if debug is not set on the command line,
        ### default to shell setting
        if not args['debug']: args['debug'] = self.debug

        action = args['action'][0]
        try:
            func = getattr(self, 'do_' + action)
            func_param_kinds = inspect.signature(func).parameters.items()
        except AttributeError as ae:
            ### if function is not found, default to `bash`
            action = "bash"
            args['action'].insert(0, "bash")
            func = getattr(self, 'do_bash')
            func_param_kinds = inspect.signature(func).parameters.items()

        ### delete the first action
        ### e.g. 'show actions' -> ['actions']
        del args['action'][0]
        if len(args['action']) == 0: args['action'] = ['']

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
        if isinstance(response, tuple):
            output = "\n"
            if not response[0]: output += "Error message: " + response[1]
            elif self.debug: output += response[1]
            if len(output) > 1:
                print(output)
        return ""

    def default(self, line):
        """
        If an action has not been declared, preprend 'bash' to the line
        and execute in a subshell
        """
        self.do_default(line)

    def do_pass(self, line):
        """
        Do nothing.
        """
        pass

    def do_default(self, action=[''], **kw):
        """
        If `action` is not implemented, execute in a subprocess.
        (preprends 'bash' to the actions)
        """
        pass

    def do_debug(self, action=[''], **kw):
        """
        Toggle the shell's debug mode.
        If debug = on, append `--debug` to all commands.

        Command: `debug {on/true | off/false}`
        Ommitting on / off will toggle the existing value.
        """
        on_commands = {'on', 'true'}
        off_commands = {'off', 'false'}
        state = action[0]
        if state == '':
            self.debug = not self.debug
        elif state.lower() in on_commands: self.debug = True
        elif state.lower() in off_commands: self.debug = False
        else: print(f"Unknown state '{state}'. Ignoring...")

        print(f"Debug mode is {'on' if self.debug else 'off'}.")

    def do_exit(self, params):
        """
        Exit the Meerschaum shell
        """
        return True

    def emptyline(self):
        pass

    def preloop(self):
        import signal, os
        from meerschaum.config._paths import SHELL_HISTORY_PATH
        if SHELL_HISTORY_PATH.exists():
            if readline:
                readline.read_history_file(SHELL_HISTORY_PATH)
        """
        Patch builtin cmdloop with my own input (defined below)
        """
        old_input = cmd.__builtins__['input']
        cmd.__builtins__['input'] = input_with_sigint(old_input)
        #  try:
            #  super().cmdloop(*args, **kw)
        #  finally:
            #  cmd.__builtins__['input'] = old_input


    def postloop(self):
        from meerschaum.config._paths import SHELL_HISTORY_PATH
        if readline:
            readline.set_history_length(get_config('system', 'shell', 'max_history', patch=patch))
            readline.write_history_file(SHELL_HISTORY_PATH)
        print('\n' + self.close_message)

    #  def cmdloop(self, *args, **kw):
def input_with_sigint(_input):
    """
    Patch builtin input()
    """
    def _input_with_sigint(*args):
        try:
            return _input(*args)
        except KeyboardInterrupt:
            print("^C")
            return "pass"
    return _input_with_sigint
