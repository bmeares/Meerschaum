# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module is the entry point for the interactive shell
"""

import sys, inspect
from meerschaum.utils.misc import attempt_import
from meerschaum.config import __doc__, config as cf, get_config
cmd = attempt_import(get_config('system', 'shell', 'cmd', patch=True), warn=False)
if cmd is None or isinstance(cmd, dict): cmd = attempt_import('cmd')
from meerschaum.actions.arguments import parse_line
from meerschaum.utils.formatting import UNICODE, CHARSET, ANSI, colored
_clear_screen = get_config('system', 'shell', 'clear_screen', patch=True)
### readline is Unix-like only. Disable readline features for Windows
try:
    import readline
except ImportError:
    readline = None

patch = True
### remove default cmd2 commands
commands_to_remove = {
    'alias',
    'macro',
    'run_pyscript',
    'run_script',
    'shell',
    'pyscript',
    #  'set',
    'py',
    'shell',
    'shortcuts',
    'history',
    'load',
}
### cmd2 only: hide commands
hidden_commands = {
    'pass',
    'exit',
    'quit',
}
class Shell(cmd.Cmd):
    def __init__(self):
        """
        Customize the CLI from configuration
        """
        try: ### try cmd2 arguments first
            super().__init__(
                allow_cli_args = False,
                auto_load_commands = False,
                persistent_history_length = 1000,
                persistent_history_file = None,
            )
        except: ### fall back to default init (cmd)
            super().__init__()

        ### remove default commands from the Cmd class
        for command in commands_to_remove:
            try:
                delattr(cmd.Cmd, f'do_{command}')
            except:
                pass

        self.intro = get_config('system', 'shell', CHARSET, 'intro', patch=patch) + '\n' + __doc__
        self.prompt = get_config('system', 'shell', CHARSET, 'prompt', patch=patch)
        self.debug = False
        self.ruler = get_config('system', 'shell', CHARSET, 'ruler', patch=patch)
        self.close_message = get_config('system', 'shell', CHARSET, 'close_message', patch=patch)
        self.doc_header = get_config('system', 'shell', CHARSET, 'doc_header', patch=patch)
        self.undoc_header = get_config('system', 'shell', CHARSET, 'undoc_header', patch=patch)

        ### create default instance connector
        self.instance_keys = get_config('meerschaum', 'instance', patch=patch)

        ### update hidden commands list (cmd2 only)
        try:
            for c in hidden_commands:
                self.hidden_commands.append(c)

        except Exception as e:
            pass

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
        ### make a backup of line for later
        import copy
        original_line = copy.deepcopy(line)

        ### cmd2 support: check if command exists
        try:
            command = line.command
            line = str(command) + (' ' + str(line) if len(str(line)) > 0 else '')
        except Exception:
            ### we're probably running the original cmd, not cmd2
            command = None
            line = str(line)

        ### if the user specifies, clear the screen before executing any commands
        if _clear_screen:
            from meerschaum.utils.formatting._shell import clear_screen
            clear_screen(debug=self.debug)

        ### return blank commands (spaces break argparse)
        if original_line is None or len(str(line).strip()) == 0:
            return original_line

        if line in {
            'exit',
            'quit',
            'EOF',
        }:
            return "exit"
        ### help shortcut
        help_token = '?'
        if line.startswith(help_token):
            return "help " + line[len(help_token):]

        ### first things first: save history BEFORE execution
        from meerschaum.config._paths import SHELL_HISTORY_PATH
        if readline:
            readline.set_history_length(get_config('system', 'shell', 'max_history', patch=patch))
            readline.write_history_file(SHELL_HISTORY_PATH)

        args = parse_line(line)

        ### if debug is not set on the command line,
        ### default to shell setting
        if not args['debug']: args['debug'] = self.debug

        action = args['action'][0]

        ### if no instance is provided, use current shell default,
        ### but not for the 'api' command (to avoid recursion)
        if 'mrsm_instance' not in args and action != 'api':
            args['mrsm_instance'] = str(self.instance_keys)

        ### parse out empty strings
        if action.strip("\"'") == '':
            self.emptyline()
            return ""

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
        
        if positional_only:
            #  if self.debug: print("Did not find keyword arguments. " + "Returning original line:\n" + str(original_line))
            return original_line
        
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

    def post_cmd(stop : bool = False, line : str = ""):
        pass

    def do_pass(self, line):
        """
        Do nothing.
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

    def do_instance(self, action : list = [''], debug : bool = False, **kw):
        """
        Temporarily set a default Meerschaum instance for the duration of the shell.
        The default instance is loaded from the Meerschaum configuraton file
          (at keys 'meerschaum:instance').

        You can change the default instance with `edit config`.

        Usage:
            instance {instance keys}

        Examples:
            ### reset to default instance
            instance

            ### set the instance to 'api:main'
            instance api

            ### set the instance to a non-default connector
            instance api:myremoteinstance

        Note that instances must be configured, be either API or SQL connections,
          and be accessible to this machine over the network.
        """
        from meerschaum import get_connector
        from meerschaum.config import get_config
        from meerschaum.utils.misc import parse_instance_keys
        from meerschaum.utils.warnings import warn

        instance_keys = action[0]
        if instance_keys == '': instance_keys = get_config('meerschaum', 'instance', patch=True)

        conn = parse_instance_keys(instance_keys, debug=debug)
        if conn is None or not conn:
            conn = get_connector(debug=debug)

        self.instance_keys = str(conn)

        print(f"Default instance for the current shell: {conn}")
        return True, "Success"

    def do_exit(self, params):
        """
        Exit the Meerschaum shell.
        """
        return True

    def emptyline(self):
        """
        If the user specifies, clear the screen.
        """
        if _clear_screen:
            from meerschaum.utils.formatting._shell import clear_screen
            clear_screen(debug=self.debug)

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
        ### if the user specifies, clear the screen before initializing the shell
        if _clear_screen:
            from meerschaum.utils.formatting._shell import clear_screen
            clear_screen(debug=self.debug)

    def postloop(self):
        from meerschaum.config._paths import SHELL_HISTORY_PATH
        if readline:
            readline.set_history_length(get_config('system', 'shell', 'max_history', patch=patch))
            readline.write_history_file(SHELL_HISTORY_PATH)
        print('\n' + self.close_message)

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

