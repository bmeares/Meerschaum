# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module is the entry point for the interactive shell.
"""

from __future__ import annotations
import os
from copy import deepcopy
from itertools import chain
import shlex

from meerschaum.utils.typing import Union, SuccessTuple, Any, Callable, Optional, List, Dict
from meerschaum.utils.packages import attempt_import
from meerschaum.config import __doc__, __version__ as version, get_config
import cmd
_old_input = cmd.__builtins__['input']
prompt_toolkit = attempt_import('prompt_toolkit', lazy=False, warn=False, install=True)
(
    prompt_toolkit_shortcuts,
    prompt_toolkit_history,
    prompt_toolkit_formatted_text,
    prompt_toolkit_styles,
) = (
    attempt_import('prompt_toolkit.shortcuts', lazy=False, warn=False, install=True),
    attempt_import('prompt_toolkit.history', lazy=False, warn=False, install=True),
    attempt_import('prompt_toolkit.formatted_text', lazy=False, warn=False, install=True),
    attempt_import('prompt_toolkit.styles', lazy=False, warn=False, install=True),
)
from meerschaum._internal.shell.ValidAutoSuggest import ValidAutoSuggest
from meerschaum._internal.shell.ShellCompleter import ShellCompleter
_clear_screen = get_config('shell', 'clear_screen', patch=True)
from meerschaum.utils.misc import string_width, remove_ansi
from meerschaum.utils.warnings import warn
from meerschaum.jobs import get_executor_keys_from_context
from meerschaum.config.static import STATIC_CONFIG
from meerschaum._internal.arguments._parse_arguments import (
    split_chained_sysargs,
    split_pipeline_sysargs,
    parse_arguments,
    parse_line,
    parse_dict_to_sysargs,
)

patch = True
### remove default cmd2 commands
commands_to_remove = {
    'alias',
    'macro',
    'run_pyscript',
    'run_script',
    'shell',
    'pyscript',
    'py',
    'shortcuts',
    'history',
    'load',
}
### cmd2 only: hide commands
hidden_commands = {
    'os',
    'sh',
    'pass',
    'quit',
    'eof',
    'exit',
    '_relative_run_script',
    'ipy',
}
reserved_completers = {
    'instance', 'repo', 'executor',
}

### To handle dynamic reloading, store shell attributes externally.
### This is because the shell object address gets lost upon reloads.
shell_attrs = {}
AND_KEY: str = STATIC_CONFIG['system']['arguments']['and_key']
ESCAPED_AND_KEY: str = STATIC_CONFIG['system']['arguments']['escaped_and_key']
PIPELINE_KEY: str = STATIC_CONFIG['system']['arguments']['pipeline_key']
ESCAPED_PIPELINE_KEY: str = STATIC_CONFIG['system']['arguments']['escaped_pipeline_key']

def _insert_shell_actions(
    _shell: Optional['Shell'] = None,
    actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
    keep_self: bool = False,
) -> None:
    """
    Update the Shell with Meerschaum actions.
    """
    from meerschaum.utils.misc import add_method_to_class
    import meerschaum._internal.shell as shell_pkg
    from meerschaum.actions import get_completer
    if actions is None:
        from meerschaum.actions import actions as _actions
        actions = _actions

    _shell_class = _shell if _shell is not None else shell_pkg.Shell

    for a, f in actions.items():
        add_method_to_class(
            func = f,
            class_def = _shell_class,
            method_name = 'do_' + a,
        )
        if a in reserved_completers:
            continue
        _completer = get_completer(a)
        if _completer is None:
            _completer = shell_pkg.default_action_completer
        completer = _completer_wrapper(_completer)
        setattr(_shell_class, 'complete_' + a, completer)


def _completer_wrapper(
    target: Callable[[Any], List[str]]
) -> Callable[['mrsm._internal.shell.Shell', str, str, int, int], Any]:
    """
    Wrapper for `complete_` functions so they can instead use Meerschaum arguments.
    """
    from functools import wraps

    ### I have no idea why I had to remove `self`.
    ### Maybe it has to do with adding to an object instead of a class.
    @wraps(target)
    def wrapper(text, line, begin_index, end_index):
        _check_keys = _check_complete_keys(line)
        if _check_keys is not None:
            return _check_keys

        args = parse_line(line)
        if target.__name__ != 'default_action_completer':
            if len(args['action']) > 0:
                del args['action'][0]
        args['text'] = text
        args['line'] = line
        args['begin_index'] = begin_index
        args['end_index'] = end_index
        return target(**args)

    return wrapper


def default_action_completer(
    text: Optional[str] = None,
    line: Optional[str] = None,
    begin_index: Optional[int] = None,
    end_index: Optional[int] = None,
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Search for subactions by default. This may be overridden by each action.
    """
    from meerschaum.actions import get_subactions
    if action is None:
        action = []
    subactions = get_subactions(action)
    sub = action[1] if len(action) > 1 else ''
    possibilities = []
    for sa in subactions:
        if sa.startswith(sub) and sa != sub:
            possibilities.append(sa)
    return sorted(possibilities)


def _check_complete_keys(line: str) -> Optional[List[str]]:
    from meerschaum._internal.arguments._parser import get_arguments_triggers

    ### TODO Add all triggers
    trigger_args = {
        '-c': 'connector_keys',
        '--connector-keys': 'connector_keys',
        '-r': 'repository',
        '--repository': 'repository',
        '-i': 'mrsm_instance',
        '--instance': 'mrsm_instance',
        '--mrsm-instance': 'mrsm_instance',
    }

    ### TODO Find out arg possibilities
    possibilities = []
    last_word = line.rstrip(' ').split(' ')[-1]

    if last_word.startswith('-'):
        for _arg, _triggers in get_arguments_triggers().items():
            for _trigger in _triggers:
                if _trigger.startswith(last_word):
                    #  and _trigger != last_word
                    if _trigger != last_word or not line.endswith(' '):
                        possibilities.append(_trigger)
        if not line.endswith(' '):
            return possibilities

    if last_word.startswith('-') and not last_word.endswith('-'):
        if line.endswith(' ') and last_word in trigger_args:
            is_trigger = True
        elif line.endswith(' '):
            ### return empty list so we don't try to parse an incomplete line.
            return []

    from meerschaum.utils.misc import get_connector_labels

    if last_word.rstrip(' ') in trigger_args:
        return get_connector_labels()

    for trigger, var in trigger_args.items():
        if trigger in line:
            ### check if any text has been entered for the key
            if line.rstrip(' ').endswith(trigger):
                return get_connector_labels()

            return get_connector_labels(search_term=last_word.rstrip(' '))

    return None


def get_shell_intro(with_color: bool = True) -> str:
    """
    Return the introduction message string.
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    intro = get_config('shell', CHARSET, 'intro', patch=patch)
    intro += (
        '\n'
        + (' ' * (string_width(intro) - len('v' + version)))
        + f'v{version}'
    )

    if not with_color or not ANSI:
        return intro

    return colored(
        intro,
        **get_config('shell', 'ansi', 'intro', 'rich')
    )

def get_shell_session():
    """
    Return the `prompt_toolkit` prompt session.
    """
    from meerschaum.config._paths import SHELL_HISTORY_PATH
    if 'session' in shell_attrs:
        return shell_attrs['session']

    shell_attrs['session'] = prompt_toolkit_shortcuts.PromptSession(
        history=prompt_toolkit_history.FileHistory(SHELL_HISTORY_PATH.as_posix()),
        auto_suggest=ValidAutoSuggest(),
        completer=ShellCompleter(),
        complete_while_typing=True,
        reserve_space_for_menu=False,
    )
    return shell_attrs['session']


class Shell(cmd.Cmd):
    """
    The interactive Meerschaum shell.
    """
    def __init__(
        self,
        actions: Optional[Dict[str, Any]] = None,
        sysargs: Optional[List[str]] = None,
        instance_keys: Optional[str] = None,
    ):
        """
        Customize the CLI from configuration
        """
        if actions is None:
            actions = {}
        if sysargs is None:
            sysargs = []
        _insert_shell_actions(_shell=self, keep_self=True)
        try:
            delattr(cmd.Cmd, '_alias_create')
            delattr(cmd.Cmd, '_alias_delete')
            delattr(cmd.Cmd, '_alias_list')
            delattr(cmd.Cmd, '_macro_create')
            delattr(cmd.Cmd, '_macro_delete')
            delattr(cmd.Cmd, '_macro_list')
        except AttributeError:
            pass

        _ = get_shell_session()

        super().__init__()

        ### remove default commands from the Cmd class
        for command in commands_to_remove:
            try:
                delattr(cmd.Cmd, f'do_{command}')
            except Exception:
                pass

        ### NOTE: custom actions must be added to the self._actions dictionary
        shell_attrs['_actions'] = actions
        shell_attrs['_sysargs'] = sysargs
        shell_attrs['_actions']['instance'] = self.do_instance
        shell_attrs['_actions']['repo'] = self.do_repo
        shell_attrs['_actions']['executor'] = self.do_executor
        shell_attrs['_actions']['debug'] = self.do_debug
        shell_attrs['_update_bottom_toolbar'] = True
        shell_attrs['_old_bottom_toolbar'] = ''
        shell_attrs['debug'] = False
        shell_attrs['_reload'] = True
        self.load_config(instance=instance_keys)
        self.hidden_commands = []
        ### update hidden commands list (cmd2 only)
        try:
            for c in hidden_commands:
                self.hidden_commands.append(c)
        except Exception as e:
            pass

        ### Finally, spawn the version update thread.
        from meerschaum._internal.shell.updates import run_version_check_thread
        self._update_thread = run_version_check_thread(debug=shell_attrs.get('debug', False))

    def load_config(self, instance: Optional[str] = None):
        """
        Set attributes from the shell configuration.
        """
        from meerschaum.utils.misc import remove_ansi
        from meerschaum.utils.formatting import CHARSET, ANSI, colored
        from meerschaum._internal.shell.updates import get_update_message

        if shell_attrs.get('intro', None) != '':
            self.intro = (
                get_shell_intro(with_color=False)
                if shell_attrs.get('intro', None) != ''
                else ""
            ) + get_update_message()

        shell_attrs['intro'] = self.intro
        shell_attrs['_prompt'] = get_config('shell', CHARSET, 'prompt', patch=patch)
        self.prompt = shell_attrs['_prompt']
        shell_attrs['ruler'] = get_config('shell', CHARSET, 'ruler', patch=patch)
        self.ruler = shell_attrs['ruler']
        shell_attrs['close_message'] = get_config('shell', CHARSET, 'close_message', patch=patch)
        self.close_message = shell_attrs['close_message']
        shell_attrs['doc_header'] = get_config('shell', CHARSET, 'doc_header', patch=patch)
        self.doc_header = shell_attrs['doc_header']
        shell_attrs['undoc_header'] = get_config('shell', CHARSET, 'undoc_header', patch=patch)
        self.undoc_header = shell_attrs['undoc_header']

        if instance is None and self.__dict__.get('instance_keys', None) is None:
            ### create default instance and repository connectors
            shell_attrs['instance_keys'] = remove_ansi(get_config('meerschaum', 'instance', patch=patch))
            ### instance is a stylized version of instance_keys
            shell_attrs['instance'] = str(shell_attrs['instance_keys'])
        else:
            shell_attrs['instance'] = instance
            shell_attrs['instance_keys'] = remove_ansi(str(instance))
        if shell_attrs.get('repo_keys', None) is None:
            shell_attrs['repo_keys'] = get_config('meerschaum', 'default_repository', patch=patch)
        if shell_attrs.get('executor_keys', None) is None:
            shell_attrs['executor_keys'] = get_executor_keys_from_context()

        ### this will be updated later in update_prompt ONLY IF {username} is in the prompt
        shell_attrs['username'] = ''

        if ANSI:
            def apply_colors(attr, key):
                return colored(
                    attr,
                    **get_config('shell', 'ansi', key, 'rich', patch=patch)
                )

            for attr_key in get_config('shell', 'ansi'):
                if attr_key not in shell_attrs:
                    continue
                shell_attrs[attr_key] = apply_colors(shell_attrs[attr_key], attr_key)
                self.__dict__[attr_key] = shell_attrs[attr_key]

        ### refresh actions
        _insert_shell_actions(_shell=self, keep_self=True)

        ### replace {instance} in prompt with stylized instance string
        self.update_prompt()

    def insert_actions(self):
        from meerschaum.actions import actions

    def update_prompt(
        self,
        instance: Optional[str] = None,
        username: Optional[str] = None,
        executor_keys: Optional[str] = None,
    ):
        from meerschaum.utils.formatting import ANSI, colored
        from meerschaum._internal.entry import _shell, get_shell

        cmd.__builtins__['input'] = input_with_sigint(
            _old_input,
            shell_attrs['session'],
            shell=self,
        )
        prompt = shell_attrs['_prompt']
        mask = prompt
        shell_attrs['_update_bottom_toolbar'] = True

        if '{instance}' in shell_attrs['_prompt']:
            if instance is None:
                instance = shell_attrs['instance_keys']
            shell_attrs['instance'] = instance
            if ANSI:
                shell_attrs['instance'] = colored(
                    shell_attrs['instance'], **get_config(
                        'shell', 'ansi', 'instance', 'rich'
                    )
                )
            prompt = prompt.replace('{instance}', shell_attrs['instance'])
            mask = mask.replace('{instance}', ''.join(['\0' for c in '{instance}']))

        if '{username}' in shell_attrs['_prompt']:
            if username is None:
                from meerschaum.utils.misc import remove_ansi
                from meerschaum.connectors.parse import parse_instance_keys
                from meerschaum.connectors.sql import SQLConnector
                try:
                    conn_attrs = parse_instance_keys(
                        remove_ansi(shell_attrs['instance_keys']),
                        construct=False,
                    )
                    if 'username' not in conn_attrs:
                        if 'uri' in conn_attrs:
                            username = SQLConnector.parse_uri(conn_attrs['uri'])['username']
                    else:
                        username = conn_attrs['username']
                except KeyError:
                    username = '(no username)'
                except Exception as e:
                    username = str(e)
                if username is None:
                   username = '(no username)'
            shell_attrs['username'] = (
                username
                if not ANSI
                else colored(username, **get_config('shell', 'ansi', 'username', 'rich'))
            )
            prompt = prompt.replace('{username}', shell_attrs['username'])
            mask = mask.replace('{username}', ''.join(['\0' for c in '{username}']))

        if '{executor_keys}' in shell_attrs['_prompt']:
            if executor_keys is None:
                executor_keys = shell_attrs.get('executor_keys', None) or 'local'
            shell_attrs['executor_keys'] = (
                executor_keys
                if not ANSI
                else colored(
                    remove_ansi(executor_keys),
                    **get_config('shell', 'ansi', 'executor', 'rich')
                )
            )
            prompt = prompt.replace('{executor_keys}', shell_attrs['executor_keys'])
            mask = mask.replace('{executor_keys}', ''.join(['\0' for c in '{executor_keys}']))

        remainder_prompt = list(shell_attrs['_prompt'])
        for i, c in enumerate(mask):
            if c != '\0':
                _c = c
                if ANSI:
                    _c = colored(_c, **get_config('shell', 'ansi', 'prompt', 'rich'))
                remainder_prompt[i] = _c

        self.prompt = ''.join(remainder_prompt).replace(
            '{username}', shell_attrs['username']
        ).replace(
            '{instance}', shell_attrs['instance']
        ).replace(
            '{executor_keys}', shell_attrs['executor_keys']
        )
        shell_attrs['prompt'] = self.prompt
        ### flush stdout
        print("", end="", flush=True)


    def precmd(self, line: str):
        """
        Pass line string to parent actions.
        Pass parsed arguments to custom actions

        Overrides `default`. If an action does not exist, assume the action is `shell`
        """
        ### Preserve the working directory.
        old_cwd = os.getcwd()

        from meerschaum._internal.entry import get_shell
        self = get_shell(sysargs=shell_attrs['_sysargs'], debug=shell_attrs.get('debug', False))

        ### make a backup of line for later
        original_line = deepcopy(line)

        ### Escape backslashes to allow for multi-line input.
        line = line.replace('\\\n', ' ')

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
            clear_screen(debug=shell_attrs['debug'])

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

        try:
            sysargs = shlex.split(line)
        except ValueError as e:
            warn(e, stack=False)
            return ""

        sysargs, pipeline_args = split_pipeline_sysargs(sysargs)
        chained_sysargs = split_chained_sysargs(sysargs)
        chained_kwargs = [
            parse_arguments(_sysargs)
            for _sysargs in chained_sysargs
        ]

        if '--help' in sysargs or '-h' in sysargs:
            from meerschaum._internal.arguments._parser import parse_help
            parse_help(sysargs)
            return ""

        patch_args: Dict[str, Any] = {}

        ### NOTE: pass `shell` flag in case actions need to distinguish between
        ###       being run on the command line and being run in the shell
        patch_args.update({
            'shell': True,
            'line': line,
        })
        patches: List[Dict[str, Any]] = [{} for _ in chained_kwargs]

        ### if debug is not set on the command line,
        ### default to shell setting
        for kwargs in chained_kwargs:
            if not kwargs.get('debug', False):
                kwargs['debug'] = shell_attrs['debug']

        ### Make sure an action was provided.
        if (
            not chained_kwargs
            or not chained_kwargs[0].get('action', None)
        ):
            self.emptyline()
            return ''

        ### Strip a leading 'mrsm' if it's provided.
        for kwargs in chained_kwargs:
            if kwargs['action'][0] == 'mrsm':
                kwargs['action'] = kwargs['action'][1:]
                if not kwargs['action']:
                    self.emptyline()
                    return ''

        ### If we don't recognize the action,
        ### make it a shell action.
        ### TODO: make this work for chained actions
        def _get_main_action_name(kwargs):
            from meerschaum.actions import get_main_action_name
            main_action_name = get_main_action_name(kwargs['action'])
            if main_action_name is None:
                if not hasattr(self, 'do_' + kwargs['action'][0]):
                    kwargs['action'].insert(0, 'sh')
                    main_action_name = 'sh'
                else:
                    main_action_name = kwargs['action'][0]
            return main_action_name

        def _add_flag_to_kwargs(kwargs, i, key, shell_key=None):
            shell_key = shell_key or key
            shell_value = remove_ansi(shell_attrs.get(shell_key) or '')
            if key == 'mrsm_instance':
                default_value = get_config('meerschaum', 'instance')
            elif key == 'repository':
                default_value = get_config('meerschaum', 'default_repository')
            elif key == 'executor_keys':
                default_value = get_executor_keys_from_context()
            else:
                default_value = None

            if key in kwargs or shell_value == default_value:
                return

            patches[i][key] = shell_value

        ### if no instance is provided, use current shell default,
        ### but not for the 'api' command (to avoid recursion)
        for i, kwargs in enumerate(chained_kwargs):
            main_action_name = _get_main_action_name(kwargs)
            if main_action_name == 'api':
                continue

            _add_flag_to_kwargs(kwargs, i, 'mrsm_instance', shell_key='instance_keys')
            _add_flag_to_kwargs(kwargs, i, 'repository', shell_key='repo_keys')
            _add_flag_to_kwargs(kwargs, i, 'executor_keys')

        ### parse out empty strings
        if chained_kwargs[0]['action'][0].strip("\"'") == '':
            self.emptyline()
            return ""

        positional_only = (_get_main_action_name(chained_kwargs[0]) not in shell_attrs['_actions'])
        if positional_only:
            return original_line

        ### Apply patch to all kwargs.
        for i, kwargs in enumerate([_ for _ in chained_kwargs]):
            kwargs.update(patches[i])

        from meerschaum._internal.entry import entry
        sysargs_to_execute = []
        for i, kwargs in enumerate(chained_kwargs):
            step_kwargs = {k: v for k, v in kwargs.items() if k != 'line'}
            step_action = kwargs.get('action', None)
            step_action_name = step_action[0] if step_action else None
            ### NOTE: For `stack`, revert argument parsing.
            step_sysargs = (
                parse_dict_to_sysargs(step_kwargs, coerce_dates=False)
                if step_action_name != 'stack'
                else chained_sysargs[i]
            )
            sysargs_to_execute.extend(step_sysargs)
            sysargs_to_execute.append(AND_KEY)

        sysargs_to_execute = sysargs_to_execute[:-1] + (
            ([':'] + pipeline_args) if pipeline_args else []
        )
        try:
            success_tuple = entry(sysargs_to_execute, _patch_args=patch_args)
        except Exception as e:
            success_tuple = False, str(e)

        from meerschaum.utils.formatting import print_tuple
        if isinstance(success_tuple, tuple):
            print_tuple(
                success_tuple,
                skip_common=(not shell_attrs['debug']),
                upper_padding=1,
                lower_padding=0,
            )

        ### Restore the old working directory.
        if old_cwd != os.getcwd():
            os.chdir(old_cwd)

        return ""

    def postcmd(self, stop : bool = False, line : str = ""):
        _reload = shell_attrs['_reload']
        if _reload:
            self.load_config(shell_attrs['instance'])
        if stop:
            return True

    def do_pass(self, line, executor_keys=None):
        """
        Do nothing.
        """

    def do_debug(self, action: Optional[List[str]] = None, executor_keys=None, **kw):
        """
        Toggle the shell's debug mode.
        If debug = on, append `--debug` to all commands.
        
        Command:
            `debug {on/true | off/false}`
            Ommitting on / off will toggle the existing value.
        """
        from meerschaum.utils.warnings import info
        on_commands = {'on', 'true', 'True'}
        off_commands = {'off', 'false', 'False'}
        if action is None:
            action = []
        try:
            state = action[0]
        except (IndexError, AttributeError):
            state = ''
        if state == '':
            shell_attrs['debug'] = not shell_attrs['debug']
        elif state.lower() in on_commands:
            shell_attrs['debug'] = True
        elif state.lower() in off_commands:
            shell_attrs['debug'] = False
        else:
            info(f"Unknown state '{state}'. Ignoring...")

        info(f"Debug mode is {'on' if shell_attrs['debug'] else 'off'}.")

    def do_instance(
        self,
        action: Optional[List[str]] = None,
        executor_keys=None,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
        """
        Temporarily set a default Meerschaum instance for the duration of the shell.
        The default instance is loaded from the Meerschaum configuraton file
          (at keys 'meerschaum:instance').
        
        You can change the default instance with `edit config`.
        
        Usage:
            instance {instance keys}
        
        Examples:
            ```
            ### reset to default instance
            instance
        
            ### set the instance to 'api:main'
            instance api
        
            ### set the instance to a non-default connector
            instance api:myremoteinstance
            ```
        
        Note that instances must be configured, be either API or SQL connections,
          and be accessible to this machine over the network.

        """
        from meerschaum import get_connector
        from meerschaum.connectors.parse import parse_instance_keys
        from meerschaum.utils.warnings import info

        if action is None:
            action = []
        try:
            instance_keys = action[0]
        except (IndexError, AttributeError):
            instance_keys = ''
        if instance_keys == '':
            instance_keys = get_config('meerschaum', 'instance', patch=True)
        if ':' not in instance_keys:
            instance_keys += ':main'

        conn_attrs = parse_instance_keys(instance_keys, construct=False, debug=debug)
        if conn_attrs is None or not conn_attrs:
            conn_keys = str(get_connector(debug=debug))
        else:
            conn_keys = instance_keys

        shell_attrs['instance_keys'] = conn_keys

        self.update_prompt(instance=conn_keys)
        info(f"Default instance for the current shell: {conn_keys}")

        return True, "Success"

    def complete_instance(
        self,
        text: str,
        line: str,
        begin_index: int,
        end_index: int,
        _executor: bool = False,
        _additional_options: Optional[List[str]] = None,
    ):
        from meerschaum.utils.misc import get_connector_labels
        from meerschaum._internal.arguments._parse_arguments import parse_line
        from meerschaum.connectors import instance_types, _load_builtin_custom_connectors
        if not self.__dict__.get('_loaded_custom_connectors', None):
            _load_builtin_custom_connectors()
            self.__dict__['_loaded_custom_connectors'] = True
        from meerschaum.jobs import executor_types

        conn_types = instance_types if not _executor else executor_types

        args = parse_line(line)
        action = args['action']
        _text = action[1] if len(action) > 1 else ""
        return get_connector_labels(
            *conn_types,
            search_term=_text,
            ignore_exact_match=True,
            _additional_options=_additional_options,
        )

    def do_repo(
        self,
        action: Optional[List[str]] = None,
        executor_keys=None,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
        """
        Temporarily set a default Meerschaum repository for the duration of the shell.
        The default repository (mrsm.io) is loaded from the Meerschaum configuraton file
          (at keys 'meerschaum:default_repository').
        
        You can change the default repository with `edit config`.
        
        Usage:
            repo {API label}
        
        Examples:
            ### reset to default repository
            repo
        
            ### set the repo to 'api:main'
            repository api
        
            ### set the repository to a non-default repo
            repo myremoterepo
        
        Note that repositories are a subset of instances.
        """
        from meerschaum import get_connector
        from meerschaum.connectors.parse import parse_repo_keys
        from meerschaum.utils.warnings import info

        if action is None:
            action = []

        try:
            repo_keys = action[0]
        except (IndexError, AttributeError):
            repo_keys = ''
        if repo_keys == '':
            repo_keys = get_config('meerschaum', 'default_repository', patch=True)

        conn = parse_repo_keys(repo_keys, debug=debug)
        if conn is None or not conn:
            conn = get_connector('api', debug=debug)

        shell_attrs['repo_keys'] = str(conn)

        info(f"Default repository for the current shell: {conn}")
        return True, "Success"

    def complete_repo(self, *args) -> List[str]:
        results = self.complete_instance(*args)
        return [result for result in results if result.startswith('api:')]

    def do_executor(
        self,
        action: Optional[List[str]] = None,
        executor_keys=None,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
        """
        Temporarily set a default Meerschaum executor for the duration of the shell.
        
        You can change the default repository with `edit config`.
        
        Usage:
            executor {API label}
        
        Examples:
            ### reset to default executor
            executor
        
            ### set the executor to 'api:main'
            executor api:main
        
        Note that executors are API instances.
        """
        from meerschaum.connectors.parse import parse_executor_keys
        from meerschaum.utils.warnings import warn, info
        from meerschaum.jobs import get_executor_keys_from_context

        if action is None:
            action = []

        try:
            executor_keys = action[0]
        except (IndexError, AttributeError):
            executor_keys = ''
        if executor_keys == '':
            executor_keys = get_executor_keys_from_context()

        if executor_keys == 'systemd' and get_executor_keys_from_context() != 'systemd':
            warn("Cannot execute via `systemd`, falling back to `local`...", stack=False)
            executor_keys = 'local'
        
        conn = parse_executor_keys(executor_keys, debug=debug)

        shell_attrs['executor_keys'] = str(conn).replace('systemd:main', 'systemd')

        info(f"Default executor for the current shell: {executor_keys}")
        return True, "Success"

    def complete_executor(self, *args) -> List[str]:
        from meerschaum.jobs import executor_types
        results = self.complete_instance(*args, _executor=True, _additional_options=['local'])
        return [result for result in results if result.split(':')[0] in executor_types]

    def do_help(self, line: str, executor_keys=None) -> List[str]:
        """
        Show help for Meerschaum actions.
        
        You can also view help for actions and subactions with `--help`.
        
        Examples:
        ```
        help show
        help show pipes
        show pipes --help
        show pipes -h
        ```
        """
        from meerschaum.actions import actions
        from meerschaum._internal.arguments._parser import parse_help
        from meerschaum._internal.arguments._parse_arguments import parse_line
        import textwrap
        args = parse_line(line)
        if len(args['action']) == 0:
            del args['action']
            shell_attrs['_actions']['show'](['actions'], **args)
            return ""
        if args['action'][0] not in shell_attrs['_actions']:
            try:
                print(textwrap.dedent(getattr(self, f"do_{args['action'][0]}").__doc__))
            except Exception:
                print(f"No help on '{args['action'][0]}'.")
            return ""
        parse_help(args)
        return ""

    def complete_help(self, text: str, line: str, begin_index: int, end_index: int):
        """
        Autocomplete the `help` command.
        """
        import inspect
        from meerschaum._internal.arguments._parse_arguments import parse_line
        from meerschaum.actions import get_subactions
        from meerschaum._internal.shell import Shell as _Shell
        args = parse_line(line)
        if len(args['action']) > 0 and args['action'][0] == 'help':
            ### remove 'help'
            del args['action'][0]

        action = args['action'][0] if len(args['action']) > 0 else ""
        sub = args['action'][1] if len(args['action']) > 1 else ""
        possibilities = []

        ### Search for subactions
        if sub is not None:
            for sa in get_subactions(action):
                if sa.startswith(sub) and sa != sub:
                    possibilities.append(sa)
        
        ### We found subarguments. Stop looking here.
        if len(possibilities) > 0:
            return possibilities

        ### No subactions. We're looking for an action.
        for name, f in inspect.getmembers(_Shell):
            if not inspect.isfunction(f):
                continue
            if (
                name.startswith(f'do_{action}')
                    and name != f'do_{action}'
                    and name.replace('do_', '') not in self.hidden_commands
            ):
                possibilities.append(name.replace('do_', ''))
        return possibilities

    def do_exit(self, params, executor_keys=None) -> True:
        """
        Exit the Meerschaum shell.
        """
        return True

    def emptyline(self):
        """
        If the user specifies, clear the screen.

        **NOTE:** The screen clearing is defined in the custom input below
        """

    def preloop(self):
        """
        Patch builtin cmdloop with my own input (defined below).
        """
        cmd.__builtins__['input'] = input_with_sigint(
            _old_input,
            shell_attrs['session'],
            shell = self,
        )

        ### if the user specifies, clear the screen before initializing the shell
        if _clear_screen:
            from meerschaum.utils.formatting._shell import clear_screen
            clear_screen(debug=shell_attrs['debug'])

        ### if sysargs are provided, skip printing the intro and execute instead
        if shell_attrs['_sysargs']:
            shell_attrs['intro'] = ""
            self.precmd(' '.join(shell_attrs['_sysargs']))

    def postloop(self):
        print('\n' + self.close_message)

def input_with_sigint(_input, session, shell: Optional[Shell] = None):
    """
    Replace built-in `input()` with prompt_toolkit.prompt.
    """
    from meerschaum.utils.formatting import CHARSET, ANSI, colored
    from meerschaum.connectors import is_connected, connectors
    from meerschaum.utils.misc import remove_ansi
    from meerschaum.config import get_config
    import platform
    if shell is None:
        from meerschaum.actions import get_shell
        shell = get_shell()

    style = prompt_toolkit_styles.Style.from_dict({
        'bottom-toolbar': 'black',
    })
    last_connected = False
    def bottom_toolbar():
        nonlocal last_connected
        if not get_config('shell', 'bottom_toolbar', 'enabled'):
            return None
        if not shell_attrs['_update_bottom_toolbar'] and platform.system() == 'Windows':
            return shell_attrs['_old_bottom_toolbar']
        size = os.get_terminal_size()
        num_cols, num_lines = size.columns, size.lines

        instance_colored = (
            colored(
                remove_ansi(shell_attrs['instance_keys']),
                'on ' + get_config('shell', 'ansi', 'instance', 'rich', 'style')
            )
            if ANSI
            else colored(shell_attrs['instance_keys'], 'on white')
        )
        repo_colored = (
            colored(
                remove_ansi(shell_attrs['repo_keys']),
                'on ' + get_config('shell', 'ansi', 'repo', 'rich', 'style')
            )
            if ANSI
            else colored(shell_attrs['repo_keys'], 'on white')
        )
        executor_colored = (
            colored(
                remove_ansi(shell_attrs['executor_keys']),
                'on ' + get_config('shell', 'ansi', 'executor', 'rich', 'style')
            )
            if ANSI
            else colored(remove_ansi(shell_attrs['executor_keys']), 'on white')
        )

        try:
            typ, label = shell_attrs['instance_keys'].split(':', maxsplit=1)
            connected = typ in connectors and label in connectors[typ]
        except Exception as e:
            connected = False
        last_connected = connected
        connected_str = (('dis' if not connected else '') + 'connected')
        connection_text = (
            get_config(
                'formatting', connected_str, CHARSET, 'icon'
            ) + ' ' + (
                colored(connected_str.capitalize(), 'on ' + get_config(
                    'formatting', connected_str, 'ansi', 'rich', 'style'
                ) + '  ') if ANSI else (colored(connected_str.capitalize(), 'on white') + ' ')
            )
        )

        left = (
            ' '
            + instance_colored
            + colored(' | ', 'on white')
            + executor_colored
            + colored(' | ', 'on white')
            + repo_colored
        )
        right = connection_text
        buffer_size = (
            num_cols - (len(remove_ansi(left)) + len(remove_ansi(right)) + (2 if ANSI else 0))
        )
        buffer = (' ' * buffer_size) if buffer_size > 0 else '\n '
        text = left + buffer + right
        shell_attrs['_old_bottom_toolbar'] = prompt_toolkit_formatted_text.ANSI(text)
        shell_attrs['_update_bottom_toolbar'] = False
        return shell_attrs['_old_bottom_toolbar']

    def _patched_prompt(*args):
        _args = []
        for a in args:
            try:
                _a = prompt_toolkit_formatted_text.ANSI(a)
            except Exception as e:
                _a = a
            _args.append(_a)
        try:
            parsed = session.prompt(*_args, bottom_toolbar=bottom_toolbar, style=style)
            ### clear screen on empty input
            ### NOTE: would it be better to do nothing instead?
            if len(parsed.strip()) == 0:
                if _clear_screen:
                    from meerschaum.utils.formatting._shell import clear_screen
                    clear_screen()
        except KeyboardInterrupt:
            print("^C")
            return "pass"
        return parsed

    return _patched_prompt
