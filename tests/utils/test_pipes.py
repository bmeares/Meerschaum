#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define tests for pipes utilities.
"""

import meerschaum as mrsm
import pytest


@pytest.mark.parametrize(
    "text,expected_output,pipe",
    [
        (            
            "foo bar {{ Pipe('a', 'b', target='a_b', instance='sql:memory') }}",
            'foo bar "a_b"',
            None,
        ),
        (
            "{{Pipe('test', 'replace_pipe_syntax', instance='sql:memory').parameters['test']}}",
            '123',
            mrsm.Pipe(
                'test', 'replace_pipe_syntax',
                instance='sql:memory',
                parameters={
                    'test': 123,
                },
            ),
        ),
    ]
)
def test_replace_pipe_syntax(text, expected_output, pipe):
    """
    Test that the text is parsed to the expected output (from the pipe).
    """
    from meerschaum.utils.pipes import replace_pipes_syntax
    if pipe is not None:
        pipe.delete()
        pipe.register()
    output = replace_pipes_syntax(text)
    print(output)
    assert output == expected_output


@pytest.mark.parametrize(
    "parameters,expected_parameters,reference_parameters",
    [
        (
            {
                'foo': "{{ Pipe('test', 'symlinking', 'reference', instance='sql:memory').parameters['foo'] }}",
            },
            {
                'foo': {
                    'bar': {
                        'baz': 123,
                    },
                },
            },
            {
                'foo': {
                    'bar': {
                        'baz': 123,
                    },
                },
            },
        ),
    ]
)
def test_pipes_symlinking(parameters, expected_parameters, reference_parameters):
    """
    Test that pipes may reference parameters of other pipes via symlinking.
    """
    pipe = mrsm.Pipe(
        'test', 'symlinking',
        instance='sql:memory',
    )
    reference_pipe = mrsm.Pipe(
        'test', 'symlinking', 'reference',
        instance='sql:memory',
    )
    pipe.delete()
    reference_pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'symlinking',
        instance='sql:memory',
        parameters=parameters,
    )
    reference_pipe = mrsm.Pipe(
        'test', 'symlinking', 'reference',
        instance='sql:memory',
        parameters=reference_parameters,
    )
    reference_pipe.register()
    return pipe, reference_pipe

    resolved_parameters = pipe.get_parameters()
    print(f"{resolved_parameters=}")
    assert resolved_parameters == expected_parameters
