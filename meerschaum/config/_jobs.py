#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default configuration for jobs.
"""

default_jobs_config = {
    'timeout_seconds': 8,
    'check_timeout_interval_seconds': 0.1,
    'logs' : {
        'num_files_to_keep': 5,
        'max_file_size': 100_000,
        'lines_to_show': 30,
        'refresh_files_seconds': 5.0,
        'min_buffer_len': 15,
        'colors' : [
            'cyan',
            'magenta',
            'orange3',
            'green',
            'blue',
            'red',
            'spring_green3',
            'medium_purple3',
            'medium_violet_red',
            'slate_blue1', 
            'bright_red',
            'steel_blue3',
            'aquamarine1',
            'dark_khaki',
            'pink3',
            'gold3',
            'pale_green1',
            'light coral',
            'light_goldenrod2',
            'cornsilk1',
            'orange_red1',
            'deep_pink1',
            'aquamarine3',
            'sky_blue2',
            'tan',
            'honeydew2',
        ],
    },
}
