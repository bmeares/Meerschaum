#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generate random names for jobs.
"""

from __future__ import annotations
import os, random
from meerschaum.utils.typing import Dict, List, Tuple
from meerschaum.config._paths import DAEMON_RESOURCES_PATH

_bank : Dict[str, Dict[str, List[str]]] = {
    'adjectives' : {
        'colors' : [
            'red', 'blue', 'green', 'orange', 'pink', 'amber',
            'bright', 'dark', 'neon',
        ],
        'sizes' : [
            'big', 'small', 'large', 'huge', 'tiny', 'long', 'short', 'grand', 'mini', 'micro'
        ],
        'personalities' : [
            'groovy', 'cool', 'awesome', 'nice', 'fantastic', 'sweet', 'great', 'amazing',
            'terrific', 'funky', 'fancy', 'sneaky', 'elegant', 'dreamy',
        ],
        'emotions' : [
            'angry', 'happy', 'excited', 'suspicious', 'sad', 'thankful', 'grateful', 'satisfied',
        ],
        'sensations' : [
            'sleepy', 'awake', 'alert', 'thirsty', 'comfy', 'warm', 'cold', 'chilly', 'soft',
            'smooth', 'chunky',
        ],
        'materials' : [
            'golden', 'silver', 'metal', 'plastic', 'wool', 'wooden', 'nylon', 'fuzzy', 'silky',
        ],
        'qualities' : [
            'expensive', 'cheap', 'premier', 'best', 'favorite', 'better', 'good', 'affordable',
        ],
    },
    'nouns' : {
        'animals' : [
            'mouse', 'fox', 'horse', 'dragon', 'pig', 'hippo', 'elephant' , 'tiger', 'deer',
            'gerbil', 'snake', 'turtle', 'rhino', 'dog', 'cat', 'giraffe', 'rabbit', 'squirrel',
            'unicorn', 'lizard', 'lion', 'bear', 'gazelle', 'whale', 'dolphin', 'fish', 'butterfly',
            'ladybug', 'fly', 'shrimp', 'flamingo', 'parrot', 'tuna', 'panda', 'lemur', 'duck',
            'seal', 'walrus', 'seagull', 'iguana', 'salamander', 'kitten', 'puppy', 'octopus',
        ],
        'plants' : [
            'tree', 'flower', 'vine', 'fern', 'palm', 'palmetto', 'oak', 'pine', 'rose', 'lily',
            'ivy',
        ],
        'foods' : [
            'pizza', 'sushi', 'apple', 'banana', 'sandwich', 'burger', 'taco', 'bratwurst',
            'grape', 'coconut', 'bread', 'toast',
        ],
        'geographies' : [
            'ocean', 'mountain', 'desert', 'forest', 'tundra', 'savanna', 'grassland', 'prairie',
            'lake', 'city', 'river',
        ],
        'vehicles' : [
            'car', 'bike', 'boat', 'bus', 'trolley', 'tram', 'plane', 'skates', 'skateboard',
            'kayak', 'canoe', 'paddleboard', 'skis', 'snowboard', 'truck', 'bicycle', 'unicycle',
            'tricycle',
        ],
        'clothing' : [
            'socks', 'shirt', 'dress', 'shoes', 'hat', 'glasses', 'pocket', 'shorts', 'pants',
            'skirt', 'capris', 'helmet',
        ],
        'instruments' : [
            'guitar', 'piano', 'trombone', 'drums', 'viola', 'violin', 'trumpet', 'bass',
            'harmonica', 'banjo',
        ],
    },
}

_disallow_combinations : List[Tuple[str, str]] = [
    #  ('colors', 'animals'), ('emotions', 'animals'),
]

_adjectives : List[str]= []
for category, items in _bank['adjectives'].items():
    _adjectives += items

_nouns : List[str] = []
for category, items in _bank['nouns'].items():
    _nouns += items

def generate_random_name(seperator : str = '_'):
    """
    Return a random combination of names.
    """
    while True:
        adjective_category = random.choice(list(_bank['adjectives'].keys()))
        noun_category = random.choice(list(_bank['nouns'].keys()))
        if (adjective_category, noun_category) in _disallow_combinations:
            continue
        break
    return (
        random.choice(_bank['adjectives'][adjective_category])
        + seperator
        + random.choice(_bank['nouns'][noun_category])
    )

def get_new_daemon_name():
    """
    Return a new random Daemon name.

    Validates that generated names are unique, up to ~6000 maximum possibilities.
    """
    existing_names = os.listdir(DAEMON_RESOURCES_PATH)
    while True:
        _name = generate_random_name()
        if _name in existing_names:
            continue
        break
    return _name
