#!/usr/bin/env python3
"""
Documentation builder script.
"""

import os
import subprocess

CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.join(CURRENT_PATH, 'source')
BUILD_DIR = os.path.join(CURRENT_PATH, 'build')


def run_build(target: str):
    subprocess.run(['sphinx-build', '-b', target, SOURCE_DIR, BUILD_DIR])


if __name__ == '__main__':
    run_build('html')

