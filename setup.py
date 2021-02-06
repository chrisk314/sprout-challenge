#!/usr/bin/env python3

from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='task_scheduler',
    version='0.1',
    description='Scheduler for Sprout tasks.',
    author="Chris Knight",
    author_email="chrisk314@gmail.com",
    packages=find_packages(),
    install_requires=requirements,
)
