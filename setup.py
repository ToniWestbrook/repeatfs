#! /usr/bin/env python3

#
#   This file is part of RepeatFS
#
#   SPDX-FileCopyrightText: 2020  Anthony Westbrook, University of New Hampshire <anthony.westbrook@unh.edu>
#
#   SPDX-License-Identifier: GPL-3.0-only WITH LicenseRef-repeatfs-graphviz-linking-source-exception
#


import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='RepeatFS',
    version='0.9.3',
    author='Anthony Westbrook',
    author_email='anthony.westbrook@unh.edu',
    packages=setuptools.find_packages(),
    scripts=['scripts/repeatfs'],
    url='http://github.com/ToniWestbrook/repeatfs',
    license='LICENSE',
    description='File system providing reproducibility through provenance and automation',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
    ],
    install_requires=[
        "fusepy >= 3.0.1",
        "python-daemon >= 2.2.0",
        "pygraphviz >= 1.3.1",
    ],
)
