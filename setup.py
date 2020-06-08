import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='RepeatFS',
    version='0.9.1',
    author='Anthony Westbrook',
    author_email='anthony.westbrook@unh.edu',
    packages=['repeatfs'],
    scripts=['repeatfs.py'],
    url='http://github.com/ToniWestbrook/repeatfs',
    license='LICENSE',
    description='File system providing reproducibility through provenance and automation',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Langauge :: Python :: 3",
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
