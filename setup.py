""" Setup for cupyopt """
import os
import pathlib

import setuptools

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.rst").read_text()

# Pull requirements from the text file
REQUIREMENT_PATH = HERE / "requirements.txt"
INSTALL_REQUIRES = []
if os.path.isfile(REQUIREMENT_PATH):
    with open(REQUIREMENT_PATH) as f:
        INSTALL_REQUIRES = f.read().splitlines()

# This call to setup() does all the work
setuptools.setup(
    name="cupyopt",
    version="0.15.13.1",
    description="CU Python Opinionated Prefect Tasks",
    long_description=README,
    long_description_content_type="text/x-rst",
    author="CU Boulder, OIT",
    author_email="stta9820@colorado.edu",
    license="Apache License 2.0",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    package_dir={"": "src"},
    install_requires=INSTALL_REQUIRES,
)
