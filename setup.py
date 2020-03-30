import pathlib
import setuptools
 
# The directory containing this file
HERE = pathlib.Path(__file__).parent
 
# The text of the README file
README = (HERE / "README.rst").read_text()
 
# This call to setup() does all the work
setuptools.setup(
    name="cupyopt",
    version="0.0.1",
    description="CU Python Opionated Prefect Tasks",
    long_description=README,
    long_description_content_type="text/x-rst",
    author="CU Boulder, OIT",
    author_email="stta9820@colorado.edu",
    license="Apache License 2.0",
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.6"