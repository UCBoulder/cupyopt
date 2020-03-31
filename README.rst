cupyopt
=======

CU's Python Opinionated Prefect Tasks

A package of Prefect Tasks and helpers

Minimum Python 3.6

Tasks provided:

SFTP Tasks:
- SFTPGet
- SFTPRemove
- SFTPPoll
- DFGetOldestFile

ORADB Tasks:
- ORADBGetEngine

Installation
------------

From pip: pip install git+https://bitbucket.colorado.edu/scm/datasvcs/cupyopt.git@master#egg=cupyopt

Alternatively, and importantly for development, clone this into an existing project and install in "development mode" with -e.

git clone https://bitbucket.colorado.edu/scm/datasvcs/cupyopt.git
pip install -e cupyopt/

Use
---

Import the Task you need similar to,

from cupyopt import SFTPGet, ORADBGetEngine

