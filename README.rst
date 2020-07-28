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

From pip: pip install git+https://github.com/CUBoulder-OIT/cupyopt.git@master#egg=cupyopt

Alternatively, and importantly for development, clone this into an existing project and install in "development mode" with -e.

git clone https://github.com/CUBoulder-OIT/cupyopt.git
pip install -e cupyopt/

Use
---

Import the Task you need similar to,

from cupyopt import SFTPGet, ORADBGetEngine

Contributing?
-------------

Create an issue, submit a PR. All are welcome. Flow practices very roughly hew to one-flow: https://reallifeprogramming.com/git-process-that-works-say-no-to-gitflow-50bf2038ccf7
