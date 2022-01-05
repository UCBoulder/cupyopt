CU's Python Opinionated Prefect Tasks (cupyopt)
===============================================

A package of `Prefect <https://github.com/PrefectHQ/prefect>`_ Tasks and helpers.

Minimum Python 3.6

The following task 'families' are included:

* SFTP Tasks
* ORADB (Oracle Database) Tasks
* Object Storage Tasks

Installation
------------

From pip: pip install git+https://github.com/CUBoulder-OIT/cupyopt.git@master#egg=cupyopt

Alternatively, and importantly for development, clone this into an existing project and install in "development mode" with -e.

git clone https://github.com/CUBoulder-OIT/cupyopt.git
pip install -e cupyopt/

NB, The 'x.y.z' (e.g. 0.12.6) in the versioning matches the versioning in Prefect. However the 4th section is ours to be unique within that version.

Use
---

Import the Task you need similar to:

from cupyopt.objectstore_tasks import ObjstrClient

Tests and Linting
-----------------

::

	make install
	make lint
	make test

Contributing?
-------------

Create an issue, fork the repo, fix an issue, submit a PR. All are welcome. 

Flow practices very roughly hew to one-flow: https://reallifeprogramming.com/git-process-that-works-say-no-to-gitflow-50bf2038ccf7
