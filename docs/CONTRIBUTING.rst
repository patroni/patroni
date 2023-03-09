.. _contributing:

Contributing guidelines
=======================

Wanna contribute to Patroni? Yay - here is how!

Chatting
--------

Just want to chat with other Patroni users?  Looking for interactive troubleshooting help? Join us on channel `#patroni <https://postgresteam.slack.com/archives/C9XPYG92A>`__ in the `PostgreSQL Slack <https://join.slack.com/t/postgresteam/shared_invite/zt-1qj14i9sj-E9WqIFlvcOiHsEk2yFEMjA>`__.

Running tests
-------------

Requirements for running behave tests:

1. PostgreSQL packages need to be installed.
2. PostgreSQL binaries must be available in your `PATH`. You may need to add them to the path with something like `PATH=/usr/lib/postgresql/11/bin:$PATH python -m behave`.
3. If you'd like to test with external DCSs (e.g., Etcd, Consul, and Zookeeper) you'll need the packages installed and respective services running and accepting unencrypted/unprotected connections on localhost and default port. In the case of Etcd or Consul, the behave test suite could start them up if binaries are available in the `PATH`.

Install dependencies:

.. code-block:: bash

    # You may want to use Virtualenv or specify pip3.
    pip install -r requirements.txt
    pip install -r requirements.dev.txt

After you have all dependencies installed, you can run the various test suites:

.. code-block:: bash

    # You may want to use Virtualenv or specify python3.

    # Run flake8 to check syntax and formatting:
    python setup.py flake8

    # Run the pytest suite in tests/:
    python setup.py test

    # Run the behave (https://behave.readthedocs.io/en/latest/) test suite in features/;
    # modify DCS as desired (raft has no dependencies so is the easiest to start with):
    DCS=raft python -m behave

Testing with tox
----------------

To run tox tests you only need to install one dependency (other than Python)

.. code-block:: bash

    pip install tox>=4

If you wish to run `behave` tests then you also need docker installed.

Tox configuration in `tox.ini` has "environments" to run the following tasks:

* lint: Python code lint with `flake8`
* test: unit tests for all available python interpreters with `pytest`,
  generates XML reports or HTML reports if a TTY is detected
* dep: detect package dependency conflicts using `pipdeptree`
* type: static type checking with `mypy`
* black: code formatting with `black`
* docker-build: build docker image used for the `behave` env
* docker-cmd: run arbitrary command with the above image
* docker-behave-etcd: run tox for behave tests with above image
* py*behave: run behave with available python interpreters (without docker, although
  this is what is called inside docker containers)
* docs: build docs with `sphinx`

Running tox
^^^^^^^^^^^

To run the default env list; dep, lint, test, and docs, just run:

.. code-block:: bash

    tox

The `test` envs can be run with the label `test`:

.. code-block:: bash

   tox -m test

The `behave` docker tests can be run with the label `behave`:

.. code-block:: bash

   tox -m behave

Similarly, docs has the label `docs`.

All other envs can be run with their respective env names:

.. code-block:: bash

   tox -e lint
   tox -e py39-test-lin

You can list all configured combinations of environments with tox (>=v4) like so

.. code-block:: bash

    tox l

The envs `test` and `docs` will attempt to open the HTML output files
when the job completes, if tox is run with an active terminal. This
is intended to be for benefit of the developer running this env locally.
It will attempt to run `open` on a mac and `xdg-open` on Linux.
To use a different command set the env var `OPEN_CMD` to the name or path of
the command. If this step fails it will not fail the run overall.
If you want to disable this facility set the env var `OPEN_CMD` to the `:` no-op command.

.. code-block:: bash

   OPEN_CMD=: tox -m docs

Reporting issues
----------------

If you have a question about patroni or have a problem using it, please read the :ref:`README <readme>` before filing an issue.
Also double check with the current issues on our `Issues Tracker <https://github.com/zalando/patroni/issues>`__.

Contributing a pull request
---------------------------

1) Submit a comment to the relevant issue or create a new issue describing your proposed change.
2) Do a fork, develop and test your code changes.
3) Include documentation
4) Submit a pull request.

You'll get feedback about your pull request as soon as possible.

Happy Patroni hacking ;-)
