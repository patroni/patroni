.. _contributing_guidelines:

Contributing guidelines
=======================

.. _chatting:

Chatting
--------

If you have a question, looking for an interactive troubleshooting help or want to chat with other Patroni users, join us on channel `#patroni <https://postgresteam.slack.com/archives/C9XPYG92A>`__ in the `PostgreSQL Slack <https://pgtreats.info/slack-invite>`__.

.. _reporting_bugs:

Reporting bugs
--------------

Before reporting a bug please make sure to **reproduce it with the latest Patroni version**!
Also please double check if the issue already exists in our `Issues Tracker <https://github.com/zalando/patroni/issues>`__.

Running tests
-------------

Requirements for running behave tests:

#. PostgreSQL packages including `contrib <https://www.postgresql.org/docs/current/contrib.html>`__ modules need to be installed.
#. PostgreSQL binaries must be available in your `PATH`. You may need to add them to the path with something like `PATH=/usr/lib/postgresql/11/bin:$PATH python -m behave`.
#. If you'd like to test with external DCSs (e.g., Etcd, Consul, and Zookeeper) you'll need the packages installed and respective services running and accepting unencrypted/unprotected connections on localhost and default port. In the case of Etcd or Consul, the behave test suite could start them up if binaries are available in the `PATH`.

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

    # Moreover, you may want to run tests in different scopes for debugging purposes,
    # the -s option include print output during test execution.
    # Tests in pytest typically follow the pattern: FILEPATH::CLASSNAME::TESTNAME.
    pytest -s tests/test_api.py
    pytest -s tests/test_api.py::TestRestApiHandler
    pytest -s tests/test_api.py::TestRestApiHandler::test_do_GET

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
* type: static type checking with `pyright`
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

It is also possible to select partial env lists using `factors`. For example, if you want to run
all envs for python 3.10:

.. code-block:: bash

    tox -f py310

This is equivalent to running all the envs listed below:

.. code-block:: bash

    $ tox -l -f py310
    py310-test-lin
    py310-test-mac
    py310-test-win
    py310-type-lin
    py310-type-mac
    py310-type-win
    py310-behave-etcd-lin
    py310-behave-etcd-win
    py310-behave-etcd-mac


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

Behave tests
^^^^^^^^^^^^

Behave tests with `-m behave` will build docker images based on PG_MAJOR version 11 through 16 and then run all
behave tests. This can take quite a long time to run so you might want to limit the scope to a select version of
Postgres or to a specific feature set or steps.

To specify the version of postgres include the full name of the dependent image build env that you want and then the
behave env name. For instance if you want Postgres 14 use:

.. code-block:: bash

    tox -e pg14-docker-build,pg14-docker-behave-etcd-lin

If on the other hand you want to test a specific feature you can pass positional arguments to behave. This will run
the watchdog behave feature test scenario with all versions of Postgres.

.. code-block:: bash

    tox -m behave -- features/watchdog.feature

Of course you can combine the two.

Contributing a pull request
---------------------------

#. Fork the repository, develop and test your code changes.
#. Reflect changes in the user documentation.
#. Submit a pull request with a clear description of the changes objective. Link an existing issue if necessary.

You'll get feedback about your pull request as soon as possible.

Happy Patroni hacking ;-)
