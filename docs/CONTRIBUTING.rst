.. _contributing:

Contributing guidelines
=======================

Wanna contribute to Patroni? Yay - here is how!

Chatting
--------

Just want to chat with other Patroni users?  Looking for interactive troubleshooting help? Join us on channel #patroni in the `PostgreSQL Slack <https://postgres-slack.herokuapp.com/>`__.

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
