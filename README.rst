|bout_runners|

.. |bout_runners| image:: https://raw.githubusercontent.com/CELMA-project/bout_runners/master/docs/source/_static/logo_full.svg
    :alt: bout runners

=====

|lint| |test| |docker| |codecov| |docs|

|pypi| |python| |license|

|bandit| |code_style| |mypy|

.. |lint| image:: https://github.com/CELMA-project/bout_runners/workflows/Lint/badge.svg?branch=master
    :alt: lint status
    :target: https://github.com/CELMA-project/bout_runners/actions?query=workflow%3A%22Lint%22

.. |test| image:: https://github.com/CELMA-project/bout_runners/workflows/Test/badge.svg?branch=master
    :alt: test status
    :target: https://github.com/CELMA-project/bout_runners/actions?query=workflow%3A%22Test%22

.. |docker| image:: https://github.com/CELMA-project/bout_runners/workflows/Docker/badge.svg?branch=master
    :alt: docker build status
    :target: https://github.com/CELMA-project/bout_runners/actions?query=workflow%3A%22Docker%22

.. |codecov| image:: https://codecov.io/gh/CELMA-project/bout_runners/branch/master/graph/badge.svg
    :alt: codecov percentage
    :target: https://codecov.io/gh/CELMA-project/bout_runners

.. |docs| image:: https://readthedocs.org/projects/bout-runners/badge/?version=latest
    :alt: documentation status
    :target: https://bout-runners.readthedocs.io/en/latest/?badge=latest

.. |pypi| image:: https://badge.fury.io/py/bout-runners.svg
    :alt: pypi package number
    :target: https://pypi.org/project/bout-runners/

.. |python| image:: https://img.shields.io/badge/python->=3.6-blue.svg
    :alt: supported python versions
    :target: https://www.python.org/

.. |license| image:: https://img.shields.io/badge/license-LGPL--3.0-blue.svg
    :alt: licence
    :target: https://github.com/CELMA-project/bout_runners/blob/master/LICENSE

.. |code_style| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :alt: code standard
    :target: https://github.com/psf/black

.. |mypy| image:: http://www.mypy-lang.org/static/mypy_badge.svg
    :alt: checked with mypy
    :target: http://mypy-lang.org/

.. |bandit| image:: https://img.shields.io/badge/security-bandit-yellow.svg
    :alt: security status
    :target: https://github.com/PyCQA/bandit

Manage your BOUT++ runs through python

Overview
--------

BOUT Runners is a package to manage and orchestrate your BOUT++ runs. More
specifically it's a tool:

- Which automatically makes your project
- Where you can override default parameters, and parameters found in ``BOUT.inp``
- Where you can submit a single run, or even a chain of runs
- Which automatically stores parameters and other metadata in a database (inspired by |sacred|_)
- Logs the entire process

Read the full documentation at ReadTheDocs_

.. _ReadTheDocs: https://bout-runners.readthedocs.io/en/latest/
.. |sacred| replace:: ``sacred``
.. _sacred: https://github.com/IDSIA/sacred

Getting Started
---------------

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

Prerequisites
-------------

What things you need to install the software and how to install them

```
Give examples
```

Installing
----------

A step by step series of examples that tell you how to get a development env running

Say what the step will be

.. code:: python

   Give the example


And repeat

.. code:: python

   Until finished

End with an example of getting some data out of the system or using it for a little demo

Running the tests
-----------------

Explain how to run the automated tests for this system

Break down into end to end tests
--------------------------------

Explain what these tests test and why ``Some code``

.. code:: python

   Give
   an
   example
   for code


And coding style tests
----------------------

Explain what these tests test and why

.. code:: python

   Give
   an
   example
   for code

Deployment
----------

Add additional notes about how to deploy this on a live system

Built With
----------


* Dropwizard_ - The web framework used
* Maven_ - Dependency Management
* ROME_ - Used to generate RSS Feeds

.. _Dropwizard: http://www.dropwizard.io/1.0.2/docs/
.. _Maven: http://www.dropwizard.io/1.0.2/docs/
.. _ROME: http://www.dropwizard.io/1.0.2/docs/

Contributing
------------

Please read CONTRIBUTING.rst (FIXME: link to this)

Versioning
----------

Check out auto versioning (symver has been mentioned)

Authors
-------

* **Michael Løiten** - *Initial work*

See also the list of contributors_ who participated in this project.

.. _contributors: https://github.com/CELMA-project/bout_runners/contributors

License
-------

This project is licensed under the GNU LESSER GENERAL PUBLIC LICENSE - see the LICENSE_
file for details

.. _LICENSE: https://github.com/CELMA-project/bout_runners/blob/master/LICENSE

Acknowledgments
---------------

* Hat tip to anyone whose code was used
* Inspiration
* etc

https://docutils.sourceforge.io/docs/user/rst/quickref.html
