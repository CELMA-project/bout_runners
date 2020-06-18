"""Package builder for bout_runners."""

import setuptools

# python setup.py sdist bdist_wheel
# twine upload dist/*
# pip install .[tests,fast]
# or on zsh
# pip install ./[tests,fast/]


# .pypirc
# [distutils]
# index-servers =
#     pypi
#
# [pypi]
# repository: https://upload.pypi.org/legacy/
# username:
# password:
setuptools.setup()
