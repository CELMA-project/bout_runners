"""Package builder for bout_runners."""

import setuptools

# python setup.py sdist bdist_wheel
# twine upload dist/*

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
