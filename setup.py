"""Package builder for bout_runners."""

import re
from pathlib import Path

import setuptools

import bout_runners

NAME = "bout_runners"
PROJECT = "CELMA-project"
KEYWORDS = [
    "bout++",
    "bout",
    "python wrapper",
    "metadata recorder",
    "plasma",
    "turbulence",
]
INSTALL_REQUIRES = ["pyyaml", "pandas", "python-dotenv"]

ROOT_PATH = Path(__file__).parent
INIT_PATH = ROOT_PATH.joinpath(NAME, "__init__.py")
README_PATH = ROOT_PATH.joinpath("README.md")

# https://packaging.python.org/guides/single-sourcing-package-version/
with INIT_PATH.open("r") as f:
    VERSION_FILE = f.read()
    VERSION_MATCH = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", VERSION_FILE, re.M)
    if VERSION_MATCH:
        VERSION = VERSION_MATCH.group(1)
    else:
        raise RuntimeError("Unable to find version string.")

with README_PATH.open("r") as f:
    LONG_DESCRIPTION = f.read()

setuptools.setup(
    name=NAME,
    version=VERSION,
    author="Michael Løiten",
    author_email="michael.l.magnussen@gmail.com",
    description=bout_runners.__doc__,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url=f"https://github.com/{PROJECT}/{NAME}",
    packages=setuptools.find_packages(),
    keywords=KEYWORDS,
    install_requires=INSTALL_REQUIRES,
    package_data={
        # Include all .ini files
        "": ["*.ini"]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        (
            "License :: OSI Approved :: "
            "GNU Lesser General Public License v3 or later (LGPLv3+)"
        ),
        "Operating System :: OS Independent",
    ],
)
