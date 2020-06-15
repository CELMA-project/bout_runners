# pylint: skip-file
# NOTE: Ideally we'd ignore this file in pylintrc, but there seem to be an open issue
#       https://github.com/PyCQA/pylint/issues/2686
"""
Configuration file for the Sphinx documentation builder.

References
----------
https://www.sphinx-doc.org/en/master/usage/configuration.html
"""

import re
import sys
from pathlib import Path

root_path = Path(__file__).absolute().parents[2]
sys.path.insert(0, str(root_path))


# Project information
project = "bout_runners"
author = "Løiten, Michael"
copyright = f"2019; {author}"
# Automatic detection of release
init_path = root_path.joinpath(project, "__init__.py")
with init_path.open("r") as f:
    version_file = f.read()
    release_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if release_match:
        release = release_match.group(1)
    else:
        raise RuntimeError("Unable to find version string.")


# General configuration
master_doc = "index"  # Where sphinx should look for the documentation
extensions = [
    "sphinx.ext.autosummary",  # Summary of documentation
    "sphinx.ext.viewcode",  # Add [source] link
    "numpydoc",  # Convert numpy to rst
    "sphinx_rtd_theme",  # Gives read the docs theme
]
exclude_patterns = []  # When looking for source files
templates_path = ["_templates"]  # Templates relative to this direction
pygments_style = "friendly"  # How code is rendered

# Extensions
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autodoc_default_options
autodoc_default_options = {
    "members": True,  # Include auto documentation of members
    "inherited-members": True,  # Include inherited members
}
autoclass_content = "both"  # Include __init__ in the documentation
autosummary_generate = True  # Generate autosummary


# HTML output
html_theme = "sphinx_rtd_theme"  # One of many possibilities
html_static_path = ["_static"]
html_logo = str(root_path.joinpath("docs", "source", "_static", "logo_b.svg"))
