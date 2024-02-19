# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import pathlib
import sys
import os
#sys.path.insert(0, pathlib.Path(__file__).parents[2].resolve().as_posix()+"/src/cloud_storage_pwc/__init__.py")
#sys.path.append("C:/Users/astarosta001/repos/BASDATA/StorageAccountPythonLib/src/lumache.py")
#from cloud_storage_pwc import create_cloudstorage_reference
#sys.path.append('../../src/cloud_storage_pwc')

#from src.cloud_storage_pwc import azure_storage
#sys.path.insert(0, pathlib.Path(__file__).parents[2].resolve().as_posix())
#sys.path.append('/home/workspaces/CloudFileStorageModule/src')
sys.path.insert(0, os.path.abspath('..'))

project = 'cloud-storage'
copyright = '2022, artur'
author = 'artur'
release = '0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
            'sphinx.ext.duration',
            'sphinx.ext.doctest',
            'sphinx.ext.autodoc',
            'sphinx.ext.autosummary',
]

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
