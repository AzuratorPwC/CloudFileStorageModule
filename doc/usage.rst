
Usage
=====


.. py:currentmodule:: cloud-storage-pwc

A :class:`DeltaTable` AAA


.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/delta-0.2.0")
    >>> dt.version()
    3
    >>> dt.files()
    ['part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet', 
     'part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet', 
     'part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet']


Custom Storage Backends
~~~~~~~~~~~~~~~~~~~~~~~


przyklad uzycia https://raw.githubusercontent.com/delta-io/delta-rs/main/python/docs/source/usage.rst
https://delta-io.github.io/delta-rs/python/usage.html#custom-storage-backends