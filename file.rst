Quickstart
==========

This page shows a minimal example of using ``source-rim-pack``.

Importing the package
---------------------

.. code-block:: python

   import rim_pack

Using selected modules
----------------------

Example:

.. code-block:: python

   from rim_pack.source.etl.sqldatabaseconnector import SQLDatabaseConnector

   connector = SQLDatabaseConnector(...)
   result = connector.fetch("select * from example_table")
