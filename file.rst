Installation
============

Requirements
------------

``source-rim-pack`` requires Python 3.8 or newer.

Basic installation
------------------

Install the package using pip:

.. code-block:: bash

   pip install source-rim-pack

Development installation
------------------------

If you are working with the source code, clone the repository and install the
package in editable mode:

.. code-block:: bash

   git clone <repository-url>
   cd source-rim-pack
   pip install -e .

Optional dependencies
---------------------

Some modules may require additional dependencies, for example Spark, HDFS,
database connectors or dashboard-related libraries.

.. code-block:: bash

   pip install pyspark sqlalchemy pandas
