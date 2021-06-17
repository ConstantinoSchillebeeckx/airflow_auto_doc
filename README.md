# airflow_auto_doc

Automatically document Airflow DAGs and Tasks

**NOTE** the Sphinx extension (location in `docs/source/_ext`) will eventually need to be re-written so that it is installed as a standalone plugin.

## Get started

```
pip install -r requirements.txt
cd docs
make html
```

After running the above, you'll find the rendered docs at `docs/build/html/index.html`.

## Usage

This Sphinx extension introduces a new [autodoc](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html) directive that makes documenting DAGs easy; see for example [this file](/docs/source/example_dag.rst):

```
Example DAG
***********

.. autodag:: project.example_dag
```

The example above generates documentation for the DAG available [here](project.example_dag.py). Read more about configuring the documentation in the [extension docstring](docs/source/_ext/dags.py).

