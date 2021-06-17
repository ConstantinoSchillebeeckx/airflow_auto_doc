# -*- coding: utf-8 -*-
"""Sphinx extension for documenting DAGs

This extends the `autodoc extension <https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html>`_ by
creating an ``autodag`` directive.

Use it in restructured text like::

    .. autodag:: dags.dag_name

Doing so will generate a documentation page populated by the DAG's docstring along with:

    * a Graphviz diagram of the DAG
    * the DAG's schedule
    * docs for each task in the DAG

Furthermore, there are multiple options to configure the daigram's output formatting:

    * hide_diagram: whether to omit rendering the Graphiz diagram; defaults to False
    * hide_tasks: whether to exclude the list of tasks; defaults to False
    * hide_schedule: whether to exclude the schedule; defaults to False
    * title: diagram title (shown on top); defaults to None
    * caption: diagram caption (shown underneath diagram); defaults to None

Usage with options looks like::

    .. autodag:: dags.dag_name
        :hide_tasks:

.. seealso::

    `Graphviz extension <https://www.sphinx-doc.org/en/master/usage/extensions/graphviz.html>`_
    documentation for configuring the DAG diagrams; for example, add the following lines to conf.py to orient i
    graphs top to bottom and render them as SVG (instead of PNG)::

        graphviz_dot_args = ['-Grankdir="TB"']
        graphviz_output_format = 'svg'

"""
import os
from pathlib import Path

import m2r
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.dot_renderer import render_dag
from cron_descriptor import get_description
#from docutils.parsers.rst import Directive
from sphinx.ext.autodoc import ClassDocumenter
from sphinx.ext.autodoc import ModuleDocumenter
from sphinx.ext.autodoc.importer import get_module_members
from sphinx.ext.autodoc.importer import import_object
from sphinx.util import logging


logger = logging.getLogger(__name__)


def qual_name(x):
    return f"{x.__module__}.{type(x).__name__}"


class DagTaskDocumenter(ClassDocumenter):
    """DagTaskDocumenter

    Subclasses :class:`sphinx.ext.autodoc.ClassDocumenter` by extending the
    :meth:`sphinx.ext.autodoc.ClassDocumenter.generate` method to display the:

    * associated :attr:`doc_rst` for the task (if available)
    * type of task :class:`airflow.operators`, with hyperlink to Airflow source code

    """

    def generate(self, *args, **kwargs):
        super(DagTaskDocumenter, self).generate(*args, **kwargs)

        sn = self.get_sourcename()
        op = self.object

        # use doc_rst if available, otherwise try the auto_doc property
        if hasattr(op, "doc_rst") and isinstance(op.doc_rst, str):
            for i, l in enumerate(op.doc_rst.split("\n")):
                self.add_line(l, sn, i)
        if hasattr(op, "doc_md") and isinstance(op.doc_md, str):
            for i, l in enumerate(m2r.convert(op.doc_md).split("\n")):
                self.add_line(l, sn, i)
        elif hasattr(op, "auto_doc") and isinstance(op.auto_doc, str):
            self.add_line(" ", sn)
            for i, l in enumerate(op.auto_doc.split("\n")):
                self.add_line(l, sn, i)
            self.add_line(" ", sn)

        self.add_line(" ", sn)
        self.add_line(f":Operator: :class:`~{qual_name(op)}`", sn)


class DagDocumenter(ModuleDocumenter):
    """DagDocumenter

    Subclasses :class:`sphinx.ext.autodoc.ModuleDocumenter` by extending the
    :meth:`sphinx.ext.autodoc.ModuleDocumenter.generate` method to:

    * include the schedule interval
    * include a dot diagram of the DAG
    * all tasks rendered as :class:`DagTaskDocumenter`

    """

    objtype = "dag"
    directivetype = "module"
    optional_arguments = 5
    option_spec = {
        "hide_diagram": bool,  # default False
        "hide_tasks": bool,  # default False
        "hide_schedule": bool,  # default False
        "title": str,  # defaults to None
        "caption": str,  # defaults to None
    }

    def get_dag_tasks(self, dag: DAG) -> list:
        """Get the DAG's tasks

        Returns:
            tasks: list of :class:`airflow.operators.BaseOperator` associated with the given `dag`
        """

        tasks = []
        for mbr_name, mbr in get_module_members(self.object):
            if issubclass(type(mbr), BaseOperator) and mbr.dag == dag:
                tasks.append((mbr_name, mbr))

        if not len(tasks):
            logger.warning(f"Could not find a single task for DAG {dag.dag_id}")

        return tasks

    def get_dags(self) -> list:
        """Get any DAG attribute(s) in the module

        Iterates over all the module members and returns a list of all those
        that are subclases of :class:`airflow.DAG`

        Returns:
            dags: list of :class:`airflow.DAG` found
        """

        dags = []
        for mbr_name, mbr in get_module_members(self.object):
            if issubclass(type(mbr), DAG):
                dags.append(mbr)

        if not len(dags):
            logger.warning(f"Could not find a single DAG in {self.env.docname}.rst")

        return dags

    def add_title(self, title: str, sep: str) -> None:
        """Adds a section header

        Args:
            title: section string to write
            sep: punctuation character for section, must be one of #, *, =, -, ^, ~
        """

        allowed = "#*=-^~"
        assert sep in allowed, f"Title separator must be one of `{allowed}`; instead received `{sep}`"

        sn = self.get_sourcename()
        self.add_line(" ", sn)
        self.add_line(title, sn)
        self.add_line(sep * len(title), sn)
        self.add_line(" ", sn)

    def add_schedule(self, dag: DAG, title="Schedule interval") -> None:
        """Adds a section for the DAG's schedule

        DAG's schedule section formatted like::

            Schedule interval
                human-readable description

        Args:
            dag: DAG for which to add tasks
            title (optional): string used as header for section; defaults to
                "Schedule interval"
        """
        sn = self.get_sourcename()

        self.add_line(title, sn)
        if dag.schedule_interval is not None:
            self.add_line(f"  {get_description(dag.schedule_interval)}", sn)
        else:
            self.add_line(f"  None", sn)

    def add_tasks(self, dag: DAG, title="Tasks") -> None:
        """Adds a section for the DAG's tasks

        Will render :class:`DagTaskDocumenter` for each task of the given DAG.

        Args:
            dag: DAG for which to add tasks
            title (optional): string used as header for section; defaults to
                "Tasks"
        """
        tasks = self.get_dag_tasks(dag)

        for i, (t_name, t_class) in enumerate(tasks):

            if i == 0:
                self.add_title(title, "=")

            DagTaskDocumenter(self.directive, f"{self.fullname}.{t_name}").generate()

    def add_diagram(self, dag: DAG, heading="Diagram", title=None, caption=None) -> None:
        """Adds a Graphiz diagram of the DAG

        Will write a {dag.dag_id}.dot file to the html_static_path configured for this Sphinx
        project; this file will then be referenced in a Graphviz directive.

        .. seealso::

            `Graphviz extension <https://www.sphinx-doc.org/en/master/usage/extensions/graphviz.html>`_
            `html_static_path <https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-html_static_path>`_

        Args:
            dag: DAG for which to add tasks
            heading (optional): string used as header for section; defaults to "Diagram"
            title (optional): diagram title; defaults to None (no title)
            caption (optional) diagram caption (shown underneath diagram); defaults to None
        """

        sn = self.get_sourcename()

        static = self.env.config.html_static_path[0]

        # path to rst that calls autodag
        caller = f"{self.env.doc2path(self.env.docname)}"

        graph_out = f"{self.env.srcdir}/{static}/{dag.dag_id}.dot"

        logger.info(f"Writing to {graph_out}")

        dot = render_dag(dag)
        # label automatically defaults to dag_id
        dot.graph_attr["label"] = title
        dot.save(graph_out)

        self.add_line(heading, sn)
        self.add_line(f"  .. graphviz:: {os.path.relpath(graph_out, Path(caller).parent)}", sn)

        if caption:
            self.add_line(f"    :caption: {caption}", sn)

    def add_md_docstring(self, md: list) -> None:
        """Add module's docstring

        DAGs only support markdown docstring; so we use this to convert it to rst,
        and submit that as the actual docstring

        Also adds a python module domain so that the dag can be referenced like::

            :mod:`dags.dag_name`

        Args:
            md: list of lists, markdown docstring, return value of :meth:`get_doc``
        """
        sn = self.get_sourcename()

        self.add_line(f".. py:module:: {self.fullname}", sn)
        self.add_line("", sn)

        if len(md):
            doc_string = m2r.convert("\n".join(md[0]))

            self.add_line("", sn)
            for line in doc_string.split("\n"):
                self.add_line(line, sn)

    def generate(self, *args, **kwargs):

        self.parse_name()
        self.import_object()

        self.add_md_docstring(self.get_doc())

        # are multiple DAGs even allowed per file?
        for dag in self.get_dags():
            if not self.options.get("hide_diagram", False):
                self.add_diagram(
                    dag, title=self.options.get("title", None), caption=self.options.get("caption", None)
                )
            if not self.options.get("hide_schedule", False):
                self.add_schedule(dag)

            if not self.options.get("hide_tasks", False):
                self.add_tasks(dag)


def setup(app):
    app.add_autodocumenter(DagDocumenter)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
