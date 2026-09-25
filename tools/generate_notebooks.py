"""Deterministically scaffold thin notebook entry points from the sidecar."""

from __future__ import annotations

import json
from pathlib import Path


def code(source: str, *, parameters: bool = False) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "outputs": [],
        "metadata": {"tags": ["parameters"]} if parameters else {},
        "source": source.splitlines(keepends=True),
    }


def save(path: Path, title: str, cells: list[dict]) -> None:
    all_cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# {title}\n",
                "\n",
                "Generated reference wrapper; business logic lives in `spark_dag.components`.\n",
            ],
        },
        *cells,
    ]
    for index, cell in enumerate(all_cells):
        cell["id"] = f"cell-{index}"
    document = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": all_cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def generate(root: Path) -> None:
    config = json.loads((root / "job_config.json").read_text(encoding="utf-8"))
    defaults = {
        "deployment_dir": "",
        "config_file": "job_config.json",
        "environment": "",
        "business_run_key": "",
        "restart_mode": "normal",
        "parent_run_id": "",
        "restart_from": "",
        "reason": "",
        "approved_nodes_json": "[]",
    }
    parameter_code = "".join(f"{key} = {value!r}\n" for key, value in defaults.items())
    arguments = ", ".join(f"{key!r}: {key}" for key in defaults)
    save(
        root / "orchestrator.ipynb",
        "Restartable DAG orchestrator",
        [
            code(parameter_code, parameters=True),
            code(
                "from spark_dag.notebook_runtime import parameter_values, run_orchestrator_notebook, exit_result\n"
                f"parameters = parameter_values({{{arguments}}}, dbutils=globals().get('dbutils'))\n"
                "result = run_orchestrator_notebook(parameters, spark=globals().get('spark'))\n"
            ),
            code("exit_result(result, dbutils=globals().get('dbutils'))\n"),
        ],
    )
    children = [
        (definition["path"], definition["component"])
        for key, definition in config["notebooks"].items()
        if key != "orchestrator"
    ]
    children.append(("child_template.ipynb", "__REGISTER_COMPONENT__"))
    for path, component in children:
        save(
            root / path,
            f"Child notebook: {component}",
            [
                code('context_json = ""\n', parameters=True),
                code(
                    "from spark_dag.notebook_runtime import parameter_values, run_child_notebook, exit_result\n"
                    "parameters = parameter_values({'context_json': context_json}, dbutils=globals().get('dbutils'))\n"
                    f"result = run_child_notebook(parameters['context_json'], {component!r}, spark=globals().get('spark'))\n"
                ),
                code("exit_result(result, dbutils=globals().get('dbutils'))\n"),
            ],
        )


if __name__ == "__main__":
    generate(Path(__file__).resolve().parents[1])
