"""Execute a Jupyter notebook and checkpoint outputs after every cell."""

import json
from datetime import UTC, datetime
from pathlib import Path

import nbformat
from nbclient import NotebookClient


def run_notebook(source, output, *, cloud_output=None, timeout=7200):
    """Run through a Jupyter kernel; preserve failed cells and return a status."""
    source, output = Path(source), Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    notebook = nbformat.read(source, as_version=4)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None
    notebook.metadata["execution_status"] = "running"
    notebook.metadata["started_at_utc"] = datetime.now(UTC).isoformat()

    def checkpoint(**_):
        temporary = output.with_suffix(".ipynb.tmp")
        nbformat.write(notebook, temporary)
        temporary.replace(output)
        if cloud_output:
            from google.cloud import storage

            bucket, key = cloud_output.removeprefix("gs://").split("/", 1)
            storage.Client().bucket(bucket).blob(key).upload_from_filename(str(output))

    checkpoint()
    status = 0
    try:
        NotebookClient(
            notebook,
            timeout=timeout,
            kernel_name="python3",
            resources={"metadata": {"path": str(source.parent)}},
            on_cell_executed=checkpoint,
        ).execute()
        notebook.metadata["execution_status"] = "completed"
    except Exception as error:  # noqa: BLE001 — save the failed notebook before returning.
        status = 1
        notebook.metadata["execution_status"] = "failed"
        notebook.metadata["execution_error_type"] = type(error).__name__
        print(
            f"Notebook failed: {type(error).__name__}; inspect its saved error output."
        )
    finally:
        notebook.metadata["finished_at_utc"] = datetime.now(UTC).isoformat()
        checkpoint()
        output.with_suffix(".status.json").write_text(
            json.dumps(dict(notebook.metadata), indent=2) + "\n"
        )
    print(f"Notebook {notebook.metadata['execution_status']}: {output.name}")
    return status
