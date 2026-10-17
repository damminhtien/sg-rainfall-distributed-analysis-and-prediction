"""Run the frozen v2 development and final notebooks sequentially on Dataproc."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


BUCKET = os.environ.get("BUCKET", "sg-rainfall-dsa5208-261011-data")
RUN_ID = os.environ.get("RUN_ID", "")
ROOT = f"gs://{BUCKET}/runs/{RUN_ID}"
ARTIFACT_ROOT = f"{ROOT}/results/overnight_execution"
WORK_DIR = Path(os.environ.get("RUN_WORK_DIR", f"/var/tmp/dsa5208-{RUN_ID}"))
NOTEBOOKS = (
    ("03_model_development.ipynb", "notebook_03_model_development"),
    ("04_final_fit_evaluate.ipynb", "notebook_04_final_fit_evaluate"),
)


def gcloud(args: list[str]) -> None:
    subprocess.run(["gcloud", *args], check=True, text=True)


def upload(path: Path, uri: str) -> None:
    gcloud(["storage", "cp", str(path), uri])


def notebook_code(path: Path) -> str:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    cells = []
    for index, cell in enumerate(notebook.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", "")
        source = "".join(source) if isinstance(source, list) else source
        for line in source.splitlines():
            if line.lstrip().startswith(("%", "!")) or "get_ipython()" in line:
                raise ValueError(f"Unsupported notebook magic in {path.name}, cell {index}")
        cells.append(f"# Notebook code cell {index}\n{source}")
    if not cells:
        raise ValueError(f"Notebook has no code cells: {path}")
    return "\n\n".join(cells) + "\n"


def write_status(status: dict) -> None:
    status["updated_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    path = WORK_DIR / "status.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)
    upload(path, f"{ARTIFACT_ROOT}/status.json")


def require_gcs_object(uri: str) -> None:
    subprocess.run(
        ["gcloud", "storage", "objects", "describe", uri],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def main() -> int:
    if not re.fullmatch(r"\d{8}-v2-[a-z0-9-]+", RUN_ID):
        raise ValueError("Set RUN_ID to a fresh, versioned v2 run prefix.")

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    required = (
        f"{ROOT}/config/experiment.json",
        f"{ROOT}/config/input_manifest.json",
        f"{ROOT}/config/source_hashes.json",
        f"{ROOT}/base_features/_SUCCESS",
        *(f"{ROOT}/source/notebooks/{name}" for name, _ in NOTEBOOKS),
    )
    for uri in required:
        require_gcs_object(uri)

    spec_uri = f"{ROOT}/artifacts/frozen_spec.json"
    existing_spec = subprocess.run(
        ["gcloud", "storage", "objects", "describe", spec_uri],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if existing_spec.returncode == 0:
        raise FileExistsError(f"Refusing to reuse run with frozen spec: {spec_uri}")
    if "not found" not in existing_spec.stderr.lower() and "no objects" not in existing_spec.stderr.lower():
        raise RuntimeError(f"Could not verify frozen spec absence: {existing_spec.stderr}")

    status = {
        "run_id": RUN_ID,
        "state": "running",
        "execution_mode": "persistent_dataproc_master_sequential_spark_submit",
        "notebooks": {},
        "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    write_status(status)

    try:
        for notebook_name, job_name in NOTEBOOKS:
            notebook_path = WORK_DIR / notebook_name
            notebook_uri = f"{ROOT}/source/notebooks/{notebook_name}"
            subprocess.run(
                ["gcloud", "storage", "cp", notebook_uri, str(notebook_path)],
                check=True,
                text=True,
            )
            script_path = WORK_DIR / f"{Path(notebook_name).stem}.py"
            code = notebook_code(notebook_path)
            script_path.write_text(
                "import sys\n\n"
                "try:\n"
                "    import IPython.display\n"
                "except ImportError:\n"
                "    import types\n"
                "    _ipython = types.ModuleType('IPython')\n"
                "    _display_module = types.ModuleType('IPython.display')\n"
                "    def display(value):\n"
                "        print(value)\n"
                "    _display_module.display = display\n"
                "    _ipython.display = _display_module\n"
                "    sys.modules['IPython'] = _ipython\n"
                "    sys.modules['IPython.display'] = _display_module\n\n"
                + code,
                encoding="utf-8",
            )
            compile(script_path.read_text(encoding="utf-8"), str(script_path), "exec")
            script_hash = hashlib.sha256(script_path.read_bytes()).hexdigest()
            upload(script_path, f"{ARTIFACT_ROOT}/source/{script_path.name}")

            log_path = WORK_DIR / f"{job_name}.log"
            status["notebooks"][job_name] = {
                "state": "running",
                "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "script_sha256": script_hash,
            }
            status["current_notebook"] = job_name
            write_status(status)

            environment = os.environ.copy()
            environment["RUN_ID"] = RUN_ID
            environment["BUCKET"] = BUCKET
            # Dataproc's default Conda Python contains the scientific stack used here.
            environment["PYSPARK_PYTHON"] = "/opt/conda/default/bin/python"
            environment["PYSPARK_DRIVER_PYTHON"] = "/opt/conda/default/bin/python"
            with log_path.open("w", encoding="utf-8") as log_file:
                result = subprocess.run(
                    [
                        "spark-submit", "--master", "yarn", "--deploy-mode", "client",
                        str(script_path),
                    ],
                    cwd=WORK_DIR,
                    env=environment,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                )

            upload(log_path, f"{ARTIFACT_ROOT}/logs/{log_path.name}")
            status["notebooks"][job_name].update({
                "state": "succeeded" if result.returncode == 0 else "failed",
                "return_code": result.returncode,
                "completed_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "log_uri": f"{ARTIFACT_ROOT}/logs/{log_path.name}",
            })
            write_status(status)
            if result.returncode != 0:
                raise subprocess.CalledProcessError(result.returncode, job_name)

        status["state"] = "succeeded"
        status["current_notebook"] = None
        status["completed_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        write_status(status)
        return 0
    except Exception as error:
        status["state"] = "failed"
        status["current_notebook"] = None
        status["error"] = str(error)
        status["failed_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        try:
            write_status(status)
        except Exception as status_error:
            print(f"Could not upload failure status: {status_error}", file=sys.stderr)
        raise


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"Overnight notebook execution failed: {error}", file=sys.stderr)
        sys.exit(1)
