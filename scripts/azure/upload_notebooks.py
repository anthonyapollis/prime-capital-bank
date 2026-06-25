"""
Upload all Prime Capital Bank Databricks notebooks to workspace.
Uses Databricks REST API 2.0 — no Databricks SDK required.
"""

import os
import base64
import json
import requests
from pathlib import Path

DATABRICKS_HOST  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
WORKSPACE_FOLDER = "/Prime Capital Bank"

NOTEBOOKS_DIR = Path(__file__).parent.parent.parent / "notebooks"

def api(method: str, path: str, payload: dict = None) -> dict:
    url = f"{DATABRICKS_HOST}/api/2.0{path}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    resp = requests.request(method, url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json() if resp.text else {}

def ensure_folder(path: str):
    try:
        api("POST", "/workspace/mkdirs", {"path": path})
        print(f"  Folder ready: {path}")
    except requests.HTTPError as e:
        if "RESOURCE_ALREADY_EXISTS" not in str(e.response.text):
            raise

def upload_notebook(local_path: Path, workspace_path: str):
    content = local_path.read_bytes()
    encoded = base64.b64encode(content).decode("utf-8")
    api("POST", "/workspace/import", {
        "path": workspace_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded,
        "overwrite": True
    })
    print(f"  ✓ Uploaded: {local_path.name} → {workspace_path}")

def main():
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        raise ValueError("Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")

    print(f"\nUploading notebooks to {DATABRICKS_HOST}{WORKSPACE_FOLDER}")
    print("=" * 60)

    ensure_folder(WORKSPACE_FOLDER)

    notebooks = sorted(NOTEBOOKS_DIR.glob("*.py"))
    if not notebooks:
        print(f"No notebooks found in {NOTEBOOKS_DIR}")
        return

    for nb in notebooks:
        workspace_path = f"{WORKSPACE_FOLDER}/{nb.stem}"
        upload_notebook(nb, workspace_path)

    print(f"\n✓ {len(notebooks)} notebooks uploaded successfully.")
    print(f"  View at: {DATABRICKS_HOST}/#workspace{WORKSPACE_FOLDER}")

if __name__ == "__main__":
    main()
