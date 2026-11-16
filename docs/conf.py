"""Sphinx configuration for the CIDP provider docs: ``uv run --group docs sphinx-build -W docs docs/_build``."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

project = "airflow-provider-cidp"
author = "Schuberg Philis"
copyright = "2026, Schuberg Philis"
release = "0.1.0"

extensions: list[str] = []
exclude_patterns = ["_build"]
html_theme = "alabaster"
html_title = "CIDP provider for Apache Airflow"
html_theme_options = {"description": "Cloud Independent Data Platform", "page_width": "1100px"}
