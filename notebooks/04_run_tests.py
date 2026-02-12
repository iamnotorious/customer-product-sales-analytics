# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Run Test Suite
# MAGIC 
# MAGIC This notebook executes the project's test suite using `pytest`.
# MAGIC It ensures code quality and correctness by verifying all components (Ingestion, Silver Logic, Gold Metrics, Data Quality).

# COMMAND ----------

# Install pytest if not already available
# MAGIC %pip install pytest

# COMMAND ----------

import pytest
import os
import sys

# Dynamically find and append the 'src' and 'tests' directories
current_dir = os.getcwd()
project_root = None

# Navigate up until we find 'src'
search_dir = current_dir
while True:
    if os.path.exists(os.path.join(search_dir, "src")):
        project_root = search_dir
        break
    
    parent_dir = os.path.dirname(search_dir)
    if parent_dir == search_dir:  # Root reached
        break
    search_dir = parent_dir

if project_root:
    sys.path.append(os.path.join(project_root, "src"))
    sys.path.append(os.path.join(project_root, "tests"))
    print(f"Project root found at: {project_root}")
else:
    print("Warning: Project root not found. Tests may fail due to import errors.")

# COMMAND ----------

# Run pytest on the 'tests' directory
# -v: verbose
# -p no:cacheprovider: disable caching to avoid permission issues
# -x: stop on first failure (optional, removed for full report)
exit_code = pytest.main(["-v", "-p", "no:cacheprovider", os.path.join(project_root, "tests")])

if exit_code == 0:
    print("\nAll tests passed!")
else:
    print("\nSome tests failed. Check output above.")
    # Raise error to fail the notebook execution if embedded in a workflow
    raise Exception("Test suite failed")
