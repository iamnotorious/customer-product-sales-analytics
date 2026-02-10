from setuptools import setup, find_packages

setup(
    name="sales_ecommerce_analytics_ingestion_utils",
    version="0.1.0",
    description="Databricks Data Engineering Project",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        # "pyspark" is usually available in Databricks runtime
        # Add other dependencies here if needed, e.g. "pandas"
    ],
)
