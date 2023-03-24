from setuptools import find_packages, setup


setup(
        name="dqlib",
        version="1.0.6",
        author="Rabindra Neupane",
        description="DQ Engine",
        long_description="Library to check data quality on spark",
        packages=["dqlib"],
        python_requires='>=3.7', 
)
