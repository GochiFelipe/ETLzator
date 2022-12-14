# Always prefer setuptools over distutils
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# This call to setup() does all the work
setup(
    name="etlzator",
    version="0.1.42",
    description="This library, implements, and abstract a way to construct ETL/ELT Pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://etlzator.readthedocs.io/",
    author="Felipe Gochi",
    author_email="felipeud@gmail.com",
    license="Apache-2.0 license",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent"
    ],
    packages=["etlzator", "etlzator.etl", "etlzator.extract", "etlzator.load", "etlzator.transform"],
    include_package_data=True,
    install_requires=["pytterns"]
)
