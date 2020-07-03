from setuptools import setup

from pgraphdb.version import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="pgraphdb",
    version=__version__,
    description="A wrapper around the GraphDB REST interface",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/arendsee/pgraphdb",
    author="Zebulun Arendsee",
    author_email="zbwrnz@gmail.com",
    packages=["pgraphdb"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[ "SPARQLWrapper" ],
    entry_points={"console_scripts": ["pgraphdb=pgraphdb.ui:main"]},
    py_modules=["pgraphdb"],
    zip_safe=False,
)
