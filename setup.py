from setuptools import setup # type: ignore

# Initialize version variable for the linter, it will be assigned when version.py is evaluated
__version__ : str
exec(open('pgraphdb/version.py', "r").read())

# Read the requirements from the requirements.txt file
with open("requirements.txt", "r") as fh:
    requirements = [r.strip() for r in fh.readlines()]

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
    package_data={"pgraphdb": ["py.typed"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=requirements,
    entry_points={"console_scripts": ["pgraphdb=pgraphdb.ui:main"]},
    py_modules=["pgraphdb"],
    zip_safe=False,
)
