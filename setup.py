from setuptools import setup, find_packages

with open("README.rst", "r") as f:
    long_description = f.read()

NAME = "vinum"
VERSION = "0.0.1"
AUTHOR = "Dmitry Koval"
AUTHOR_EMAIL = "dima@koval.space"
DESCRIPTION = (
    "Vinum is a SQL processor written in pure Python, "
    "designed for data analysis workflows and in-memory analytics. "
)
URL = "https://github.com/dmitrykoval/vinum"
PROJECT_URLS = {
    "Bug Tracker": "https://github.com/dmitrykoval/vinum/issues",
    "Documentation": "https://vinum.readthedocs.io/en/latest/index.html",
    "Source Code": "https://github.com/dmitrykoval/vinum",
}
INSTALL_REQUIRES = [
    "pyarrow >= 2.0.0",
    "numpy >= 1.19.0",
    "moz_sql_parser == 3.32.20026"
]


setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url=URL,
    project_urls=PROJECT_URLS,
    keywords=["sql", "python", "numpy", "pandas", "pyarrow", "data analysis"],
    packages=find_packages(),
    python_requires=">=3.7.0",
    install_requires=INSTALL_REQUIRES,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: SQL",
        "Topic :: Scientific/Engineering",
    ]
)
