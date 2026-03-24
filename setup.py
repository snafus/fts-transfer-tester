from setuptools import setup, find_packages

setup(
    name="fts-testframework",
    version="0.1.0",
    description="Production-grade FTS3 REST transfer test framework",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.6",
    install_requires=[
        "requests>=2.27.1,<2.28",   # 2.28+ dropped Python 3.6 support
        "urllib3>=1.26.0,<2",       # urllib3 2.x dropped Python 3.6 support
        "PyYAML>=5.4.1,<6",
        "certifi>=2021.10.8",
    ],
    entry_points={
        "console_scripts": [
            "fts-run=fts_framework.runner:main",
            "fts-sequence=fts_framework.sequence.__main__:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],
)
