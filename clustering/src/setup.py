from setuptools import setup, find_packages

setup(
    name="sp",
    version="0.1",
    #packages=find_packages(include=['', '']),
    install_requires=[
        "tqdm",
    ],
    entry_points={
        "console_scripts": [
            "sp=cli:cli",
        ],
    },
)
