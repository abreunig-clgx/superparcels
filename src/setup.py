from setuptools import setup, find_packages

setup(
    name="sp",
    version="0.1",
    packages=find_packages(include=['phase2'], exclude=['main_sp_fixed']),
    install_requires=[
        "tqdm",
        "click",
    ],
    entry_points={
        "console_scripts": [
            "sp=cli:cli",
        ],
    },
)
