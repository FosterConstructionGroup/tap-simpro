#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-simpro",
    version="1.0.0",
    description="Singer.io tap for extracting data from the Simpro API",
    author="Sam Woolerton",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_simpro"],
    install_requires=["pipelinewise-singer-python==1.*", "aiohttp==3.13.3"],
    extras_require={
        "dev": [
            "pylint",
            "ipdb",
            "nose",
        ]
    },
    entry_points="""
          [console_scripts]
          tap-simpro=tap_simpro:main
      """,
    packages=["tap_simpro"],
    package_data={"tap_simpro": ["tap_simpro/schemas/*.json"]},
    include_package_data=True,
)
