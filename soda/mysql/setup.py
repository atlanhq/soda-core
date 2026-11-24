#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-mysql"
package_version = "3.0.48"
description = "Soda Core MySQL Package"

requires = [
    f"soda-core=={package_version}",
    "mysql-connector-python>=9.1.0,<10",
]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    # mysql-connector-python 9.1 dropped Python 3.8.
    python_requires=">=3.9",
    packages=find_namespace_packages(include=["soda*"]),
)
