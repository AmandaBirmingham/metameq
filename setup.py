# ----------------------------------------------------------------------------
# Copyright (c) 2024, Amanda Birmingham.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import versioneer
from setuptools import setup, find_packages

setup(name='qiimp',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      long_description="QIIMP: Quick and Intuitive Interactive Metadata "
                       "Preparation, a tool for generating and validating "
                       "Qiita-compliant metadata files.",
      license='BSD-3-Clause',
      description='Qiita-compliant metadata generation and validation tool',
      author="Amanda Birmingham",
      author_email="abirmingham@ucsd.edu",
      url='https://github.com/AmandaBirmingham/qiimp2',
      packages=find_packages(),
      include_package_data=True,
      package_data={
          'qiimp': [
              '*.*',
              'data/*.*']
      },
      install_requires=['pandas', 'pyyaml', 'nose', 'pep8', 'flake8'],
      )
