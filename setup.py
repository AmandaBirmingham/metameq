# ----------------------------------------------------------------------------
# Copyright (c) 2024, Amanda Birmingham.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import versioneer
from setuptools import setup, find_packages

setup(name='metameq',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      long_description=(
          "METAMEQ: "
          "Metadata Extension Tool to Annotate Microbiome Experiments for Qiita, "
          "a tool for generating and validating Qiita-compliant metadata files."),
      license='BSD-3-Clause',
      description='Qiita-compliant metadata generation and validation tool',
      author="Amanda Birmingham",
      author_email="abirmingham@ucsd.edu",
      url='https://github.com/AmandaBirmingham/metameq',
      packages=find_packages(),
      include_package_data=True,
      # NB: if changing here, also change the environment.yml
      install_requires=[
          'click>=8.0.0',
          'pandas>=1.3.0',
          'PyYAML>=5.4.0',
          'Cerberus>=1.3.4',
      ],
      package_data={
          'metameq': ['config/*.yml']
      },
      entry_points={
          'console_scripts': ['metameq=metameq.src.__main__:root']}
      )
