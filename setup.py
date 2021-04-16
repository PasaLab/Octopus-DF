#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages 

packages = ['Octopus', 'Octopus.dataframe', 'Octopus.dataframe.core', 'Octopus.dataframe.io',
            'Octopus.dataframe.predict', 'tests', 'tests.dataframe', 'tests.dataframe.core',
            'tests.dataframe.test_data', 'Octopus.dataframe.predict']


setup(name='Octopus-DataFrame',    #项目名称
      version='3.0',    #版本
      author='Jun Shi',
      author_email='junshinju@gmail.com',
      license='',
      url='github.com/shizihao123/Octopus-DataFrame',
      packages=packages,#项目根目录下需要打包的目录
      include_package_data=True,
      py_modules=[],    #需要打包的模块
      install_requires=[    #依赖的包
        'pyspark >= 2.3.0',
        'pandas >= 0.24.0',
        'numpy >= 1.16.0',
        'py4j >= 0.10.7',
        'configparser >= 3.5.0',
        'pytest >= 3.3.0',
        'pyarrow >= 0.11.1',
        'redis >= 2.10.6',
        'distributed >= 1.25.2',
        'dask >= 1.1.0',
    ],
    scripts=[],
)


