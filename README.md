
<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]



<!-- PROJECT LOGO -->
<br />

<p align="center">
  <!-- <a href="https://github.com/PasaLab/Octopus-DF">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a> -->

  <h3 align="center">Octopus-DataFrame</h3>

  <p align="center">
    A cross-platform pandas-like DataFrame base on pandas, spark and dask.
    <!-- <br />
    <a href="https://github.com/PasaLab/Octopus-DF"><strong>Explore the docs »</strong></a>
    <br /> -->
    <br />
    <a href="https://github.com/PasaLab/Octopus-DF">View Demo</a>
    ·
    <a href="https://github.com/PasaLab/Octopus-DF/issues">Report Bug</a>
    ·
    <a href="https://github.com/PasaLab/Octopus-DF/issues">Request Feature</a>
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <!-- <li><a href="#roadmap">Roadmap</a></li> -->
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

<!-- [![Product Name Screen Shot][product-screenshot]](https://example.com) -->

Octopus-DF, which integrates Pandas, Dask, and Spark as the backend computing platforms and exposes the most widely used Pandas-style APIs to users.

Why Octopus-DF?
* Efficient for diffrent data scales. A DataFrame-based algorithm has quite different performances over different platforms with various data scales, it is not efficient to process DataFrame of different scales
* Ease to use. Octopus-DF provide Pandas-style API for data analysts to model and develop their problems and programs.


### Built With

* [pyspark](https://spark.apache.org)
* [pandas](https://pandas.pydata.org)
* [dask](https://dask.org)
* [numpy](https://numpy.org)
* [py4j](https://www.py4j.org)
* [pytest](https://pytest.org)
* [pyarrow](https://arrow.apache.org)
* [redis](https://redis.io)

<!-- GETTING STARTED -->
## Getting Started
Octopus-DF 

### Prerequisites
You should install **python3.6+** environment first.

* Clone the repo
  ``` sh
  git clone https://github.com/PasaLab/Octopus-DF.git
  ```
* Install all dependencies.
  ```sh
  cd Octopus-DF
  pip install –r requirements.txt
  ```

### Installation

1. Generate the target package.
    ```sh
    python setup.py sdist
    ```
2. Install the package
    ```sh
    pip install Octopus-DataFrame
    ```

<!-- USAGE EXAMPLES -->
## Usage

Octopus-DF is built on pandas, spark, and dask, you need to deploy them on your distributed environment first. For spark, we use [Spark on Yarn](https://spark.apache.org/docs/2.3.2/running-on-yarn.html) mode. For dask, we use [dask distributed](https://distributed.dask.org/en/latest/). Please first check the official documentation to complete the installation and deployment.

To optimize the secondary index, you should install redis first. The suggested way of installing Redis is compiling it from sources as Redis has no dependencies other than a working GCC compiler and libc.  Please check the [redis official documentation](https://redis.io/topics/quickstart) to complete the installation and deployment. 

In order to optimize the local index, you should install plasma store first. In stand-alone mode, install `pyarrow0.11.1` on the local machine (`pip install pyarrow==0.11.1`), and use  `plasma_store –m 1000000000 –s /tmp/plasma &` to open up memory space to store memory objects. The above command opens up 1g of memory space For details, please refer to [plasma official documentation](http://arrow.apache.org/docs/python/plasma.html#starting-the-plasma-store). In cluster mode, install on each machine and start the plasma store process.

When you install and deploy the above dependencies, you need to config Octopus-DF by edit `$HOME/.config/octopus/config.ini` file.

The configuration example is as follows:
```
[installation]
          
HADOOP_CONF_DIR = $HADOOP_HOME/etc/hadoop  
SPARK_HOME = $SPARK_HOME
PYSPARK_PYTHON = ./ANACONDA/pythonenv/bin/python         # python environment
SPARK_YARN_DIST_ARCHIVES = hdfs://host:port/path/to/pythonenv.zip#ANACONDA
SPARK_YARN_QUEUE = root.users.xquant
SPARK_EXECUTOR_CORES = 4
SPARK_EXECUTOR_MEMORY =10g
SPARK_EXECUTOR_INSTANCES = 6
SPARK_MEMORY_STORAGE_FRACTION = 0.2
SPARK_YARN_JARS = hdfs://host:port/path/to/spark/jars    # jars for spark runtime, which is equivalent to spark.yarn.jars in spark configuration
SPARK_DEBUG_MAX_TO_STRING_FIELDS = 1000
SPARK_YARN_EXECUTOR_MEMORYOVERHEAD = 10g
```
For imperative interface such as SparkDataFrame and OctDataFrame, using Octopus-DF is like using pandas.

### SparkDataFrame
```python
from Octopus.dataframe.core.sparkDataFrame import SparkDataFrame
odf = SparkDataFrame.from_csv("file_path")
print(odf.head(100))
print(odf.loc[0:10:2,:])
print(odf.filter(like='1'))
```
### OctDataFrame

```python
from Octopus.dataframe.core.octDataFrame import OctDataFrame
# engine_type can be dask, pandas.
odf = OctDataFrame.from_csv("file_path",engine_type="spark")
print(odf.head(100))
print(odf.loc[0:10:2,:])
print(odf.filter(like='1'))
```
For declarative interface such as SymbolDataframe, using Octopus-DF is like using spark, due to its lazy computation, you should call `compute()` to do the calculation.
### SymbolDataFrame

```python
from Octopus.dataframe.core.symbolDataFrame import SymbolDataFrame
# engine_type can be dask, pandas. 
# If not declared, Octopus-DF will automatically select the best platform.
# Note that this function is experimental, need to be improved.
odf = SymbolDataFrame.from_csv(filie_path,engine_type='Spark')
odf1 = odf.iloc[0:int(0.8*M):2,:]
odf2 = odf1.iloc[0:int(0.2*M):1,:]
odf2.compute()
```

ROADMAP
## Roadmap

See the [open issues](https://github.com/PasaLab/Octopus-DF/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing
Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact Us

Gu Rong - gurong@nju.edu.cn






<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/PasaLab/Octopus-DF.svg?style=for-the-badge
[contributors-url]: https://github.com/PasaLab/Octopus-DF/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/PasaLab/Octopus-DF.svg?style=for-the-badge
[forks-url]: https://github.com/PasaLab/Octopus-DF/network/members
[stars-shield]: https://img.shields.io/github/stars/PasaLab/Octopus-DF.svg?style=for-the-badge
[stars-url]: https://github.com/PasaLab/Octopus-DF/stargazers
[issues-shield]: https://img.shields.io/github/issues/PasaLab/Octopus-DF.svg?style=for-the-badge
[issues-url]: https://github.com/PasaLab/Octopus-DF/issues
[license-shield]: https://img.shields.io/github/license/PasaLab/Octopus-DF.svg?style=for-the-badge
[license-url]: https://github.com/PasaLab/Octopus-DF/blob/master/LICENSE.txt
[product-screenshot]: images/screenshot.png