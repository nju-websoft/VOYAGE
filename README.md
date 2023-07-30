# VOYAGE

Source codes for paper "VOYAGE: A Large Collection of Vocabulary Usage in Open RDF Datasets".


## Data

The data used in the code is provided [here](https://zenodo.org/record/7902675).


## Source Codes and Dependencies

The codes are organized into two parts, Java and Python. The Java codes are used for dataset parsing, dataset deduplication, vocabulary extraction, and EDP extraction. The Python codes are used for crawling, analysis, fitting, and plotting.

### Java Codes

The Java codes use `JDK 1.8`, and the rest of the dependencies are shown in [porm.xml](https://github.com/nju-websoft/VOYAGE/blob/main/Java/src/pom.xml), which can be imported using Maven. In addition, the code also refers to [blabel](http://blabel.github.io/).

Before running the Java codes, a config file `parse.properties` needs to be prepared and placed in `src/main/resources/`. The contents of the config file are similar to the following:

```
tid_base=1
batch_size=3000000
....
```

For details, see the `loadProperties()` function in [ParseRDF.java](https://github.com/nju-websoft/VOYAGE/blob/main/Java/src/main/java/team/ws/rdf/app/ParseRDF.java) for the specific items to be configured.

Not required, it is recommended to edit a `log4j.properties` file and place it in `Java/src/main/resources/` to enable logging. 

To run the code, execute the `main()` function of [ParseRDF.java](https://github.com/nju-websoft/VOYAGE/blob/main/Java/src/main/java/team/ws/rdf/app/ParseRDF.java). The `parseFiles()` provides the function of parsing and EDP extraction, `handleVocabularies()` for vocabulary extraction, and `hashTriples()` for hashing triples to deduplicate the dataset.

### Python Codes

Dependencies of Python codes:

- Python 3.8
- powerlaw==1.5
- pandas==1.2.4
- numpy==1.20.1
- scipy==1.6.2
- requests==2.25.1
- urllib3==1.25.11

Before running the Python codes, download the [data](https://zenodo.org/record/7902675) and prepare a config file `config.py` which should be placed in `Python/`. The file should include the following:

```
DATA_PATH = '...'  # path of downloaded data
PLOT_SAVE_PATH = '...'
```

[crawl.py](https://github.com/nju-websoft/VOYAGE/blob/main/Python/crawl.py) provides the function of multi-threaded crawling datasets. The storage directory and log directory can be specified in the main function. 

[plot.py](https://github.com/nju-websoft/VOYAGE/blob/main/Python/plolt.py) provides functions for analyzing, fitting and plotting. In the main function, specifying a keyword allows reproducing the figures in the paper, while the console prints out the results of power-law fitting and analyzing.


## License
This project is licensed under the Apache License 2.0 - see the [LICENSE](https://github.com/nju-websoft/VOYAGE/blob/main/LICENSE) file for details.

## Contact

Qing Shi (qingshi@smail.nju.edu.cn) and Gong Cheng (gcheng@nju.edu.cn)