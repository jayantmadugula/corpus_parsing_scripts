# Text Dataset Parsing Scripts
This repository contains a collection of scripts that parse four different NLP datasets (3 aspect-based sentiment analysis datasets and 1 sentiment analysis dataset). Each script puts the parsed data into a SQLite database with minimal changes to the individual texts in the datasets.

Each of the four scripts correspond to one dataset. For citations and more information on each dataset, please look at the comments at the top of each script file. These scripts do not currently extract every part of the supported datasets since these scripts were specifically written for my Masters thesis.

**Supported Datasets**:
1. The restaurant corpus from [SemEval 2016's Aspect-Based Sentiment Analysis Task](https://alt.qcri.org/semeval2016/task5/)
2. [Restaurant Reviews dataset](https://www.cs.cmu.edu/~mehrbod/RR/)
3. [SOCC (SFU Opinion and Comments) corpus](https://github.com/sfu-discourse-lab/SOCC), which contains opinion articles from the Globe and Mail
4. [Stanford Sentiment Treebank](https://nlp.stanford.edu/sentiment/index.html)

The datasets are not included in this repository. Please look at `parameters.json` for where the scripts expect the data to placed by default.