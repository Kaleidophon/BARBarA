# BARBarA
## A **B**ad **A**cronym **R**egarding a **Ba**chelo**r** **A**ssignment

### DESCRIPTION

This is the source code of a senior thesis done at the University of Heidelberg at the
department for Computational Linguistics until August 2016.

The goal of this project was to analyze the use of word vector representations for the task
of semantic relation prediction. A paper from [(Bordes et al., 2013)](http://papers.nips.cc/paper/5071-translating-embeddings-for-modeling-multi-relational-data.pdf)
was used as an inspiration.
Hereafter, three experiments were conducted:
    * Replicating the results of (Bordes et al., 2013) with a smaller dataset based on German relation triples from *Freebase*
    * Clustering word embedding offsets to find new word pairs belonging to a semantic relation
    * Training relation vectors with the training procedure from (Bordes et al. 2013), but using word embeddings for the
    entity representations.

### REQUIREMENTS

The following external python modules need to be installed to use this code:
    * `gensim`
    * `gzip`
    * `matplotlib`
    * `mpl_toolkits`
    * `numpy`
    * `scipy`
    * `sklearn`
    * `yaml`

Not all of those dependencies are obligatory depending on the part of the project you want to
execute.
[`Word2Vec`](https://code.google.com/archive/p/word2vec/) was used in this project to train word embeddings.

### USAGE

The project is divided into multiple packages and subpackages:
  * `src.clustering`: Clustering of mapping vectors.
  * `src.eval`: Evaluation of word embeddings.
  * `src.mapping`: Creation of mapping vectors from word embedding pairs.
  * `src.misc`: Helper function and decorators.
  * `src.prep`: Scripts used for different preparations steps for fundamental resources.
  * `src.trans_e`: *TransE* related preparation scripts as well as *TransE* inspired Training with word embeddings.

Describing the usage of all of the scripts within the packages would go beyond the capabilities
of a README. Therefore, it is recommended to consult the extensive documentation found in
the `docs/_build/` folder in `html` and `latex` format.
Also, most of the scripts are equipped with Python's `argparse` or `optparse` module.
As so, executing the script via `python <script_name> -h` will display possible command line
arguments and short descriptions thereof.

### LICENSE & COPYRIGHT

This is free and unencumbered software released into the public domain.
Anyone is free to copy, modify, publish, use, compile, sell, or distribute this software, either in source code form or as a compiled binary, for any purpose, commercial or non-commercial, and by any means.
In jurisdictions that recognize copyright laws, the author or authors of this software dedicate any and all copyright interest in the software to the public domain. We make this dedication for the benefit of the public at large and to the detriment of our heirs and successors. We intend this dedication to be an overt act of relinquishment in perpetuity of all present and future rights to this software under copyright law.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
For more information, please refer to http://unlicense.org

