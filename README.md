[![DOI](https://zenodo.org/badge/259206119.svg)](https://zenodo.org/badge/latestdoi/259206119) ![R-CMD-check](https://github.com/simdiversity/data-politics/workflows/R-CMD-check/badge.svg)
# simdiversity.data.politics


This package contains four datasets

* `swiss_legislator_49`

* `swiss_legislator_50`

* `italian_legislator_17`

* `italian_legislator_18`

Each dataset contains data about a legislative period split in 4 datatables. One contains the information about members of parliment, the second the information about all the votes, the third is a numeric matrix containing all the polls for each member of parliment and each vote, the final datatable contains the meaning of the numbers in the numeric matrix e.g. Yes,No,Absention...


The swiss datasets is obtained combining the xls files avilable on https://www.parlament.ch/de/ratsbetrieb/abstimmungen/abstimmung-nr-xls with some informatio from http://ws-old.parlament.ch/ the swiss parlamentary data webservice.

The Italian datasets are obtained by querying the SPARQL endpoint of the italian parliment https://dati.camera.it/ .

For more details see the scripts in the "data-raw" folder.

## Installation

```R
# Install the development version from GitHub
devtools::install_github("simdiversity/data_politics")
```

-----

