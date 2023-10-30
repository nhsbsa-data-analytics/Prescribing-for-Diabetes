# Prescribing-for-Diabetes

This code is published as part of the NHSBSA Official Statistics team's commitment to open code and transparency in how we produce our publications. The Prescribing for Diabetes (PfD) reproducible analytical pipeline (RAP) is owned and maintained by the Official Statistics team.

# Introduction

This RAP aims to bring together all code needed to run a pipeline in R to produce the PfD publication. It includes accompanying documentation in line with RAP best practice. The RAP includes a `functions` folder containing several files with functions specific to this publication, as well as a `sql` folder containing SQL code for extracting the raw data used by the pipeline. The RAP will produce an HTML report and accompanying HTML background and methodology document. This RAP makes use of many R packages, including several produced internally at the NHSBSA. Therefore, some of these packages cannot be accessed by external users. 

This RAP cannot be run in it's entirety by external users. However it should provide information on how the Official Statistics team extract the data from the NHSBSA data warehouse, analyse the data, and produce the outputs released on the NHSBSA website as part of this publication.

This RAP is a work in progress and may be replaced as part of updates and improvements for each new release of the PfD publication. The functions in the `functions` folder do not contain unit testing, although we will investigate adding this in future.

## Installation

You can clone the repository containing the RAP through [GitHub](https://github.com/) by:

``` r
# install.packages("devtools")
devtools::install_github("nhsbsa-data-analytics/mumhquarterly")
```
You can view the [source code for the PfD RAP](https://github.com/nhsbsa-data-analytics/Prescribing-for-Diabetes) on GitHub.

## Running this RAP

Users outside of the Official Statistics team may not have the required access permissions to run all parts of this RAP. 



## Functions guide

Functions used specificially for this RAP can be found in the [`functions` folder](https://github.com/nhsbsa-data-analytics/Prescribing-for-Diabetes/tree/main/functions). The RAP also makes use of functions from a range of packages. A list of packages used is included at the beginning of the `pipeline.R` file, and these packages are installed and loaded as part of running the pipeline code.

Functions written directly for PfD have been split into several R script files.


# Contributing

Contributions are not currently being accepted for this RAP. If this changes, a contributing guide will be made available.

# License

The `Prescribing-for-Diabetes` RAP, including associated documentation, is released under the MIT license. Details can be found in the `LICENSE` file.