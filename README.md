# panstarrs-catalogue-tools

## Description

Python tools to download [Pan-STARRS1](https://panstarrs.stsci.edu/) catalogues from 
their [CasJobs](http://mastweb.stsci.edu/ps1casjobs/default.aspx) system. Splits the catalogue into ```nchunks```
(i.e. subregions) and deletes each chunk from your MyDB after it's downloaded. The code will also output a text
file listing the names of the catalogues and the RA/DEC ranges they cover. The name of this text file is
configurable via the ```list-file``` variable.

## Setup
As well as the required Python modules, you'll also need to follow the instructions on how to
download and configure the CasJobs Java script [here](http://mastweb.stsci.edu/mcasjobs/casjobscl.aspx). The 
Python code is a wrapper for that script. You'll also need an account with the CasJobs system at STSci. 

## Usage 

``` 
usage: download_ps1_cat.py [-h] [--verbose] [--nchunks NCHUNKS]
                           [--list-file LIST_FILE] [--test]
                           [--psps-auth PSPS_AUTH] [--nskip NSKIP]
                           username ra_range ra_range dec_range dec_range
                           output_template
```                           
### Positional arguments:

  1. username: Your username on CasJobs/PSPS
  2. ra_range: Lower and upper RA limits
  3. dec_range: Lower and upper dec limits
  4. output_template: Template for output catalogues, must have integer format statement in it, e.g. cat_{:d}, 
     which will be filled by the number of the current chunk

### Example:

``` python ~/ps1/panstarrs-catalogue-tools-public/download_ps1_cat.py farrow 100 101 -1.0 1.0 test_{:d} ```
