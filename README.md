# DB automation

This module is designed to automatically update the CBE and NBB databases. It exists out of several sub-modules.

In order to understand what all modules do, it is necessary to understand how data is provided from the sources, to understand the difficulties of handling larger amounts of data, and database limitations.

## NBB database
The National Bank of Belgium provides annual reports via it's API. Generally speaking, we differentiate between "references" and "filings".

#### References
References refers to the summarisation of all known filings of an enterprise. When querying references from the API, a list of dictionaries will be returned, with each dictionary representing one distinct filing and it's basic information (mostly qualitative).

Due to the nature of juridical forms (e.g. BV, VZW, ...) that exist, not all companies are obliged to submit annual reports.

#### Filings
Filings refers to a distinct filing that contains financial information (quantitative data) of said filing. Usually this is returned as a dictionary.

Not all filings in the references are available for download, only the standard ones (excl. Consolidated filings, etc.).

## Methodology
Due to the current architecture, copying the NBB database was done running a script outside this module. The script fetched all CBE numbers of enterprises and, in turn, fetched the corresponding references + filings. These were stored in the `original` tables of the database `cd_nbb_archive`.

In a later stage, using the module `updater.nbb`, current data is updated. The script fetches the daily update files (extracts) from the NBB. References get updated via means of appending the latest reference to the existing references. `Filings` get stored as a new row.

In some cases, `references` are present but no filing. In that case, only the `references` get stored.

### Difference in keys
Due to the difference between download from the NBB `authentic` database and the `extract`s database, the dictionary keys of both `authentic` and `extract`s need to be updated to the standard of **UpperCamelCase**. The latter is done via `update.py` in the module `updater.nbb_archive`.


## config.py
This file holds all external variables and loads them from an environmental file.

## database
This module holds database connectors. These are somewhat generalised ways to connect with the database and need minimal parameter input to work. 

They are a way to quickly make connections to the database by creating an instance of said connectors.

The module also holds the models of database tables that can be referenced.

## loader
This module holds the means to copy the NBB database in bulk. It does not regard any semantic meaning of the data but rather copy/paste the response of the API in the database.

## logger
This module can be used to initiate logger or update existing loggers.

## mailer
This module can be used to set or create preferences regarding automatic mail updates after updating.

## updater