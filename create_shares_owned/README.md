The purpose of this group of .py files is to create a dataset named 'shares_owned'. 
This dataset will contain the number of shares owned by a publicly traded parent company in a 
publicly traded subsidiary as informed in the 13D/G filings. 
Moreover, a cusip/cik mapping is also produced.

The modules are presented in the order in which they should be executed:

**create_index_file.py** will download the indexes of all filings of the SEC. 

**download_files.py** will download all 13D and 13G SEC schedules and store them 
in two folders named respectively 13D and 13G.

**cusip_parser** this will parse all the 13D and 13G schedules to obtain the cusip
of the corresponding asset. The output are two csv files named **13D.csv** and **13G.csv**.
They contain the cik of the issued asset, the cik of the owner and a cusip number
of the corresponding asset. 

**create_cusip_mapping.py** this creates a mapping between cusip numbers and cik numbers. The 
output file is named **cik-cusip-maps.csv**.

**find_public_traded_owners.py** will create a file named **public_file_data.csv** that will
store 13D/G filings where the owner's cik number is also in the cik column. This implies 
that the cik of the owner also correspond to a publicly traded assets. This allows to select the
subset of schedule that correspond to situation where a publicly traded parent company owns shares
in a publicly traded subsidiary. Could miss parent/subsidiary pairs in the case where no 13D/G
filing have been issued for a given parent. Rather unlikely. Maybe this could be improved?

**filing_parser.py** this filing contain a class named `FileSec`. This object is instantiate by
providing the file path of 13D or G schedule. Then, the different numbers of shares owned, the
percentage of shares owned and the cusip number are provided by using the adequate methods. 

**scrap.py** this will use the **filing_parser.py** to extract information on each schedule. 
The output file is named **shares_owned.csv**. 

**launch.py** this allows to launch multiple of the above filings by launching only this module.

