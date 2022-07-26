# aena_scrapping_licitaciones

Aena scrapping is a repository where we do the next steps do web scrapping to Aena. Once this is done we have all the data that we need to differents purpose which are:

* Apply web scrapping to [Aena](https://contratacion.aena.es/contratacion/) scrapping.py
* Clean the data and create new features
* Discover new Insights analyzing the data
* Create relations between airports/companies with graphs
* Apply NLP to create differents topics in relation to the title of contracts

## scrapping.py:

In this file we do the web scrapping and to do it we need to create a request session because we need to save the cookies. If we do not do this, the page will give an error or it will return us to the first page. Once this is done, we take all the data found in the table and also enter the existing link in each table to scrape this page as well. This was a very slow process and that is why it was decided to parallelize the process. With this reduce at least a 90% the time of compute (in my computer)

## clean_data.ipynb:

dd