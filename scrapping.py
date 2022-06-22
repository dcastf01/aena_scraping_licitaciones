import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
# from selenium import webdriver

# driver = webdriver.Chrome('chromedriver\chromedriver.exe')
main_url='https://contratacion.aena.es/contratacion/'

def get_details_from_extra_url(url_with_details:str)->dict:
    url=main_url+url_with_details
    page = requests.get(url)
    soup = BeautifulSoup(page.text,'html.parser')
    args={ 'class':'detalles'}
    main_table= soup.find('table',**args )
    rows = main_table.findChildren(['tr'])
    values={}
    for row in rows:
        children_column_name=row.find('th')
        children_value=row.find('td')
        
        #check fecha contrato  suele ser la tercera iteración
        if children_column_name:
            column_name=children_column_name.text.strip()
        else:
            continue
        if column_name=='Fechas contrato:':
            values['Fecha de inicio']=children_value.contents[2].text.strip()
            values['Fecha fin actual']=children_value.contents[4].text.strip()
            #arreglar también si tiene adjudicatario y otra cosa más
        if column_name=='Identidad del adjudicatario:':
            print('hi')
        if children_value:
            value=children_value.text.strip()
        else:
            value=0
        values[column_name]=value
    return values

# Create an URL object
url = 'https://contratacion.aena.es/contratacion/principal?portal=contratos&pagina={replace_me}'

# We need create the session to keep the cookies, if not don't return the page correctly
r=requests.Session() 
# Create object page

page = r.get(url)
soup = BeautifulSoup(page.text,'html.parser')

# Obtain information from tag <table>
main_table= soup.find('table', id='Taperturas')

headers=[ column.text.strip() for column in main_table.find_all('th')]
# headers.append('url_with_details')
url = 'https://contratacion.aena.es/contratacion/principal'
values=[]
for page in tqdm(range(1,20),desc=' doing scrapping per page'): #until 120 take this value from the website
    url_update=url.replace('{replace_me}',str(page))
    # driver.get(url_update)
    payload={
        'portal': 'contratos',
            'pagina': str(page)}
    page = r.get(url_update,
    params=payload,
    # headers={'User-Agent': 'Custom user agent'}
    ) #using url funciona
    # soup = BeautifulSoup(driver.page_source,'html.parser')
    soup = BeautifulSoup(page.text,'html.parser')
    main_table= soup.find('table', id='Taperturas')
    rows = main_table.findAll('tr')

    for row in tqdm(rows[1:],desc='doing scrapping per row'):
        
        cells_raw = row.findAll('td')
        assert len(cells_raw)==len(headers),\
            'the number of columns in the row are different that in the header'

        cells={header:cell.text.strip() for header,cell in zip(headers,cells_raw)}
        cells['Fecha de Contrato']=cells['Fecha de Contrato'][:10] #cleaning

        cells['url_with_details']=cells_raw[2].find('a', href=True).get('href')

        details=(get_details_from_extra_url(cells['url_with_details']))
        information={**cells, **details}
        values.append(information)

# headers_from_detail=list(details[0].keys())
# headers=headers+headers_from_detail
df=pd.DataFrame(values)
print(df.head())
print(df.tail())
print(df.shape)
df.to_csv('results.csv')