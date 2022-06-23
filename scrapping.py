import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import os
from joblib import Parallel, delayed

def collecting_contratacion_data_from_aena(output:str,n_jobs=1):
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
            if children_column_name:
                column_name=children_column_name.text.strip()
            else:
                continue
            if column_name=='Fechas contrato:':
                values['Fecha de inicio']=children_value.contents[2].text.strip() 
                values['Fecha fin actual']=children_value.contents[4].text.strip()
                continue
            elif column_name=='Identidad del adjudicatario:':
                values['NIF']=children_value.contents[2].text.strip()
                values['Razón social:']=children_value.contents[4].text.strip()
                continue
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
    def collect_info_per_page(page):
        url_update=url.replace('{replace_me}',str(page))
        payload={
            'portal': 'contratos',
                'pagina': str(page)}
        page = r.get(url_update,
        params=payload,
        )
        soup = BeautifulSoup(page.text,'html.parser')
        main_table= soup.find('table', id='Taperturas')
        rows = main_table.findAll('tr')
        values=[]
        for row in rows[1:]:
            
            cells_raw = row.findAll('td')
            assert len(cells_raw)==len(headers),\
                'the number of columns in the row are different that in the header'

            cells={header:cell.text.strip() for header,cell in zip(headers,cells_raw)}
            cells['Fecha de Contrato']=cells['Fecha de Contrato'][:10] #cleaning

            cells['url_with_details']=cells_raw[2].find('a', href=True).get('href')

            details=(get_details_from_extra_url(cells['url_with_details']))
            information={**cells, **details}
            values.append(information)
        return values

    if n_jobs==1:
        values=[collect_info_per_page(page) for page in tqdm(range(1,120),desc=' doing scrapping per page') ]
    else:   
        values=Parallel(n_jobs=n_jobs)(delayed(collect_info_per_page)(
            page) for page in tqdm(range(1,120),desc=' doing scrapping per page')   
        )
        
    values = [x for xs in values for x in xs]
    df=pd.DataFrame(values)
    df.to_csv(output,index=False)

folder_with_data='data'
name_file='raw_data.csv'
output=os.path.join(folder_with_data,name_file)
n_jobs=10
collecting_contratacion_data_from_aena(output,n_jobs=10)