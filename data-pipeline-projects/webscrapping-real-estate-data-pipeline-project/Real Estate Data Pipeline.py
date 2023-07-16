#Importing modules
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

#Price
#Creating a loop that gets price information from all 159 pages
page = 1
price_column = []
while page != 3:
      url = f"https://domoplius.lt/skelbimai/butai?action_type=1&page_nr={page}&slist=141298144"
      response = requests.get(url)
      response_content = response.content
      soup = bs(response_content, 'html.parser')
    
      for element in soup.select('p strong'):
            price_column.append(element.get_text(separator=" ", strip=True))
      page = page + 1
df_prices = pd.DataFrame(price_column, columns = ['Price'])
print(f'Loaded {len(df_prices)} price rows')

