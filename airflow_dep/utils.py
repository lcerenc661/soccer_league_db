import pandas as pd
from datetime import datetime
import random
import time

def get_data(url, liga):
    
    col_names = ['Team','G','W','D','L','GS','GC','DIF','PTS']
    tiempo = [1,2,3]
    time.sleep(random.choice(tiempo))
    df = pd.read_html(url)
    df = pd.concat([df[0],df[1]], axis=1, ignore_index=True)
    df[0] = df[0].apply(lambda x: x[5:] if x[:2].isnumeric() else x[4:])
    df.columns = col_names
    df['League'] = liga
    
    loading_date = datetime.now().strftime("%Y-%m-%d")
    df['Loading_date'] = loading_date
    
    return df

def data_processing(df):
    
    df_final = pd.DataFrame(columns=['Team','G','W','D','L','GS','GC','DIF','PTS'])
    
    for i in range(len(df['Liga'])):
        df_temp = get_data(df['url'][i],df['Liga'][i])
        df_final = pd.concat([df_final,df_temp], axis=0, ignore_index=False)
    
    return df_final



if __name__ == "__main__":
    df = get_data("https://www.espn.com.co/futbol/posiciones/_/liga/esp.1", "España")
    
    urls = ['https://www.espn.com.co/futbol/posiciones/_/liga/esp.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/ita.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/eng.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/ger.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/fra.1',
        'https://www.espn.com.co/futbol/posiciones/_/liga/por.1',
        ]

    ligas = [ 'Spain', 'Italy', 'England', 'Germany', 'France', 'Portugal']

    df = {
        'Liga': ligas, 
        'url': urls
    }
    df_2 = data_processing(df)
    print(df_2)
    #print( range(len(df['Liga'])))


