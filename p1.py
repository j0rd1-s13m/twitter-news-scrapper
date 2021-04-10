import tweepy
import pandas as pd
from datetime import date, timedelta
import numpy as np

#Configuració per a pandas per poder copiar un dataframe
pd.options.mode.chained_assignment = None


######################################
#### Part 1
######################################

#Us de la API

#Autenticació per a la API de Twitter
auth = tweepy.OAuthHandler('kcbeFkkjDkRy73HmFpQfYJCVd', '5HogkCd4dolM5CKz7juFR9zxc4QOFm6pamW3qBnnx7AZeClViv')
auth.set_access_token('260171703-Vr9z8EV3EqZWisMyIliH4uHUzBKFv75gngjvbSD3', 'PIGEBLh416IIgi71nVG8Xb5z6PqUUYKOXLWQqDll0sYpe')

#Declarem la api i li passem els valors de la autenticació
api = tweepy.API(auth)

#Usuari per evaluar
userID = "DiariDeSabadell"

#Descarregarem els últims 200 tweets de l'usuari en qüestió
tweets = api.user_timeline(screen_name=userID,count=200,include_rts = False,tweet_mode = 'extended')

#Crearem array per guardar els tweets
all_tweets = []
all_tweets.extend(tweets)

#Guardem a una variable el ID del tweet més vell
oldest_id = tweets[-1].id

#Amb el següent loop intentarem descarregar tweets més vells als 200 tweets descarregats
while True:
    tweets = api.user_timeline(screen_name=userID, count=200, include_rts = False, max_id = oldest_id - 1, tweet_mode = 'extended')
    if len(tweets) == 0:
        break
    oldest_id = tweets[-1].id
    all_tweets.extend(tweets)
    print('Número de Tweets descarregats fins ara {}'.format(len(all_tweets)))

#Crearem un array per tal de poder crear el dataset
d = []

#Per a tots els tweets guardarem la informació que ens interessa en diferents variables
for tweet in all_tweets:
    #Guardarem el ID de cada tweeet
    tweetID = tweet.id
    #Guardarem la creació de cada tweet
    createdAt = tweet.created_at
    #Crearem una nova variable per tenir el dia de creació per separat de la hora
    createdAt_Day = str(createdAt).split(" ")
    createdAt_Day = createdAt_Day[0]
    #Guardarem el número total de "likes" per a cada tweet
    favorites = tweet.favorite_count
    #Guardarem el número total de "retweets" per a cada like
    retweets = tweet.retweet_count
    #Guardarem el contingut del tweet, en aquest cas eliminarem tots els salts de línea per fer-ho monolinea.
    full_Tweet = tweet.full_text.encode("utf-8").decode("utf-8")
    fullTweet = full_Tweet.replace('\n',' ')
    #Guardarem la URL relacionada per a cada tweet. En aquest cas ens quedarem només la última o única URL publicada al tweet.
    #Com a punt de millora podriem treballar amb totes les possibles URL presentades.
    url = ""
    if len(tweet.entities['urls']) != 0:
        for x in range (0, len(tweet.entities['urls'])):
            url = tweet.entities['urls'][0]['expanded_url']

    #Per a cada tweet afegirem els següents caps com "objecte" dins del array.
    d.append(
        {
            'ID': tweetID,
            'Day_Created': createdAt_Day,
            'Created': createdAt,
            'Favorites': favorites,
            'Retweets': retweets,
            'URL': url,
            'fullTweet': fullTweet,
        }
    )

#Definirem un dataframe
created_Dataframe = pd.DataFrame(d)
#Crearem un dataframe amb tota la informació descarregada
created_Dataframe.to_csv('%s_tweets.csv' % userID,index=False,sep=';')


######################################
#### Part 2
######################################

#Postprocessat
#Demanarem a l'usuari que ens indiqui entre quines dates vol revisar els tweets
#start_Date = "2021-04-01"
start_Date = input("Siusplau introdueix una data inicial (format AAAA-MM-DD):\n")
#Imprimirem per pantalla la data inicial
print(f'You entered {start_Date}')
#end_Date = "2021-04-07"
end_Date = input("Siusplau introdueix una data final (format AAAA-MM-DD):\n")
#Imprimirem per pantalla la data final
print(f'You entered {end_Date}')

#Revisarem que el "rang" de dates és correcte. Sempre haurem de mirar un rang del "passat" al "futur"
if (start_Date > end_Date):
    print("Executa novament el script introduïnt una data inicial inferior a la data final")
else:
    #Començarem a preparar el dataset després de fer el check de dates
    print("Dates a consultar:")
    #Convertirem el string introduït en format date.
    start_Date = start_Date.split("-")
    end_Date = end_Date.split("-")
    sdate = date(int(start_Date[0]), int(start_Date[1]), int(start_Date[2]))
    edate = date(int(end_Date[0]), int(end_Date[1]), int(end_Date[2]))
    delta = edate - sdate
    days = []
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        days.append(str(day))
    #Mostrem per pantalla els dies
    print(days)

    #Crearem un dataset amb és dies que ens interessen a partir del dataset amb tots els tweets
    dataFrame_x_days = created_Dataframe.loc[created_Dataframe['Day_Created'].isin(days)]
    #Crearem un nou dataset ordenant els tweets per la URL publicada al tweet
    dataFrame_x_days_sorted_by_url = dataFrame_x_days.sort_values(by=['URL'])

    #Eliminarem del nostre datset les entrades en les que la URL és en blanc ja que només interesen els tweets amb URL.
    dataFrame_x_days_sorted_by_url['URL'].replace('', np.nan, inplace=True)
    dataFrame_x_days_sorted_by_url.dropna(subset=['URL'], inplace=True)
    
    #Printarem per consola el dataframe creat    
    print (dataFrame_x_days_sorted_by_url)

    #Definirem el dataset final especifiant fent agregacions
    #URL publicada
    #Numero total de likes per url
    #Maxim numero de likes que ha rebut una url per separat
    #Minim numero de likes que ha rebut una url per separat
    #Numero total de retweets per url
    #Maxim numero de retweets que ha rebut una url per separat
    #Minim numero de retweets que ha rebut una url per separat
    #Numero total de tweets que han publicat una mateixa URL
    URL_Dataset = dataFrame_x_days_sorted_by_url.groupby('URL').agg(Total_likes=('Favorites', 'sum'), Tweet_max_likes=('Favorites', 'max'), Tweet_min_likes=('Favorites', 'min'), Total_retweets=('Retweets', 'sum'), Tweet_max_retweets=('Retweets', 'max'), Tweet_min_retweets=('Retweets', 'min'), Total_tweets_mateixa_URL=('URL', 'count'))
    #Pintarem el dataset
    print(URL_Dataset)

    #Generarem un fitxer .csv amb el dataset creat
    URL_Dataset.to_csv('repetibilitat_diari_de_sabadell.csv',sep=';')
