import pandas as pd
import datetime 
def transfrom_maven():
    mvn_cust = pd.read_csv("Files for Technical Test - New\Files for Technical Test\Files for Python Test\maven_music_customers.csv")
    mvn_hist = pd.read_excel("Files for Technical Test - New\Files for Technical Test\Files for Python Test\maven_music_listening_history.xlsx")

    #customer id to string
    mvn_cust['Customer ID'] = mvn_cust['Customer ID'].astype(str)
    #convert date column to datetime
    mvn_cust['Member Since'] = pd.to_datetime(mvn_cust['Member Since'])
    mvn_cust['Cancellation Date'] = pd.to_datetime(mvn_cust['Cancellation Date'])
    #fill discount with No
    mvn_cust['Discount?'] = mvn_cust['Discount?'].fillna('No')
    #Subscription Plan fill with Basic(Ads)
    mvn_cust['Subscription Plan'] = mvn_cust['Subscription Plan'].fillna('Basic (Ads)')

    #create column is_active from cancellation date
    mvn_cust['is_active'] = mvn_cust['Cancellation Date'].isnull().map({True: 1, False: 0})

    #create new column member_duration, which take the difference between cancellation date and member since and fill with current date
    mvn_cust['member_duration'] = mvn_cust['Cancellation Date'] - mvn_cust['Member Since']
    mvn_cust['member_duration'] = mvn_cust['member_duration'].fillna(datetime.datetime.now() - mvn_cust['Member Since'])
    mvn_cust['member_duration'] = mvn_cust['member_duration'].dt.days

    #convert Customer ID to string
    mvn_hist['Customer ID'] = mvn_hist['Customer ID'].astype(str)
    #convert Session ID to string
    mvn_hist['Session ID'] = mvn_hist['Session ID'].astype(str)
    #convert Audio ID to string
    mvn_hist['Audio ID'] = mvn_hist['Audio ID'].astype(str)

    #get total session per customer
    mvn_hist_grouped = mvn_hist.groupby('Customer ID').agg({'Session ID': 'nunique'}).reset_index()
    mvn_hist_grouped.columns = ['Customer ID', 'Total Session']

    #get total song played per customer
    mvn_hist_grouped_audio = mvn_hist.groupby('Customer ID').agg({'Audio ID': 'count'}).reset_index()
    mvn_hist_grouped_audio.columns = ['Customer ID', 'Total Audio']

    #get average song played per session per customer
    mvn_hist_grouped_audio_per_session = mvn_hist.groupby('Customer ID').agg({'Audio ID': 'count', 'Session ID': 'nunique'}).reset_index()
    mvn_hist_grouped_audio_per_session['Average Audio per Session'] = mvn_hist_grouped_audio_per_session['Audio ID'] / mvn_hist_grouped_audio_per_session['Session ID']
    mvn_hist_grouped_audio_per_session = mvn_hist_grouped_audio_per_session[['Customer ID', 'Average Audio per Session']]

    #get total Podcast and song in audio type per customer id
    mvn_hist_grouped_audio_type = mvn_hist.groupby(['Customer ID', 'Audio Type']).size().reset_index(name='Count')
    mvn_hist_grouped_audio_type = mvn_hist_grouped_audio_type.pivot(index='Customer ID', columns='Audio Type', values='Count').reset_index()
    mvn_hist_grouped_audio_type = mvn_hist_grouped_audio_type.fillna(0)
    mvn_hist_grouped_audio_type.columns = ['Customer ID', 'total_Podcast', 'total_Song']

    #get average Podcast and song in audio type per session per customer id
    mvn_hist_grouped_audio_type_per_session = mvn_hist.groupby(['Customer ID', 'Audio Type']).agg({'Audio ID': 'count', 'Session ID': 'nunique'}).reset_index()
    mvn_hist_grouped_audio_type_per_session['Average Audio per Session'] = mvn_hist_grouped_audio_type_per_session['Audio ID'] / mvn_hist_grouped_audio_type_per_session['Session ID']
    mvn_hist_grouped_audio_type_per_session = mvn_hist_grouped_audio_type_per_session.pivot(index='Customer ID', columns='Audio Type', values='Average Audio per Session').reset_index()
    mvn_hist_grouped_audio_type_per_session = mvn_hist_grouped_audio_type_per_session.fillna(0)
    #rename column
    mvn_hist_grouped_audio_type_per_session.columns = ['Customer ID', 'avg_Podcast_per_session', 'avg_Song_per_session']

    #merge all dataframe
    output = mvn_cust.merge(mvn_hist_grouped, on='Customer ID', how='left')\
        .merge(mvn_hist_grouped_audio, on='Customer ID', how='left')\
        .merge(mvn_hist_grouped_audio_per_session, on='Customer ID', how='left')\
        .merge(mvn_hist_grouped_audio_type, on='Customer ID', how='left')\
        .merge(mvn_hist_grouped_audio_type_per_session, on='Customer ID', how='left')
    
    output.to_parquet(r'output\raw\maven_raw.parquet',index=False,engine='fastparquet')


def clean_maven():
    raw_maven = pd.read_parquet(r"output\raw\maven_raw.parquet")
    #map subscription plan to 0 and 1 with 0 is Basic (Ads) and 1 is Premium
    raw_maven['Subscription Plan'] = raw_maven['Subscription Plan'].map({'Basic (Ads)': 0, 'Premium (No Ads)': 1})
    #convert subscription rate to float
    raw_maven['Subscription Rate'] = raw_maven['Subscription Rate'].str.replace('$', '').astype(float)
    #map discount to 0 and 1 with 0 is No and 1 is Yes
    raw_maven['Discount?'] = raw_maven['Discount?'].map({'No': 0, 'Yes': 1})
    # Get only numeric features from raw_maven
    clean = raw_maven[['Subscription Plan', 
            'Subscription Rate',
            'is_active',
            'member_duration',
            'Total Session',
            'Total Audio',
            'Average Audio per Session',
            'total_Podcast',
            'total_Song',
            'avg_Podcast_per_session',
            'avg_Song_per_session']]
    
    clean.to_parquet(r'output\cleaned\maven_clean.parquet',index=False,engine='fastparquet')