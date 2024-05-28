# data_utils.py
import pandas as pd
from datetime import timedelta

def convert_dt(time_string):
    """
    Converts the time to a timedelta value.
    """
    tm1 = time_string.split(':')
    return timedelta(hours=int(tm1[0]), minutes=int(tm1[1]))

def process_data(blob_df):
    """
    Process the dataframe to compute connections.
    """
    blob_df_headers = ['Date', 'Station Name', 'Scheduled Arrival Time', 'Scheduled Departure Time', 'Agency', 'Route', 'Direction', 'Stop', 'Route Stop Sequence']
    blob_df = blob_df.loc[:, blob_df_headers]

    blob_df['Scheduled Arrival Time dt'] = blob_df['Scheduled Arrival Time'].apply(convert_dt)
    blob_df['Scheduled Departure Time dt'] = blob_df['Scheduled Departure Time'].apply(convert_dt)

    go_df = blob_df[blob_df['Agency'] == "GO"].reset_index(drop=True)
    msp_df = blob_df[blob_df['Agency'] != "GO"].reset_index(drop=True)

    go_df['go_trip_key'] = None
    msp_connections = []

    for i, row in go_df.iterrows():
        station_name = row['Station Name']
        trip_date = row['Date']
        trip_arrivalT = row['Scheduled Arrival Time']
        arrival_dt = row['Scheduled Arrival Time dt']
        departure_dt = row['Scheduled Departure Time dt']
        key = f"{trip_date}/{station_name}/{row['Route']}/{trip_arrivalT}/{i}"
        go_df.at[i, 'go_trip_key'] = key

        upper_t = departure_dt + timedelta(minutes=30)
        lower_t = arrival_dt - timedelta(minutes=30)

        time_cond = (msp_df['Scheduled Arrival Time dt'] >= lower_t) & (msp_df['Scheduled Departure Time dt'] <= upper_t)
        location_cond = (msp_df['Station Name'] == station_name)
        date_cond = (msp_df['Date'] == trip_date)

        connections = msp_df[date_cond & time_cond & location_cond].copy()
        connections['go_trip_key'] = key

        msp_connections.append(connections)

    msp_connections_df = pd.concat(msp_connections)
    go_connections_df = go_df.merge(msp_connections_df, how='left', on='go_trip_key', suffixes=('_GO', '_MSP'))

    go_connections_df.loc[go_connections_df['Scheduled Arrival Time dt_GO'] > go_connections_df['Scheduled Arrival Time dt_MSP'], 'Connection_Type'] = "Inbound"
    go_connections_df.loc[go_connections_df['Scheduled Arrival Time dt_GO'] < go_connections_df['Scheduled Arrival Time dt_MSP'], 'Connection_Type'] = "Outbound"
    go_connections_df.loc[~(go_connections_df['Route_MSP'].str.len() > 0), 'Connection_Type'] = "None"
    go_connections_df.loc[go_connections_df['Scheduled Arrival Time dt_GO'] == go_connections_df['Scheduled Arrival Time dt_MSP'], 'Connection_Type'] = "Both"

    go_connections_df['MSP_GO_Trip_Time_Difference'] = go_connections_df.apply(
        lambda row: (row['Scheduled Arrival Time dt_GO'] - row['Scheduled Arrival Time dt_MSP']).seconds / 60 
        if row['Connection_Type'] in ["Inbound", "None"] 
        else (row['Scheduled Arrival Time dt_MSP'] - row['Scheduled Arrival Time dt_GO']).seconds / 60,
        axis=1
    )

    return go_connections_df
