"""
Our traffic data web application.
"""

import streamlit as st
import pandas as pd
import numpy as np
import calcDaily
import calcMonthly
import import_user_day
import import_user_month
from kepler_output import kep_out
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
import geopandas as gpd
from shapely.geometry import Point


@st.cache
def convert_df(df):
    return df.to_csv().encode('utf-8')

# @st.cache


def retrieveDF(table: str) -> pd.DataFrame:
    AWS_ACCESS_KEY_ID = "Enter access id key"
    AWS_SECRET_ACCESS_KEY = "Enter Secret access key"

    # Specify different files for different types of data (here its split into day, raw, month frequency and new means data getting appended daily based on streaming data frequency)
    if table == 'raw':
        AWS_S3_BUCKET = f"s3://551projectcsv/load/Demo_Traffic_Collision_Data_from_2010_to_Present.csv"
    elif table == 'day':
        AWS_S3_BUCKET = f"s3://551projectcsv/load/Day_Traffic_Collision_Data_from_2010_to_Present.csv"
    elif table == 'month':
        AWS_S3_BUCKET = f"s3://551projectcsv/load/Month_Traffic_Collision_Data_from_2010_to_Present.csv"
    elif table == 'new':
        AWS_S3_BUCKET = f"s3://551projectcsv/load/New_Traffic_Collision_Data_from_2010_to_Present.csv"

    df = pd.read_csv(
        AWS_S3_BUCKET,
        storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY
        },
    )

    return df


page_view = st.sidebar.radio(label="",
                             options=(
                                 'Home', 'Explore The Data', 'View Geospatial Risk Ratings', 'Upload Your Own Dataset'),
                             index=0)

if page_view == 'Home':
    st.title('Geospatial Patterns in Los Angeles Traffic Collision Data')
    st.subheader('\
    According to the 2021 Urban Mobility Report by the Texas A&M Transportation Institute,\
    Los Angeles is the fourth most congested city in the United States with an average\
    yearly driver traffic time of 46 hours.')
    st.subheader('')
    st.subheader(
        'Our application helps you discover collision patterns throughout the Los Angeles area and avoid high risk areas.')
    st.text('')
    st.text('Explore traffic collision data throughout Los Angeles and attain risk rating reports for different areas!')
    st.text('')
    st.info('Use the buttons on the leftside panel to navigate the application.')

elif page_view == 'Explore The Data':
    st.subheader(
        'This dataset reflects traffic collision incidents in the City of Los Angeles dating back to 2010.')
    st.subheader('')
    st.text("Please choose a dataset to view.")
    st.text('')

    choice = st.selectbox(label="Datasets",
                          options=('Raw', 'Daily', 'Monthly', 'New'),
                          index=3)

    if choice == 'Raw':
        data_load_state = st.text('Loading data...')
        data = retrieveDF(table='raw')
        st.dataframe(data)
        data_load_state.text('')
        csv = convert_df(data)
        st.download_button(
            label='Download Data (CSV)',
            data=csv,
            file_name='Traffic_Data_raw.csv',
            mime='text/csv'
        )
        config = {
            'version': 'v1',
            'config': {
                'visState': {
                    'filters': [{
                        'dataId': ['traffic'],
                        'id': 'yvfgliwpp',
                        'name': ['Date Occurred'],
                        'type': 'timeRange',
                        'value': [1262304000000, 1293978171000],
                        'enlarged': True,
                        'plotType': 'histogram',
                        'animationWindow': 'free',
                        'yAxis': None,
                        'speed': 1
                    }],
                    'layers': [{
                        'id': '12bjkpja',
                        'type': 'point',
                        'config': {
                            'dataId':
                            'traffic',
                            'label':
                            'Point',
                            'color': [18, 147, 154],
                            'highlightColor': [252, 242, 26, 255],
                            'columns': {
                                'lat': 'latitude',
                                'lng': 'longitude',
                                'altitude': None
                            },
                            'isVisible':
                            True,
                            'visConfig': {
                                'radius': 10,
                                'fixedRadius': False,
                                'opacity': 0.8,
                                'outline': False,
                                'thickness': 2,
                                'strokeColor': None,
                                'colorRange': {
                                    'name':
                                    'Global Warming',
                                    'type':
                                    'sequential',
                                    'category':
                                    'Uber',
                                    'colors': [
                                        '#5A1846', '#900C3F', '#C70039',
                                        '#E3611C', '#F1920E', '#FFC300'
                                    ]
                                },
                                'strokeColorRange': {
                                    'name':
                                    'Global Warming',
                                    'type':
                                    'sequential',
                                    'category':
                                    'Uber',
                                    'colors': [
                                        '#5A1846', '#900C3F', '#C70039',
                                        '#E3611C', '#F1920E', '#FFC300'
                                    ]
                                },
                                'radiusRange': [0, 50],
                                'filled': True
                            },
                            'hidden':
                            False,
                            'textLabel': [{
                                'field': None,
                                'color': [255, 255, 255],
                                'size': 18,
                                'offset': [0, 0],
                                'anchor': 'start',
                                'alignment': 'center'
                            }]
                        },
                        'visualChannels': {
                            'colorField': {
                                'name': 'Time Occurred',
                                'type': 'integer'
                            },
                            'colorScale': 'quantile',
                            'strokeColorField': None,
                            'strokeColorScale': 'quantile',
                            'sizeField': None,
                            'sizeScale': 'linear'
                        }
                    }],
                    'interactionConfig': {
                        'tooltip': {
                            'fieldsToShow': {
                                'traffic': [{
                                    'name': 'DR Number',
                                    'format': None
                                }, {
                                    'name': 'Date Reported',
                                    'format': None
                                }, {
                                    'name': 'Date Occurred',
                                    'format': None
                                }, {
                                    'name': 'Time Occurred',
                                    'format': None
                                }, {
                                    'name': 'Area ID',
                                    'format': None
                                }]
                            },
                            'compareMode': False,
                            'compareType': 'absolute',
                            'enabled': True
                        },
                        'brush': {
                            'size': 0.5,
                            'enabled': False
                        },
                        'geocoder': {
                            'enabled': False
                        },
                        'coordinate': {
                            'enabled': False
                        }
                    },
                    'layerBlending':
                    'normal',
                    'splitMaps': [],
                    'animationConfig': {
                        'currentTime': None,
                        'speed': 1
                    }
                },
                'mapState': {
                    'bearing': 0,
                    'dragRotate': False,
                    'latitude': 34.02276961340012,
                    'longitude': -118.32162909146061,
                    'pitch': 0,
                    'zoom': 9.73472695533332,
                    'isSplit': False
                },
                'mapStyle': {
                    'styleType':
                    'dark',
                    'topLayerGroups': {},
                    'visibleLayerGroups': {
                        'label': True,
                        'road': True,
                        'border': False,
                        'building': True,
                        'water': True,
                        'land': True,
                        '3d building': False
                    },
                    'threeDBuildingColor':
                    [9.665468314072013, 17.18305478057247, 31.1442867897876],
                    'mapStyles': {}
                }
            }
        }

        data['Location'] = data['Location'].str.strip("()")
        data['Location'] = data['Location'].str.strip()
        data[['latitude', 'longitude']] = data['Location'].str.split(
            ',', expand=True)
        data['latitude'] = data.latitude.apply(lambda x: float(x))
        data['longitude'] = data.longitude.apply(lambda x: float(x))
        data.drop(columns=['Location'], inplace=True)

        data['Date Occurred'] = pd.to_datetime(
            data['Date Occurred']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # print(data)
        #data['Location'] = data['Location'].apply(Point)
        #raw_gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.latitude, data.longitude))
        map_1 = KeplerGl(height=1000, width=800, config=config)
        map_1.add_data(data, 'traffic')
        keplergl_static(map_1)

    elif choice == 'Daily':
        data_load_state = st.text('Loading data...')
        data = retrieveDF(table='day')
        st.dataframe(data)
        data_load_state.text('')
        csv = convert_df(data)
        st.download_button(
            label='Download Data (CSV)',
            data=csv,
            file_name='Traffic_Data_Daily.csv',
            mime='text/csv'
        )
    elif choice == 'Monthly':
        data_load_state = st.text('Loading data...')
        data = retrieveDF(table='month')
        st.dataframe(data)
        data_load_state.text('')
        csv = convert_df(data)
        st.download_button(
            label='Download Data (CSV)',
            data=csv,
            file_name='Traffic_Data_Monthly.csv',
            mime='text/csv'
        )
    elif choice == 'New':
        data_load_state = st.text('Loading data...')
        data = retrieveDF(table='new')
        st.dataframe(data)
        data_load_state.text('')
        csv = convert_df(data)
        st.download_button(
            label='Download Data (CSV)',
            data=csv,
            file_name='Traffic_Data_New.csv',
            mime='text/csv'
        )
elif page_view == 'View Geospatial Risk Ratings':
    st.subheader(
        'Our tool allows you to attain a traffic-collision risk-rating report across Los Angeles.')
    st.subheader('')
    st.text("Next Month Predicted Risk Label For Each LAPD Divisions")
    st.text('')
    # Manny's code here with parameter options?
    comb, config = kep_out("traffic_monthly")
    map_1 = KeplerGl(height=1000, width=800, config=config)
    map_1.add_data(comb, 'traffic')
    keplergl_static(map_1)


elif page_view == 'Upload Your Own Dataset':
    st.subheader('Upload Your Own Dataset to Attain Risk Ratings!')
    st.subheader('')

    form = st.form('my_form')
    uploaded_file = form.file_uploader(label='Upload a CSV File', type='csv')
    selection = form.selectbox(label='Choose a Time Aggregation.',
                               options=('Daily', 'Monthly'))

    form.form_submit_button("Submit")

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.dataframe(df)
        data_load_state = st.text('Loading data and training models...')

        if selection == 'Daily':
            calcDaily.transformToDay(df)
            import_user_day.importDay()

        elif selection == 'Monthly':
            calcMonthly.transformToMonth(df)
            import_user_month.importMonth()

            # Predicted output for monthly frequency is displayed on the keppler UI
            comb, config = kep_out("traffic_monthly_uploaded")

            st.text("Next Month Predicted Risk Label For Each LAPD Divisions")
            st.text('')

            map_1 = KeplerGl(height=1000, width=800, config=config)
            map_1.add_data(comb, 'traffic')
            keplergl_static(map_1)

        data_load_state.text('')
