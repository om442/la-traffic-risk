import geopandas as gpd
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
import pandas as pd
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from keplergl import KeplerGl
from pyspark.ml.classification import LogisticRegression
from traff_pred import traffic_pred


def kep_out(data_loc):
    spark = SparkSession.builder.appName(
        "traffic").enableHiveSupport().getOrCreate()

    gdf = gpd.read_file(
        'https://opendata.arcgis.com/datasets/031d488e158144d0b3aecaa9c888b7b3_0.geojson'
    )

    jdbcURL = "Enter jdbc url with auth credentials to access redshift"
    tempS3Dir = "s3://551projectcsv/load/"



    # Read data from a table
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbcURL) \
        .option("dbtable", data_loc) \
        .option("tempdir", tempS3Dir) \
        .load()

    pd_df = df.select(['date_occ', 'area', 'counts', 'risk_label']).toPandas()
    t_df = traffic_pred()

    t_df = t_df.select(['date_occ', 'area', 'counts', 'risk_label']).toPandas()

    if data_loc == "traffic_monthly":
        pd_df['date_occ'] = pd.to_datetime(pd_df['date_occ'])
        t_df['date_occ'] = pd.to_datetime(t_df['date_occ'])
        pd_df = pd_df[pd_df['date_occ'].dt.year != 2021]

    pd_df = pd_df.loc[pd_df.date_occ == pd_df.date_occ.max(),
                      ['area', 'counts', 'risk_label']]

    config = {
        'version': 'v1',
        'config': {
            'visState': {
                'filters': [],
                'layers': [{
                    'id': '13zk91o',
                    'type': 'geojson',
                    'config': {
                        'dataId':
                        'traffic',
                        'label':
                        'traffic',
                        'color': [248, 149, 112],
                        'highlightColor': [252, 242, 26, 255],
                        'columns': {
                            'geojson': 'geometry'
                        },
                        'isVisible':
                        True,
                        'visConfig': {
                            'opacity': 0.8,
                            'strokeOpacity': 0.8,
                            'thickness': 0.5,
                            'strokeColor': [130, 154, 227],
                            'colorRange': {
                                'name': 'Global Warming 3',
                                'type': 'sequential',
                                'category': 'Uber',
                                'colors': ['#FFC300', '#C70039', '#5A1846'],
                                'reversed': True
                            },
                            'strokeColorRange': {
                                'name':
                                'Global Warming',
                                'type':
                                'sequential',
                                'category':
                                'Uber',
                                'colors': [
                                    '#5A1846', '#900C3F', '#C70039', '#E3611C',
                                    '#F1920E', '#FFC300'
                                ]
                            },
                            'radius': 10,
                            'sizeRange': [0, 10],
                            'radiusRange': [0, 50],
                            'heightRange': [0, 500],
                            'elevationScale': 5,
                            'enableElevationZoomFactor': True,
                            'stroked': True,
                            'filled': True,
                            'enable3d': False,
                            'wireframe': False
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
                            'name': 'counts',
                            'type': 'integer'
                        },
                        'colorScale': 'quantile',
                        'strokeColorField': None,
                        'strokeColorScale': 'quantile',
                        'sizeField': None,
                        'sizeScale': 'linear',
                        'heightField': None,
                        'heightScale': 'linear',
                        'radiusField': None,
                        'radiusScale': 'linear'
                    }
                }],
                'interactionConfig': {
                    'tooltip': {
                        'fieldsToShow': {
                            'traffic': [{
                                'name': 'APREC',
                                'format': None
                            }, {
                                'name': 'PREC',
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
                'latitude': 34.02576329530199,
                'longitude': -118.37093163112915,
                'pitch': 0,
                'zoom': 9.469570305116328,
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

    comb = gdf.merge(pd_df, left_on='PREC', right_on='area')
    comb1 = gdf.merge(t_df, left_on='PREC', right_on='area')
    print(type(comb1))
    print(type(comb))
    print('1', comb1)
    print('2nd', comb)
    return comb1, config
