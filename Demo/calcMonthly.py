# Importing required packages
import pandas as pd
import numpy as np


# Applying risk label classification to all data points
def getRiskLabel(row, q1, q2):
    if row['risk_weighting'] <= q1:
        label = 'Low'
    elif row['risk_weighting'] > q1 and row['risk_weighting'] <= q2:
        label = 'Medium'
    else:
        label = 'High'

    return label


def getTransformedDF(df: pd.DataFrame) -> pd.DataFrame:
    area_mapping = {
        'Central': 1, 'Rampart': 2, 'Southwest': 3, 'Hollenbeck': 4,
        'Harbor': 5, 'Hollywood': 6, 'Wilshire': 7, 'West LA': 8,
        'Van Nuys': 9, 'West Valley': 10, 'Northeast': 11, '77th Street': 12,
        'Newton': 13, 'Pacific': 14, 'N Hollywood': 15, 'Foothill': 16,
        'Devonshire': 17, 'Southeast': 18, 'Mission': 19, 'Olympic': 20,
        'Topanga': 21
    }

    # Doing some data cleaning
    df['date_rptd'] = pd.to_datetime(df['date_rptd'])
    df['date_occ'] = pd.to_datetime(df['date_occ'])
    df['month_occ'] = df['date_occ'].dt.to_period('M')

    # Creating new dataframe with aggregated counts
    df_new = df.groupby(['month_occ', 'area_name']
                        ).size().reset_index(name='counts')

    # Retrieving all unique combinations and joining counts
    min_date = df_new['month_occ'].min().to_timestamp(how='end')
    max_date = df_new['month_occ'].max().to_timestamp(how='end')
    dates = pd.date_range(min_date, max_date, freq='M')
    areas = df_new['area_name'].unique()
    temp_dict = {'month_occ': [], 'area_name': []}
    for date in dates:
        for area in areas:
            temp_dict['month_occ'].append(date)
            temp_dict['area_name'].append(area)

    combinations = pd.DataFrame.from_dict(temp_dict)
    combinations['month_occ'] = combinations['month_occ'].dt.to_period('M')

    df_new = combinations.merge(df_new,
                                how='left',
                                left_on=['month_occ', 'area_name'],
                                right_on=['month_occ', 'area_name'])

    # Data transformations
    df_new['counts'] = pd.to_numeric(
        df_new['counts'].fillna(0), downcast='integer')
    df_new['count_diff'] = pd.to_numeric(df_new.groupby(['area_name']).diff()[
                                         'counts'].fillna(0), downcast='integer')
    df_new['area'] = df_new['area_name'].replace(area_mapping)
    df_new['month'] = df_new['month_occ'].dt.month
    df_new['year'] = df_new['month_occ'].dt.year
    df_new['risk_weighting'] = df_new['counts'].rank(pct=True)
    q1 = df_new['risk_weighting'].quantile(0.25)
    q2 = df_new['risk_weighting'].quantile(0.75)
    df_new['risk_label'] = df_new.apply(getRiskLabel, args=(q1, q2), axis=1)
    df_new['month_occ'] = df_new['month_occ'].dt.to_timestamp()

    df_new = df_new[['month_occ', 'year', 'month', 'area_name',
                     'area', 'counts', 'count_diff', 'risk_weighting', 'risk_label']]

    return df_new


def writeToS3(df: pd.DataFrame):
    # Write to S3 bucket
    AWS_ACCESS_KEY_ID = "Enter your access key id"
    AWS_SECRET_ACCESS_KEY = "Enter your secret access key"

    df.to_csv(
        f"s3://551projectcsv/load/User_Uploaded_Month_Traffic_Collision_Data_from_2010_to_Present.csv",
        encoding="utf-8",
        storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY
        },
        index=False
    )


def transformToMonth(df: pd.DataFrame):
    df_new = getTransformedDF(df)
    writeToS3(df_new)
