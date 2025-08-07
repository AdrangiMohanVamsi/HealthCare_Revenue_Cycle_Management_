import pandas as pd
from datetime import datetime, timedelta

def generate_date_dim(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date)
    df = pd.DataFrame({'date': dates})

    df['date_sk'] = df['date'].dt.strftime('%Y%m%d').astype(int)
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['quarter'] = df['date'].dt.quarter
    df['day_name'] = df['date'].dt.day_name()
    df['is_weekend'] = df['day_name'].isin(['Saturday', 'Sunday'])

    return df

if __name__ == "__main__":
    start_date = '2020-01-01'
    end_date = '2030-12-31'
    date_df = generate_date_dim(start_date, end_date)
    print("✅ dim_date preview:")
    print(date_df.head())
    date_df.to_csv("dim_date.csv", index=False)
