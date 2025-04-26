from datetime import datetime

import pandas as pd

def loadCsv(file: str, columnsToRemove: list):

    start = datetime(2022, 1, 1)
    end = datetime(2022, 12, 31, 23, 45, 00)
    index = pd.date_range(
        start=start,
        end=end,
        freq="15min",
    )

    df = pd.read_csv(file)
    df = df.drop(columns=columnsToRemove, errors='ignore')  # 'errors=ignore' prevents errors if a column is missing

    # Reverse time order (starts with 31/12/2022)
    df = df.apply(lambda col: col[::-1].values)

    # Set index to the index of the simulation
    df = df.set_index(index)

    # Resample to hourly data by summing every 4 quarters (15 min each)
    df = df.resample('h').mean()

    return df