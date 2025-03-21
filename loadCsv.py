import pandas as pd


def loadCsv(file: str, columnsToRemove: list, indexToSet: pd.Index):
    df = pd.read_csv(file)
    df = df.drop(columns=columnsToRemove, errors='ignore')  # 'errors=ignore' prevents errors if a column is missing

    # Reverse time order (starts with 31/12/2022)
    df = df.apply(lambda col: col[::-1].values)

    # Set index to the index of the simulation
    df = df.set_index(indexToSet)

    return df