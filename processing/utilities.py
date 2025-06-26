import pandas as pd

def remove_outliers(df: pd.DataFrame, col: str, std_threshold: int = 3):
    mean = df[col].mean()
    std = df[col].std()
    return df[(df[col] >= mean - std_threshold * std) & (df[col] <= mean + std_threshold * std)]