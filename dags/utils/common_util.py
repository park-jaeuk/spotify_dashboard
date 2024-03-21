import os
import logging
import pandas as pd

def save_file(df: pd.DataFrame, target_dir: str, target_file: str) -> None:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        target_dir (str): _description_
        target_file (str): _description_
    """
    save_file_path = os.path.join(target_dir, target_file)
    df.to_csv(save_file_path, index=False) 

def delete_file(df: pd.DataFrame, target_dir: str, target_file: str) -> None:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        target_dir (str): _description_
        target_file (str): _description_
    """
    delete_file_path = os.path.join(target_dir, target_file)
    os.remove(delete_file_path)
