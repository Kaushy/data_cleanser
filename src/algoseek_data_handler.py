#!/usr/bin/python

import os
import pandas as pd
import src.config as config
import pathlib
import sys
import src.utils as utils


def split_data_into_exchanges(source_path, destination_path):
    """
    Splits the data into different exchanges as currently it is one large exchange file. As groupby preserves order we
    do not further sort the outcome. Note that for some reason we store a .DS_Store file which fails. Delete this file
    before running
    Parameters
    ----------
    """
    for subdir, dirs, files in os.walk(source_path):
        for file in files:
            source_full_file = os.path.join(subdir, file)
            print(source_full_file)
            df = pd.read_csv(source_full_file)
            for group_name, df in df.groupby(['Ticker', 'Exchange']):
                file_name = destination_path / str(df['Date'].iloc[0]) / convertTuple(group_name)
                utils.make_dir(file_name)
                with open(file_name, "w+") as f:
                    df.to_csv(f, index=False)


def convertTuple(tup):
    tup_str = '_'.join(tup)
    return str(tup_str) + '.csv'

if __name__ == '__main__':
    split_data_into_exchanges(config.source_algoseek, config.destination_algoseek)