import os
import pandas as pd
import numpy as np
from windrose import WindroseAnalysis
import time
from pathlib import Path
import argparse


class WindroseProcessor:
    def __init__(self, root_directory, directory='windRoseData'):
        self.root_directory = Path(root_directory)
        self.directory = Path(directory)

        if not self.directory.exists():
            self.directory.mkdir(parents=True)

    def process_site_files(self):
        site_list = [
            d for d in self.root_directory.iterdir()
            if d.is_dir() and 'AMF_' in d.name
        ]

        for site in site_list:
            file_list = os.listdir(site)
            filtered_files = [
                file for file in file_list
                if "_BASE-" in file or "_HH_" in file
            ]

            for filtered_file in filtered_files:
                file_path = site / filtered_file
                df = pd.read_csv(file_path, skiprows=[0, 1])

                if 'WS' not in df.columns or 'WD' not in df.columns:
                    print("Both 'WS' and 'WD' or either columns are missing.")
                else:
                    self.process_season_data(df, site)

    def process_season_data(self, df, site):
        df_1 = df[(df['WD'] != -9999) | (df['WS'] != -9999)]
        speed = df_1['WS']
        direction = df_1['WD']
        timestamp = df_1['TIMESTAMP_START']
        time_series = pd.to_datetime(timestamp, format='%Y%m%d%H%M')
        hourz = time_series.dt.strftime('%H%M').astype(int)
        monthz = time_series.dt.month
        year_start = time_series.dt.year.iloc[0]
        year_end = time_series.dt.year.iloc[-1]

        all_season_df = pd.DataFrame({
            'month': monthz,
            'hour': hourz,
            'speed': speed,
            'direction': direction
        })

        all_season = list(range(1, 13))
        spring = [3, 4, 5]
        summer = [6, 7, 8]
        autumn = [9, 10, 11]
        winter = [12, 1, 2]
        day_time = list(range(600, 1810, 10))
        night_time = list(range(0, 610, 10)) + list(range(1800, 2410, 10))
        seasons = [all_season, spring, summer, autumn, winter]
        day_night = [day_time, night_time]
        season_names = ['Full_Year', 'Spring', 'Summer', 'Autumn', 'Winter']
        time_names = ['Daytime', 'Nighttime']
        ndirections = 16
        bins1 = [0, 1, 4, 9, 16, 26, 37, 50]
        percentile95 = np.percentile(speed, 95)
        bins2 = np.linspace(0, percentile95, 8)
        vwinds_list = [bins1, bins2]

        analyzer = WindroseAnalysis()

        dirname = f'{site.name[:10]}_{year_start}_{year_end}'
        directory_path = self.directory / dirname

        if not directory_path.exists():
            directory_path.mkdir(parents=True)

        for j, vwinds in enumerate(vwinds_list):
            for c, season in enumerate(seasons):
                season_df = all_season_df[all_season_df['month'].isin(season)]
                if c == 0:
                    filename = directory_path / f"{season_names[c]}{j + 1}"
                    analyzer.process_season_data(ndirections,
                                                 vwinds, season_df, filename)
                else:
                    for t, ampm in enumerate(day_night):
                        season_time_df = season_df[
                            season_df['hour'].isin(ampm)]
                        filename = directory_path / (f"{season_names[c]}"
                                                     f"{time_names[t]}{j + 1}")
                        analyzer.process_season_data(
                            ndirections, vwinds, season_time_df, filename)


# if __name__ == "__main__":
#     t1 = time.time()
#
#     root_dir = 'Dataset'
#     processor = WindroseProcessor(root_dir)
#     processor.process_site_files()
#
#     t2 = time.time()
#     print(f'Time taken: {t2 - t1} seconds')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process windrose data.")
    parser.add_argument('-d', '--directory', required=True, help="Root directory containing the site data.")

    args = parser.parse_args()
    root_dir = args.directory

    t1 = time.time()

    processor = WindroseProcessor(root_dir)
    processor.process_site_files()

    t2 = time.time()
    print(f'Time taken: {t2 - t1} seconds')