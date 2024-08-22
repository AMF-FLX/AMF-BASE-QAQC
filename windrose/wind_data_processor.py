import argparse
import calendar
import numpy as np
import pandas as pd
import time

from pathlib import Path

from wind_pattern_analyzer import WindroseAnalysis


class WindroseProcessor:
    def __init__(self, input_directory, output_directory='windRoseData'):
        """
        Initialize WindroseProcessor with directories.

        Args:
            input_directory (str): Path to the root directory containing site
            data.
            output_directory (str): Path to the directory where processed data
            will be saved.
        """
        self.input_directory = Path(input_directory)
        self.output_directory = Path(output_directory)

        if not self.output_directory.exists():
            self.output_directory.mkdir(parents=True)

    def process_site_files(self):
        """
        Process files for each site directory.
        """
        site_list = [
            site for site in self.input_directory.iterdir()
            if site.is_dir()
        ]

        for site in site_list:
            file_list = list(site.iterdir())
            # Filtering files based on naming conventions
            filtered_files = [
                file for file in file_list
                if '_BASE_HH_' in file.name or '_FULLSET_HH_' in file.name
            ]

            for filtered_file in filtered_files:
                df = pd.read_csv(filtered_file, skiprows=[0, 1])

                if 'WS' not in df.columns or 'WD' not in df.columns:
                    print('Both WS and WD columns must be present in the file')
                else:
                    self.process_season_data(df, site)

    import calendar

    def process_season_data(self, df, site):
        """
        Process seasonal wind data for a given site.

        Args:
            df (pandas.DataFrame): DataFrame containing the wind data.
            site (Path): Path to the site directory.
        """
        missing_value = -9999
        main_df = df[(df['WD'] != missing_value) & (df['WS'] != missing_value)]
        speed = main_df['WS']
        direction = main_df['WD']
        timestamp = main_df['TIMESTAMP_START']
        time_series = pd.to_datetime(timestamp, format='%Y%m%d%H%M')
        hour_series = time_series.dt.strftime('%H%M').astype(int)
        month_series = time_series.dt.month
        year_start = time_series.dt.year.iloc[0]
        year_end = time_series.dt.year.iloc[-1]

        all_season_df = pd.DataFrame({
            'month': month_series,
            'hour': hour_series,
            'speed': speed,
            'direction': direction
        })

        # Convert calendar.month_abbr to a list
        month_abbr_list = list(calendar.month_abbr)

        # Defining seasons using the list of month abbreviations
        spring = [month_abbr_list.index('Mar'), month_abbr_list.index('Apr'),
                  month_abbr_list.index('May')]  # Spring
        summer = [month_abbr_list.index('Jun'), month_abbr_list.index('Jul'),
                  month_abbr_list.index('Aug')]  # Summer
        autumn = [month_abbr_list.index('Sep'), month_abbr_list.index('Oct'),
                  month_abbr_list.index('Nov')]  # Autumn
        winter = [month_abbr_list.index('Dec'), month_abbr_list.index('Jan'),
                  month_abbr_list.index('Feb')]  # Winter

        # 6:00 AM to 6:00 PM
        daytime_hours = list(range(600, 1810, 10))

        # Nighttime: 12:00 AM to 6:00 AM, 6:00 PM to 12:00 AM
        nighttime_hours = list(range(0, 610, 10)) + list(range(1800, 2410, 10))

        seasons = [list(range(1, 13)), spring, summer, autumn, winter]
        day_night_intervals = [daytime_hours, nighttime_hours]
        season_names = ['Full_Year', 'Spring', 'Summer', 'Autumn', 'Winter']
        time_names = ['Daytime', 'Nighttime']
        ndirections = 16  # Number of wind direction bins
        # Wind speed bins (standard)
        wind_speed_bins1 = [0, 1, 4, 9, 16, 26, 37, 50]

        # 95th percentile of wind speed
        percentile95 = np.percentile(speed, 95)

        # Wind speed bins (based on 95th percentile)
        wind_speed_bins2 = np.linspace(0, percentile95, 8)
        wind_speed_bins_list = [wind_speed_bins1, wind_speed_bins2]

        analyzer = WindroseAnalysis()

        # Splitting site name based on underscore
        filename_parts = site.name.split('_')

        site_id = filename_parts[1]  # Dynamically extract site ID

        version = f'{year_start}_{year_end}'  # Define version based on years
        if 'BASE' in site.name:
            data_product = filename_parts[2].split('-')[
                0]  # Extract data product info
        elif 'FLUXNET' in site.name:
            data_product = filename_parts[2]

        output_directory_path = (self.output_directory / site_id /
                                 data_product / version)

        if not output_directory_path.exists():
            output_directory_path.mkdir(parents=True)

        for bin_set_index, wind_speed_bins in enumerate(wind_speed_bins_list):
            for season_index, season in enumerate(seasons):
                season_df = all_season_df[all_season_df['month'].isin(season)]
                if season_index == 0:
                    filename = (output_directory_path /
                                f'{season_names[season_index]}_'
                                f'{bin_set_index + 1}')
                    self.save_output(analyzer, ndirections, wind_speed_bins,
                                     season_df, filename)
                else:
                    for time_index, time_period in enumerate(
                            day_night_intervals):
                        season_time_df = season_df[
                            season_df['hour'].isin(time_period)]
                        filename = output_directory_path / (
                            f'{season_names[season_index]}_'
                            f'{time_names[time_index]}_{bin_set_index + 1}')
                        self.save_output(analyzer, ndirections,
                                         wind_speed_bins, season_time_df,
                                         filename)

    def save_output(self, analyzer, ndirections, wind_speed_bins, season_df,
                    filename):
        """
        Save the output of the windrose analysis to a file.

        Args:
            analyzer (WindroseAnalysis): Instance of WindroseAnalysis to
            process data.
            ndirections (int): Number of wind direction bins.
            wind_speed_bins (array-like): Wind speed bins to use for analysis.
            season_df (pandas.DataFrame): DataFrame containing the seasonal
            data.
            filename (Path): Path to save the output file.
        """
        analyzer.process_season_data(ndirections, wind_speed_bins, season_df,
                                     filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process windrose data.')
    parser.add_argument('-d', '--directory', required=True,
                        help='Root directory containing the site data.')

    args = parser.parse_args()
    input_directory = args.directory

    start_time = time.time()

    processor = WindroseProcessor(input_directory)
    processor.process_site_files()

    end_time = time.time()
    print(f'Time taken: {end_time - start_time} seconds')
