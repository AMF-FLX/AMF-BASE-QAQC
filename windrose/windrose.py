import numpy as np
import pandas as pd


class WindroseAnalysis:
    def __init__(self, reference_north=90, reference_east=0):
        """
        Initialize WindroseAnalysis with reference directions.

        Args:
            reference_north (int): Reference direction for North in degrees.
            reference_east (int): Reference direction for East in degrees.
        """
        self.north = reference_north
        self.east = reference_east

    def calculate_wind_frequency(self,
                                 ndirections, wind_speed_bins, speed,
                                 corrected_direction, direction):
        """
        Calculate the frequency of wind speed and direction.

        Args:
            ndirections (int): Number of direction bins (e.g., 16).
            wind_speed_bins (array-like): Wind speed bins (e.g., [0, 1, 4, 9,
            16, 26, 37, 50]).
            speed (numpy.ndarray): Array of wind speeds.
            corrected_direction (numpy.ndarray): Array of
            corrected wind directions.
            direction (numpy.ndarray): Array of wind directions.

        Returns:
            numpy.ndarray: Frequency distribution of wind speed and direction.
        """

        # center_angle: Angles at which direction bins are centered

        center_angle = np.linspace(0, 360, ndirections + 1)[:-1]
        half_bin_width = 180 / ndirections
        frequency_matrix = np.zeros((len(center_angle), len(wind_speed_bins)))

        for i in range(len(center_angle)):
            lower_bound = np.mod((center_angle[i] - half_bin_width), 360)
            upper_bound = center_angle[i] + half_bin_width

            if lower_bound > upper_bound:
                condition = np.logical_or(corrected_direction >= lower_bound,
                                          corrected_direction < upper_bound)
            else:
                condition = np.logical_and(corrected_direction >= lower_bound,
                                           corrected_direction < upper_bound)

            speed_histogram, _ = np.histogram(speed[condition],
                                              bins=np.append(wind_speed_bins,
                                                             np.inf))
            frequency_matrix[i, :] = np.cumsum(
                speed_histogram[:len(wind_speed_bins)])

        return frequency_matrix / len(direction) * 100

    def generate_frequency_table(self, ndirections, wind_speed_bins, speed,
                                 corrected_direction, direction):
        """
        Generate a frequency table of wind speed and direction.

        Args:
            ndirections (int): Number of direction bins (e.g., 16).
            wind_speed_bins (array-like): Wind speed bins (e.g.,
            [0, 1, 4, 9, 16, 26, 37, 50]).
            speed (numpy.ndarray): Array of wind speeds.
            corrected_direction (numpy.ndarray): Array of
            corrected wind directions.
            direction (numpy.ndarray): Array of wind directions.

        Returns:
            pandas.DataFrame: Frequency table.
        """

        frequency_matrix = self.calculate_wind_frequency(
            ndirections, wind_speed_bins, speed, corrected_direction,
            direction)
        frequency_matrix = np.hstack((frequency_matrix[:, [0]],
                                      np.diff(frequency_matrix, axis=1)))
        column_names = (
                [f'Wind Speed Interval:({wind_speed_bins[0]}, '
                 f'{wind_speed_bins[1]})'] +
                [f'Wind Speed Interval:[{wind_speed_bins[i]}, '
                 f'{wind_speed_bins[i + 1]})'
                 for i in range(1, len(wind_speed_bins) - 1)] +
                [f'Wind Speed Interval:[{wind_speed_bins[-1]}, Inf)']
        )
        frequency_df = pd.DataFrame(frequency_matrix, columns=column_names)

        center_angle = np.linspace(0, 360, ndirections + 1)[:-1]
        directions = np.mod(self.north - center_angle /
                            90 * (self.north - self.east), 360)
        half_bin_width = 180 / ndirections

        wind_direction_interval = \
            [(f'[{np.mod((i - half_bin_width), 360): .2f}, '
              f'{(i + half_bin_width): .2f})')
                for i in directions]

        # Convert wind_direction_interval to pandas Series
        wind_direction_interval_series = pd.Series(wind_direction_interval)

        frequency_df.insert(0, 'Direction Interval (Â°)',
                            wind_direction_interval_series)
        frequency_df.insert(1, 'Avg. Direction', directions)
        frequency_df = frequency_df.sort_values(by='Avg. Direction')

        return frequency_df

    def process_season_data(self, ndirections, wind_speed_bins, season_df,
                            filename):
        """
        Process seasonal data and generate a frequency table.

        Args:
            ndirections (int): Number of direction bins.
            wind_speed_bins (array-like): Wind speed bins.
            season_df (pandas.DataFrame): Seasonal data.
            filename (str): Output filename.

        Returns:
            None
        """

        speed = season_df['speed'].to_numpy().reshape(-1, 1)
        direction = season_df['direction'].to_numpy().reshape(-1, 1)

        corrected_direction = np.mod((self.north - direction) /
                                     (self.north - self.east) * 90, 360)
        corrected_direction = corrected_direction[speed.flatten() > 0]
        speed = speed[speed.flatten() > 0]

        if len(wind_speed_bins) > 0 and wind_speed_bins[0] != 0:
            wind_speed_bins = np.insert(wind_speed_bins, 0, 0)

        frequency_df = self.generate_frequency_table(
            ndirections, wind_speed_bins, speed, corrected_direction,
            direction)

        frequency_df.to_csv(f'{filename}.csv', index=False)
