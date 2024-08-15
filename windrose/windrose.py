import numpy as np
import pandas as pd


class WindroseAnalysis:
    def __init__(self, ref_n=90, ref_e=0):
        self.north = ref_n
        self.east = ref_e

    @staticmethod
    def calculate_wind_frequency(ndirections, vwinds, speed, dirc, direction):
        """
        Calculate the frequency of wind speed and direction.

        Args:
            ndirections (int): Number of direction bins.
            vwinds (array-like): Wind speed bins.
            speed (numpy.ndarray): Array of wind speeds.
            dirc (numpy.ndarray): Array of wind directions (corrected).
            direction (numpy.ndarray): Array of wind directions.

        Returns:
        numpy.ndarray: Frequency distribution of wind speeds and directions.
        """
        center_angle = np.linspace(0, 360, ndirections + 1)[:-1]
        n = 180 / ndirections
        count = np.zeros((len(center_angle), len(vwinds)))

        for i in range(len(center_angle)):
            d1 = np.mod((center_angle[i] - n), 360)
            d2 = center_angle[i] + n

            if d1 > d2:
                cond = (dirc >= d1) or (dirc < d2)
            else:
                cond = (dirc >= d1) and (dirc < d2)

            counter, _ = np.histogram(speed[cond],
                                      bins=np.append(vwinds, np.inf))
            count[i, :] = np.cumsum(counter[:len(vwinds)])

        return count / len(direction) * 100

    def generate_frequency_table(self,
                                 ndirections, vwinds, speed, dirc, direction):
        """
        Generate a frequency table of wind speed and direction.

        Args:
            ndirections (int): Number of direction bins.
            vwinds (array-like): Wind speed bins.
            speed (numpy.ndarray): Array of wind speeds.
            dirc (numpy.ndarray): Array of wind directions (corrected).
            direction (numpy.ndarray): Array of wind directions.

        Returns:
            pandas.DataFrame: Frequency table.
        """

        count = self.calculate_wind_frequency(ndirections,
                                              vwinds, speed, dirc, direction)
        count = np.hstack((count[:, [0]], np.diff(count, axis=1)))
        column_names = (
                [f'Wind Speed Interval:({vwinds[0]}, {vwinds[1]})'] +
                [f'Wind Speed Interval:[{vwinds[i]}, '
                 f'{vwinds[i + 1]})' for i in range(1, len(vwinds) - 1)] +
                [f'Wind Speed Interval:[{vwinds[-1]}, Inf)']
        )
        count_df = pd.DataFrame(count, columns=column_names)

        # center_angle is the angles in which direction bins are centered.
        # We do not want the 360 to appear, because 0 is already appearing.
        center_angle = np.linspace(0, 360, ndirections + 1)[:-1]
        directions = np.mod(
            self.north - center_angle / 90 * (self.north - self.east), 360)
        n = 180 / ndirections

        wdirs = [(f"[{np.mod((i - n), 360): .2f}, "
                  f"{(i + n): .2f})") for i in directions]

        wdirs_series = pd.Series(wdirs)  # Convert wdirs to pandas Series

        count_df.insert(0, 'Direction Interval (Â°)', wdirs_series)
        #
        # count_df.insert(0, 'Direction Interval (Â°)', wdirs)
        count_df.insert(1, 'Avg. Direction', directions)
        count_df = count_df.sort_values(by='Avg. Direction')

        windzero_frequency = 100 - np.sum(count)
        windzero_frequency = windzero_frequency * (
                windzero_frequency / 100 > np.finfo(float).eps)
        count_df.loc['No Direction'] = (['[0 , 360)', 'Wind Speed = 0'] +
                                        [''] * (count_df.shape[1] - 3)
                                        + [str(windzero_frequency)])
        count_df['total'] = count_df.iloc[:, 2:].sum(axis=1)
        count_df.loc['Total'] = count_df.iloc[:16, 2:].sum(axis=0)

        return count_df

    def process_season_data(self, ndirections, vwinds, season_df, filename):
        """
        Process seasonal data and generate a frequency table.

        Args:
            ndirections (int): Number of direction bins.
            vwinds (array-like): Wind speed bins.
            season_df (pandas.DataFrame): Seasonal data.
            filename (str): Output filename.

        Returns:
            None
        """

        speed = season_df['speed'].to_numpy().reshape(-1, 1)
        direction = season_df['direction'].to_numpy().reshape(-1, 1)

        dirc = np.mod((self.north - direction) / (
                self.north - self.east) * 90, 360)
        dirc = dirc[speed.flatten() > 0]
        speed = speed[speed.flatten() > 0]

        if len(vwinds) > 0 and vwinds[0] != 0:
            vwinds = np.insert(vwinds, 0, 0)

        count_df = self.generate_frequency_table(
            ndirections, vwinds, speed, dirc, direction)

        if 99.9999 <= count_df.iloc[-1, -1] < 100.0001:
            table = count_df.drop(
                index=['No Direction', 'Total'], columns=['total'])
            table.to_csv(f'{filename}.csv', index=False)
        else:
            print(f'{filename} total is not 100%')
