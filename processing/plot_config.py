import os
from configparser import ConfigParser
from logger import Logger
import matplotlib.pyplot as plt
from path_util import PathUtil

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'

_log = Logger().getLogger(__name__)
plt.switch_backend('Agg')


class PlotConfig():
    """Class for handling plotting outputs"""

    # Palette src: https://graphicdesign.stackexchange.com/revisions/3815/8
    hi_contrast_palette = [
        "#000000", "#1CE6FF", "#FF34FF", "#FF4A46", "#008941", "#92896B",
        "#006FA6", "#A30059", "#FFDBE5", "#7A4900", "#0000A6", "#63FFAC",
        "#B79762", "#004D43", "#8FB0FF", "#997D87", "#5A0007", "#809693",
        "#FEFFE6", "#1B4400", "#4FC601", "#3B5DFF", "#4A3B53", "#FF2F80",
        "#61615A", "#BA0900", "#6B7900", "#00C2A0", "#FFAA92", "#FF90C9",
        "#B903AA", "#D16100", "#DDEFFF", "#000035", "#7B4F4B", "#A1C299",
        "#300018", "#0AA6D8", "#013349", "#00846F", "#372101", "#FFB500",
        "#C2FFED", "#A079BF", "#CC0744", "#C0B9B2", "#C2FF99", "#001E09",
        "#00489C", "#6F0062", "#0CBD66", "#EEC3FF", "#456D75", "#B77B68",
        "#7A87A1", "#788D66", "#885578", "#FAD09F", "#FF8A9A", "#D157A0",
        "#BEC459", "#456648", "#0086ED", "#886F4C", "#34362D", "#B4A8BD",
        "#00A6AA", "#452C2C", "#636375", "#A3C8C9", "#FF913F", "#938A81",
        "#575329", "#00FECF", "#B05B6F", "#8CD0FF", "#3B9700", "#04F757",
        "#C8A1A1", "#1E6E00", "#7900D7", "#A77500", "#6367A9", "#A05837",
        "#6B002C", "#772600", "#D790FF", "#9B9700", "#549E79", "#FFF69F",
        "#201625", "#72418F", "#BC23FF", "#99ADC0", "#3A2465", "#922329",
        "#5B4534", "#FDE8DC", "#404E55", "#0089A3", "#CB7E98", "#A4E804",
        "#324E72", "#6A3A4C", "#83AB58", "#001C1E", "#D1F7CE", "#004B28",
        "#C8D0F6", "#A3A489", "#806C66", "#222800", "#BF5650", "#E83000",
        "#66796D", "#DA007C", "#FF1A59", "#8ADBB4", "#1E0200", "#5B4E51",
        "#C895C5", "#320033", "#FF6832", "#66E1D3", "#CFCDAC", "#D0AC94",
        "#7ED379", "#012C58", "#7A7BFF", "#D68E01", "#353339", "#78AFA1",
        "#FEB2C6", "#75797C", "#837393", "#943A4D", "#B5F4FF", "#D2DCD5",
        "#9556BD", "#6A714A", "#001325", "#02525F", "#0AA3F7", "#E98176",
        "#DBD5DD", "#5EBCD1", "#3D4F44", "#7E6405", "#02684E", "#962B75",
        "#8D8546", "#9695C5", "#E773CE", "#D86A78", "#3E89BE", "#CA834E",
        "#518A87", "#5B113C", "#55813B", "#E704C4", "#00005F", "#A97399",
        "#4B8160", "#59738A", "#FF5DA7", "#F7C9BF", "#643127", "#513A01",
        "#6B94AA", "#51A058", "#A45B02", "#1D1702", "#E20027", "#E7AB63",
        "#4C6001", "#9C6966", "#64547B", "#97979E", "#006A66", "#391406",
        "#F4D749", "#0045D2", "#006C31", "#DDB6D0", "#7C6571", "#9FB2A4",
        "#00D891", "#15A08A", "#BC65E9", "#FFFFFE", "#C6DC99", "#203B3C",
        "#671190", "#6B3A64", "#F5E1FF", "#FFA0F2", "#CCAA35", "#374527",
        "#8BB400", "#797868", "#C6005A", "#3B000A", "#C86240", "#29607C",
        "#402334", "#7D5A44", "#CCB87C", "#B88183", "#AA5199", "#B5D6C3",
        "#A38469", "#9F94F0", "#A74571", "#B894A6", "#71BB8C", "#00B433",
        "#789EC9", "#6D80BA", "#953F00", "#5EFF03", "#E4FFFC", "#1BE177",
        "#BCB1E5", "#76912F", "#003109", "#0060CD", "#D20096", "#895563",
        "#29201D", "#5B3213", "#A76F42", "#89412E", "#1A3A2A", "#494B5A",
        "#A88C85", "#F4ABAA", "#A3F3AB", "#00C6C8", "#EA8B66", "#958A9F",
        "#BDC9D2", "#9FA064", "#BE4700", "#658188", "#83A485", "#453C23",
        "#47675D", "#3A3F00", "#061203", "#DFFB71", "#868E7E", "#98D058",
        "#6C8F7D", "#D7BFC2", "#3C3E6E", "#D83D66", "#2F5D9B", "#6C5E46",
        "#D25B88", "#5B656C", "#00B57F", "#545C46", "#866097", "#365D25",
        "#252F99", "#00CCFF", "#674E60", "#FC009C", "#FFFF00"]

    def __init__(self, setup=True):
        """Constructor
        :param setup: Used to determine if setup() is called.
        :type setup: boolean.
        """
        self.plot_title_fontsize = 14
        self.plot_suptitle_fontsize = 20
        self.plot_default_dpi = 200
        if setup:
            self.setup()
        self.path_util = PathUtil()
        self.all_subplots = []

    def setup(self):
        """Tries to read plot_output_dir name
        from configuration file qaqc.cfg and builds path
        based on current working directory.
        """
        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            phase_II_cfg = 'PHASE_II'
            plots_cfg = 'PLOTS'
            config.read_file(cfg)
            if config.has_section(phase_II_cfg) and \
               config.has_section(plots_cfg):
                self.orig_base_dir = config.get(phase_II_cfg, 'output_dir')
                self.base_dir = config.get(phase_II_cfg, 'output_dir')
                self.plot_dir = config.get(plots_cfg, 'plot_output_dir')
            else:
                warning_msg = ('Cannot find plot output dir from config. '
                               'Setting to local output dir')
                _log.warning(warning_msg)
                self.base_dir = os.path.join(cwd, 'preBASE')
                self.plot_dir = 'output'
            if config.has_section('WEBSERVICES'):
                self.url_path = config.get(
                    'WEBSERVICES', 'siteres_qaqc_url_prefix')
            else:
                self.url_path = None
                _log.critical('Cannot find web services from config.')

    def get_ftp_plot_dir_for_run(self, site_id, process_id, ftp_site_dir):
        ''' get the ftp directory full path

        :param site_id: Site ID of the file processed
        :type site_id: str.
        :param process_id: Process ID
        :type process_id: str.
        :param ftp_site_dir: ftp site directory of the file processed
        :type ftp_site_dir: str
        :return: full replacement path
        '''
        plot_dir = self.get_plot_dir_for_run(site_id, process_id)
        temp_dir = plot_dir.replace(site_id, ftp_site_dir)
        ftp_dir = temp_dir.replace(self.orig_base_dir, self.url_path)
        return ftp_dir

    def get_plot_dir_for_run(self, site_id, process_id):
        """Create subdirectories if needed and returns the directory to plot

        :param site_id: Site ID of the file processed
        :type site_id: str.
        :param process_id: Process ID
        :type process_id: str.

        :rtype: str.
        :return: full path to plot directory
        """
        base_dir = self.path_util.create_dir_for_run(
            site_id, process_id, self.base_dir)
        plot_dir = self.path_util.create_valid_path(
            base_dir, self.plot_dir)
        return plot_dir

    def get_plot_dir_for_check(self, plot_dir, check_name):
        """Create subdirectory for check_name if it does not exist and
        return the path of the directory.

        :param check_name: QAQC check module name
        :type check_name: str.

        :rtype: str.
        :return full path to directory
        """
        return self.path_util.create_valid_path(plot_dir, check_name)

    def plot(self, x_vals, y_vals, x_label=None, y_label=None,
             color='.75', marker='o', marker_size=5,
             marker_fill=True, linestyle='', linewidth=0, title='',
             subplot_pos=None, xlim=None, ylim=None, label='',
             is_plot_date=True, reset_all_subplots=False):

        if reset_all_subplots:
            self.all_subplots = []
        if subplot_pos:
            if len(subplot_pos) > 2:
                pos = subplot_pos[2] - 1
                if len(self.all_subplots) < 1:
                    number_rows, number_cols = \
                        subplot_pos[0], subplot_pos[1]
                    self.all_subplots = \
                        [None for _ in range(number_rows * number_cols)]
                if self.all_subplots[pos] is None:
                    self.all_subplots[pos] = plt.subplot(*subplot_pos)
            else:
                pos = 0
                self.all_subplots = [plt.subplot(*subplot_pos)]
        else:
            pos = 0
            self.all_subplots = [plt.gca()]
        ax = self.all_subplots[pos]

        if xlim:
            ax.set_xlim(*xlim)
        if ylim:
            ax.set_ylim(*ylim)
        if x_label:
            ax.set_xlabel(x_label)
        if y_label:
            ax.set_ylabel(y_label)

        if title:
            plot_title = title
        elif x_label and y_label:
            plot_title = f'Plot of {x_label} against {y_label}'
        else:
            plot_title = None

        if plot_title:
            ax.set_title(plot_title, fontsize=self.plot_title_fontsize)

        if is_plot_date:
            plot_func = ax.plot_date
        else:
            plot_func = ax.plot

        mf_color = color if marker_fill else 'none'

        plot_args = {
            'color': color,
            'ms': marker_size,
            'lw': linewidth,
            'label': label,
            'mfc': mf_color,
        }
        fmt = '{marker}{linestyle}'.format(marker=marker, linestyle=linestyle)
        return plot_func(x_vals, y_vals, fmt, **plot_args)
