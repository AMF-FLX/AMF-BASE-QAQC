import os

__author__ = 'Danielle Christianson'
__email__ = 'dchristianson@lbl.gov'

"""Common functions that can be shared between files"""


class PathUtil:

    def create_valid_path(self, path, sub_dir):
        """Create subdirectory if does not exist and returns path
        """
        sub_dir_path = os.path.join(path, sub_dir)
        if not os.path.exists(sub_dir_path):
            os.makedirs(sub_dir_path)
        return sub_dir_path

    def create_dir_for_run(self, site_id, process_id, base_dir):
        """Create subdirectories if needed and returns the directory to plot

        :param site_id: Site ID of the file processed
        :type site_id: str.
        :param process_id: Process ID
        :type process_id: str.

        :rtype: str.
        :return: full path to plot directory
        """
        site_id_dir = self.create_valid_path(base_dir, site_id)
        return self.create_valid_path(site_id_dir, process_id)

    def get_base_ver_from_hist_fname(self, fname):
        """Parses BASE version from historical filenames.
        Filenames have following format:
        'AMF_<SITE_ID>_BASE_<RESOLUTION>_<VER>_<TYPE>.csv'
        """
        fname_elements = fname.split('_')
        if len(fname_elements) != 6:
            return None
        try:
            return fname_elements[-2]
        except Exception:
            return None
