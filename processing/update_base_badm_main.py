import os
from update_base_badm import UpdateBASEBADM


if __name__ == "__main__":
    base_attrs_fname = 'base_attrs.base'
    base_attr_files = [f for f in os.listdir() if f.endswith('.base')]
    if not base_attr_files:
        print('No .base files available')
    elif len(base_attr_files) > 1:
        print('More than one .base file available.')
    else:
        u = UpdateBASEBADM()
        if not u.init_status:
            print('Unsuccessful initialization of update BASEBADM. '
                  'Terminating.')
        else:
            with open(base_attrs_fname, 'r') as f:
                base_attrs = eval(f.read())
                u.driver(base_attrs)
                os.rename(base_attrs_fname, base_attrs_fname + '.processed')
