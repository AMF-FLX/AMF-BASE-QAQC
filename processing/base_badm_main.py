import os
import json
from base_creator import BASECreator
from update_base_badm import UpdateBASEBADM


if __name__ == "__main__":
    # For ease of debugging
    base_attrs_fname = 'base_attrs.base'
    base_attr_files = [f for f in os.listdir() if f.endswith('.base')]
    if len(base_attr_files) > 0:
        print("Previous .base file is not retired.")
    else:
        b = BASECreator()
        if not b.init_status:
            print("Unsuccessful initialization of BASECreator. Terminating.")
        else:
            base_attrs = b.driver()
            # Write to intermediary json file
            with open(base_attrs_fname, 'w') as f:
                f.write(json.dumps(base_attrs, default=str))

    u = UpdateBASEBADM()
    with open(base_attrs_fname, 'r') as f:
        base_attrs = eval(f.read())
        for flux_id, v in base_attrs.items():
            for e in v:
                if e.get('ver').startswith('1-'):
                    print(f'{flux_id} needs DOI allocated')

    u.driver(base_attrs, post_base_only=True)
