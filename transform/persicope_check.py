#!/usr/bin/env python3
import os
from os.path import join, getsize, dirname
import re
import json

# Periscope project

dirname = os.path.dirname(os.path.abspath(__file__))
parentdirname = os.path.dirname(dirname)

paths_to_check = ["dashboards", "snippets", "views"]

periscope_table_dict = dict()

for path in paths_to_check:
    fullpath = f"{parentdirname}/periscope/{path}"
    for root, files in os.walk(fullpath):
        for file in files:
            if re.search("sql", file):
                full_filename = f"{root}/{file}"
                with open(full_filename, 'r') as f:
                    lines = f.readlines()
                    for line in lines:
                        matches = re.search("(?:from|join)\s+(?:analytics|analytics_staging|boneyard)\.([\_A-z0-9]*)", line.lower())
                        if matches is not None:
                            for match in matches.groups():
                                curr_list = periscope_table_dict.get(match, set())
                                curr_list.add(full_filename)
                                periscope_table_dict[match] = curr_list

print(len(periscope_table_dict.keys()))
                            
# periscope_table_dict = {
#     "table_queried": [
#         "fully qualified file name"
#     ]
# }


# analytics project

# dbt_files_changed = [dbt, files, changed]

# files_to_check = []

# for model in dbt_files_changed:
#     if model in periscope_table_dict.keys():
#         files_to_check.extend(periscope_table_dict.get(model, []))

# print(files_to_check)
