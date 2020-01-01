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
    for root, dirs, files in os.walk(fullpath):
        for file in files:
            if re.search("sql", file):
                full_filename = f"{root}/{file}"
                with open(full_filename, 'r') as f:
                    lines = f.readlines()
                    for line in lines:
                        matches = re.search("(?:from|join)\s+(?:analytics|analytics_staging|boneyard)\.([\_A-z0-9]*)", line.lower())
                        if matches is not None:
                            for match in matches.groups():
                                curr_list = periscope_table_dict.get(match, [])
                                simplified_name = re.sub(".*\/analytics\/periscope\/", "", full_filename)
                                curr_list.append(simplified_name)
                                periscope_table_dict[match] = list(set(curr_list))


with open('comparison.txt', 'w+') as f:
    f.write("dbt models to check\r\n")

with open('diff.txt', 'r') as f:
    lines = f.readlines()
    for line in lines:
        match = periscope_table_dict.get(line.strip(), [])
        if len(match) > 0:
            with open('comparison.txt', 'a') as comp:
                write_string = f"dbt model: {line}. Periscope references: {str(match)} \r\n"
                comp.write(write_string)
          