#!/usr/bin/env python3
import os
from os.path import join, getsize, dirname
import re

# Assumes the periscope directory was checked out at the parent repository

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
                with open(full_filename, "r") as f:
                    lines = f.readlines()
                    all_lines = " ".join(lines)
                    # Removes new lines following "from" and "join" b/c people don't follow style guide
                    clean_lines = re.sub(
                        r"(from|join)([\s\\r\\n]*)", r"\1 ", all_lines.lower()
                    )
                    new_lines = clean_lines.split("\n")

                    for line in new_lines:
                        # Find from and join references. Only match group is table name(s)
                        matches = re.search(
                            r"(?:from|join)\s+(?:analytics|analytics_staging|boneyard)\.([\_A-z0-9]*)",
                            line.lower(),
                        )
                        if matches is not None:
                            for match in matches.groups():
                                curr_list = periscope_table_dict.get(match, [])
                                # Strip prefixes
                                simplified_name = re.sub(
                                    ".*\/analytics\/periscope\/", "", full_filename
                                )
                                curr_list.append(simplified_name)
                                periscope_table_dict[match] = list(set(curr_list))

with open("comparison.txt", "w+") as f:
    f.write("Check these!\r\n\r\n")

# Assumes git diff was run to output the sql files that changed
with open("diff.txt", "r") as f:
    lines = f.readlines()
    for line in lines:
        match = periscope_table_dict.get(line.strip(), [])
        if len(match) > 0:
            with open("comparison.txt", "a") as comp:
                write_string = (
                    f"dbt model: {line}Periscope references: {str(match)} \r\n\r\n"
                )
                comp.write(write_string)
