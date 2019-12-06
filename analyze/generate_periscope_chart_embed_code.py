from os import environ as env
from urllib import parse as parse
import ast
import base64
import csv
import hashlib
import hmac
import json
import logging

API_KEY = env.get("PERISCOPE_API_KEY").encode("UTF-8")
input_csv = "chart_slide_deck.csv"
output_csv = "chart_links.csv"


def csv_to_chart_list(input_csv):
    logging.info("Opening csv...")
    with open(input_csv, "r") as list_of_charts_to_embed:
        # Change each fieldname to the appropriate field name.
        reader = csv.DictReader(
            list_of_charts_to_embed,
            fieldnames=("chart_name", "dashboard_id", "widget_id"),
        )
        header = reader.fieldnames

        skip_header = next(reader)
        # Parse the CSV into JSON
        charts = [row for row in reader]
        # Save the JSON
        chart_dict_list = []

        for chart in charts:
            chart_dict = {}
            chart_dict["chart_name"] = chart["chart_name"]
            chart_dict["chart"] = int(chart["widget_id"])
            chart_dict["dashboard"] = int(chart["dashboard_id"])
            chart_dict["embed"] = "v2"
            chart_dict["border"] = "off"
            chart_dict_list.append(chart_dict)

    logging.info("Chart list captured...")
    return chart_dict_list


def generate_periscope_embed_url(chart_dict_list, output_csv):

    chart_links_list = []

    for chart in chart_dict_list:

        chart_link_dict = {}
        chart_link_dict["chart_name"] = chart["chart_name"]

        dict_input = chart
        dict_input.pop("chart_name")

        json_data = json.dumps(dict_input)

        encoded_json = parse.urlencode(({"data": json_data})).split("=")[1]

        url = "/api/embedded_dashboard?data=" + encoded_json
        sig = hmac.new(API_KEY, url.encode(), hashlib.sha256).hexdigest()

        chart_link = "https://www.periscopedata.com" + url + "&signature=" + sig

        chart_link_dict["chart_link"] = chart_link
        chart_links_list.append(chart_link_dict)

    logging.info("Writing results...")
    with open(output_csv, "w") as embed_urls:
        dict_writer = csv.DictWriter(
            embed_urls, fieldnames=["chart_name", "chart_link"]
        )
        dict_writer.writerows(chart_links_list)


if __name__ == "__main__":
    logging.basicConfig(level=20)
    chart_dict_list = csv_to_chart_list(input_csv)
    generate_periscope_embed_url(chart_dict_list, output_csv)
    logging.info("Script successful.")
