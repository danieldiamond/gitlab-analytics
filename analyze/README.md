## Generating Periscope Embed Links

Edit the `chart_links.csv` file to include the chart name, the dashboard ID, and the widget ID.
These all come from the chart's URL.
Follow the example in the csv here.
Set the API key in your env vars. 
Run `generate_periscope_chart_embed_code.py`.
The embed URL will be output in the `chart_slide_deck.csv` file.
**You should not commit changes to either of these two csv files.**
