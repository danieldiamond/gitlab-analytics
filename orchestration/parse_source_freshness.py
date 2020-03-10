import json
import os

import slack

if __name__ == "__main__":
    file_name = "target/sources.json"
    sources_state = {}
    with open(file_name) as json_file:
        sources_state = json.load(json_file)
    sources_information_generated_at = sources_state["meta"]["generated_at"]
    sources_to_alert_on = []
    for attribute, value in sources_state["sources"].items():
        individual_source_state = value["state"]
        if individual_source_state != "pass":
            sources_to_alert_on.append(
                f"{attribute}: {individual_source_state} -- last loaded at: {value['max_loaded_at']}"
            )
    alert_message = "\n".join(sources_to_alert_on)
    slack_client = slack.WebClient(token=os.environ["SLACK_API_TOKEN"])
    if alert_message:
        alert_message = "THE FOLLOWING SOURCES ARE STALE:\n" + alert_message
        slack_client.chat_postMessage(
            channel="#analytics-pipelines", text=alert_message, username="Airflow",
        )
