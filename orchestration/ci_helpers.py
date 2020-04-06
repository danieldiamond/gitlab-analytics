#!/usr/bin/env python3

import pandas as pd
import sys

from atexit import register
from fire import Fire
from functools import partial
from logging import info, error, basicConfig
from os import environ as env, remove
from subprocess import run, PIPE, Popen, _active, _cleanup
from typing import Tuple. 


@register
def exit_cleanup():
    """
    Make sure that when the script exits it kills all subprocesses.
    """

    for proc in _active:
        proc.kill()
        proc.communicate()
    _cleanup()
    run("kill -9 $(pgrep cloud_sql_proxy)", shell=True, stderr=PIPE)
    if not _active:
        info("Processes cleaned up.")
    else:
        error("Sub processes could not be cleaned up.")


def auth_gcloud(bash_partial) -> None:
    """
    Authenticate the gcloud service account.
    """

    try:
        with open("gcp_credentials.json", "w") as file:
            file.write(env["GCP_SERVICE_CREDS"])

        bash_partial(
            "gcloud auth activate-service-account --key-file=gcp_credentials.json"
        )
    except IOError:
        error("Could not store GCP creds as a json file.")
        sys.exit(1)
    except:
        error("Could not authenticate service account.")
        sys.exit(1)

    info("Account successfully authenticated.")


def find_sql_instance(gcp_instance_ref_slug) -> str:
    """
    Find the gcp instance with the same ref slug, don't worry about the ID.
    """

    if env["CI_COMMIT_REF_NAME"] == "master":
        return env["GCP_PRODUCTION_INSTANCE_NAME"]

    instance_list_raw = run(
        "gcloud sql instances list --project {}".format(env["GCP_PROJECT"]),
        stdout=PIPE,
        shell=True,
        check=True,
    ).stdout
    try:
        [instance_name] = [
            instance_name
            for instance_row in instance_list_raw.decode("utf-8").split(" ")
            for instance_name in instance_row.split("\n")
            if gcp_instance_ref_slug in instance_name
        ] or [None]
    except:
        info("No instance found.")
    return instance_name


def set_sql_instance() -> Tuple[str, str, str]:
    """
    Create a sql instance using the slug and ci_job_id.

    Max length for instance name is 95, take away the length of the
    job_id and the two hyphens and that leaves 83 characters for the name.
    """

    slug_length = 83 - len(env["CI_PROJECT_NAME"])
    gcp_instance_ref_slug = "{}-{}".format(
        env["CI_PROJECT_NAME"], env["CI_COMMIT_REF_SLUG"][0:slug_length]
    )
    job_gcp_instance_name = "{}-{}".format(gcp_instance_ref_slug, env["CI_JOB_ID"][:8])

    instance_name = find_sql_instance(gcp_instance_ref_slug)
    info("Found instance with name: {}".format(instance_name))
    return instance_name, job_gcp_instance_name, gcp_instance_ref_slug


def async_run(command: str, instance: str) -> None:
    """
    Run gcloud commands using the async flag and waiting for them to finish/fail.
    """

    info("Running command using async: {}".format(command))
    run(command + " --async", shell=True, check=True, stdout=PIPE)
    # Returns bytes with newlines, assume the first operation id is correct
    operation = (
        run(
            "gcloud sql operations list --instance='{}' --filter='status!=DONE' --format='value(name)'".format(
                instance
            ),
            shell=True,
            check=True,
            stdout=PIPE,
        )
        .stdout.decode("utf-8")
        .rstrip()
    )
    info("Pending Operation: {}".format(operation))
    info("Waiting for operation to finish/fail...")
    while True:
        status = (
            run(
                'sleep 10; gcloud sql operations describe "{}" --format="value(status)"'.format(
                    operation
                ),
                shell=True,
                check=True,
                stdout=PIPE,
            )
            .stdout.decode("utf-8")
            .rstrip()
        )
        if status == "DONE":
            info(status)
            break
        else:
            info(status)


def use_cloudsqlproxy(command: str) -> None:
    """
    Execute a command while running the cloud sql proxy in the background.
    """

    # Get the instance name and start the proxy
    instance_name, *_ = set_sql_instance()
    sql_proxy_command = "cloud_sql_proxy -instances={}:{}:{}=tcp:5432 -credential_file=gcp_credentials.json -verbose=False"
    sql_proxy = Popen(
        sql_proxy_command.format(env["GCP_PROJECT"], env["GCP_REGION"], instance_name),
        shell=True,
    )

    info("Proxy is running.")
    run(command, shell=True, check=True)

    return


def delete_review_cloudsql() -> None:
    """
    Delete the cloudsql instance unless it is master (production).
    """

    # Set the instance name and make sure it exists and isn't master
    instance_name, *_ = set_sql_instance()
    if not instance_name:
        error("This instance does not exist. Call manage_instances to create one.")
        sys.exit(1)
    if instance_name == env["GCP_PRODUCTION_INSTANCE_NAME"]:
        info("The branch name cannot match the production EDW instance name.")
        sys.exit(1)

    delete_instance_command = 'gcloud sql instances delete -q --project "{}" "{}"'
    run(
        delete_instance_command.format(env["GCP_PROJECT"], instance_name),
        shell=True,
        check=True,
    )
    info("Instance Deleted.")


def manage_review_cloudsql() -> None:
    """
    Detemine whether to create, do nothing to, or clean up cloud instances.
    """

    instance_name, job_gcp_instance_name, gcp_instance_ref_slug = set_sql_instance()

    # Check if script should force delete related instances
    if instance_name and env.get("FORCE") == "true":
        info("Cleaning up old sql instances.")
        list_instances_command = "gcloud sql instances list --project {} --filter {}"
        instance_list_raw = run(
            list_instances_command.format(env["GCP_PROJECT"], gcp_instance_ref_slug),
            shell=True,
            check=True,
            stdout=PIPE,
        ).stdout
        instance_list = [
            instance_name
            for instance_row in instance_list_raw.decode("utf-8").split(" ")
            for instance_name in instance_row.split("\n")
            if env["CI_PROJECT_NAME"] in instance_name
        ]
        delete_instance_command = "gcloud sql instances delete -q --project {} {}"
        for instance in instance_list:
            info("Deleting instance: {}".format(instance))
            run(
                delete_instance_command.format(env["GCP_PROJECT"], instance),
                shell=True,
                check=True,
            )
    # If not forcing deletion and there is an instance, echo the name
    elif instance_name:
        info("Instance is available at: {}".format(instance_name))
        return

    # If no instance existed or force deleted, create a new instance
    info("Cloning new instance {}".format(job_gcp_instance_name))
    clone_instance_command = 'gcloud sql instances clone -q --project "{}" "{}" "{}"'
    async_run(
        clone_instance_command.format(
            env["GCP_PROJECT"],
            env["GCP_PRODUCTION_INSTANCE_NAME"],
            job_gcp_instance_name,
        ),
        job_gcp_instance_name,
    )
    return


def refresh_dev_cloudsql():
    """
    Update the dev instance.
    """

    info("Restoring the dev instance from the latest successful prod backup.")

    # Dump  a list of recent backups into a txt file
    run(
        "gcloud config set project {}".format(env["GCP_PROJECT"]),
        shell=True,
        check=True,
    )

    backup_list_filename = "backup_list.txt"
    run(
        "gcloud sql backups list --instance {} > {}".format(
            env["GCP_PRODUCTION_INSTANCE_NAME"], backup_list_filename
        ),
        shell=True,
        check=True,
        stdout=PIPE,
    ).stdout

    # Get the most recent successful backup ID
    backup_df = pd.read_table("backup_list.txt", delim_whitespace=True).query(
        'STATUS == "SUCCESSFUL"'
    )
    backup_df["WINDOW_START_TIME"] = pd.to_datetime(backup_df["WINDOW_START_TIME"])
    [backup_id] = backup_df.query("WINDOW_START_TIME == @newest_backup")["ID"]
    remove(backup_list_filename)

    # Trigger the dev instance refresh
    instance_refresh_command = 'gcloud sql backups restore {} -q --restore-instance="{}" --backup-instance="{}"'
    run(
        instance_refresh_command.format(
            backup_id, env["GCP_DEV_INSTANCE_NAME"], env["GCP_PRODUCTION_INSTANCE_NAME"]
        ),
        shell=True,
        check=True,
    )


if __name__ == "__main__":
    # Do some setup before running any Fire functions
    basicConfig(stream=sys.stdout, level=20)
    bash = partial(run, shell=True, check=True)
    auth_gcloud(bash)
    Fire(
        {
            "get_sql_instance": set_sql_instance,
            "use_proxy": use_cloudsqlproxy,
            "manage_instances": manage_review_cloudsql,
            "delete_instance": delete_review_cloudsql,
            "refresh_dev_instance": refresh_dev_cloudsql,
        }
    )
