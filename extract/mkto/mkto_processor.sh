#!/bin/bash

function job_exit() {
    if [[ ${JOB_EXIT:=0} = 0 ]]; then
        export JOB_EXIT=$1
    else
        export JOB_EXIT=$JOB_EXIT
    fi

    return $JOB_EXIT
}

function red() {
    tput setaf 1
    echo $1
    tput sgr0
}

job_exit 0 # success per default

python3 extract/mkto/mkto_export.py -s leads \
               --schema mkto \
               apply_schema

SCHEMA_LEAD_EXIT=$?

python3 extract/mkto/mkto_export.py -s activities \
               --schema mkto \
               apply_schema

SCHEMA_ACTIVITY_EXIT=$?

job_exit $SCHEMA_LEAD_EXIT
job_exit $SCHEMA_ACTIVITY_EXIT

if [[ $SCHEMA_LEAD_EXIT ]]; then
    echo "Schema updated, importing leads..."
    python3 extract/mkto/mkto_export.py -s leads \
               -t updated \
               --days 1 \
               --schema mkto \
               export

    job_exit $?
    echo "Leads import completed."
else
    red "Failed to update leads schema."
fi

if [[ $SCHEMA_ACTIVITY_EXIT ]]; then
    python3 extract/mkto/mkto_export.py -s activities \
                   -t created \
                   --days 1 \
                   --schema mkto \
                   export

    job_exit $?
    echo "Activities import completed."
else
    red "Failed to update activities schema."
fi

if [ ! $JOB_EXIT ]; then
    red "Marketo extraction failed."
fi

# HACK: exit $JOB_EXIT
exit 0
