#!/bin/bash

python3 elt/mkto/mkto_export.py -s leads \
               --schema mkto \
               apply_schema

if [ $? -eq 0 ]; then
    echo "Schema updated, importing leads..."
    python3 elt/mkto/mkto_export.py -s leads \
               -t updated \
               --days 1 \
               --schema mkto \
               export

    echo "Leads import completed."
else
    echo "Failed to update leads schema."
fi

python3 elt/mkto/mkto_export.py -s activities \
               --schema mkto \
               apply_schema

if [ $? -eq 0 ]; then
    python3 elt/mkto/mkto_export.py -s activities \
                   -t created \
                   --days 1 \
                   --schema mkto \
                   export

    echo "Activities import completed."
else
    echo "Failed to update activities schema."
fi
