#!/bin/bash

# Get's current UTC time
H=$(date -u +%H)
STARTTIME=2
ENDTIME=5

# Checks if between 2 and 5 AM UTC (9-11 PM CST)
# 10#$H returns the hour in base 10
if (( STARTTIME <= 10#$H && 10#$H < ENDTIME )); then
    kitchen.sh -file=elt/customers/update_gl_customers.kjb -level=Detailed

    kitchen.sh -file=elt/version/update_gl_version.kjb -level=Detailed

    kitchen.sh -file=elt/license/update_gl_license.kjb -level=Detailed
else
    echo The extract Pings ELT will not run: Only runs between the hours of $STARTTIME AM UTC and $ENDTIME AM UTC
fi
