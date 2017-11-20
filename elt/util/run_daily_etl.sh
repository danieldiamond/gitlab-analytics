#!/bin/bash

/opt/data-integration/kitchen.sh -file=/opt/etl/customers/update_gl_customers.kjb -level=Detailed

/opt/data-integration/kitchen.sh -file=/opt/etl/version/update_gl_version.kjb -level=Detailed

/opt/data-integration/kitchen.sh -file=/opt/etl/license/update_gl_license.kjb -level=Detailed
