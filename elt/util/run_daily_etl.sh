#!/bin/bash

kitchen.sh -file=elt/customers/update_gl_customers.kjb -level=Detailed

kitchen.sh -file=elt/version/update_gl_version.kjb -level=Detailed

kitchen.sh -file=elt/license/update_gl_license.kjb -level=Detailed
