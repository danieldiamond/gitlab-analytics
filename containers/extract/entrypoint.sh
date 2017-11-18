#!/bin/bash
set -e

# Render kettle template
for f in $(find $KETTLE_TEMPLATES -type f); do
    envsubst < "$f" > "$KETTLE_HOME/.kettle/$(basename $f)"
done

# Render carte template
for f in $(find $CART_TEMPLATES -type f); do
    envsubst < "$f" > "$PDI_PATH/carte/$(basename $f)"
done

# Link the right template and replace the variables in it
if [ ! -e "$KETTLE_HOME/carte/carte-config.xml" ]; then
    if [ "$CARTE_INCLUDE_MASTERS" = "Y" ]; then
        ln -sf "$KETTLE_HOME/carte/carte-slave.xml" "$PDI_PATH/carte/carte-config.xml"
    else
        ln -sf "$KETTLE_HOME/carte/carte-master.xml" "$PDI_PATH/carte/carte-config.xml"
    fi
fi

# Run any custom scripts
for f in $(find /etc/entrypoint/conf.d -type f); do
    source "$f"
done

exec "$@"