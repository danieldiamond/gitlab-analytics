import io
import json
import yaml
from collections import OrderedDict

from .mkto_leads import describe_leads
#from .mkto_activities import describe_activities

data_types_map = {
  "date": "DATE",
  "string": "VARCHAR",
  "phone": "VARCHAR",
  "text": "VARCHAR",
  "percent": "REAL",
  "integer": "INTEGER",
  "boolean": "BOOLEAN",
  "lead_function": "VARCHAR",
  "email": "VARCHAR",
  "datetime": "TIMESTAMP",
  "currency": "VARCHAR",
  "reference": "VARCHAR",
  "url": "VARCHAR",
  "float": "REAL"
}

schema_overrides = {
    'leads': {}
}

schema_func_map = {
    'leads': describe_leads
    #'activities': describe_activities,
}

schema_primary_key = ['id']

'''
schema public:
  owner: postgres
  table t1:
    columns:
    - c1:
        not_null: true
        type: integer
    - c2:
        type: smallint
    - c3:
        default: 'false'
        type: boolean
    - c4:
        type: text
'''
def schema_export(args):
    output_file = args.output_file or 'schema.yaml'

    schema = schema_func_map[args.source]()
    # json.dump(schema, io.open(output_file, 'w'),
    #      indent=2)

    output_schema = table(args.source, schema)
    yaml.dump(output_schema, io.open(output_file, 'w'))


def table(source, schema):
    fields = schema['result']

    table_name = "mkto_%s" % source
    table_pkey = "%s_pkey" % table_name
    table_def = "table %s" % table_name
    columns = list(filter(None,
                          (column(source, field) for field in fields))
              )

    columns.sort(key=lambda t: next(iter(t.keys())))

    return {
        'schema generated': {
            'owner': 'gitlab',
            table_def: {
                'primary_key': {
                    table_pkey: { 'columns': schema_primary_key }
                },
                'columns': columns
            }
        }
    }


'''
{
    "id": 2,
    "displayName": "Company Name",
    "dataType": "string",
    "length": 255,
    "rest": {
        "name": "company",
        "readOnly": false
    },
    "soap": {
        "name": "Company",
        "readOnly": false
    }
},
'''
def column(source, field):
    if not 'rest' in field:
        print("REST field not found for '%s'" % field['id'])
        return None

    rest_field = field['rest']
    column_name = rest_field['name']
    column_def = column_name.lower()
    dt_type = data_type(source, column_name, field['dataType'])

    print("%s -> %s as %s" % (column_name, column_def, dt_type))

    column = {
        column_def: {
            'type': dt_type
        }
    }

    # Primary key must be non-null
    if column_def in schema_primary_key:
        column[column_def]['not_null'] = True

    return column


def data_type(source, field_name, src_type):
    overrides = schema_overrides.get(source, {})
    return overrides.get(field_name, data_types_map[src_type])
