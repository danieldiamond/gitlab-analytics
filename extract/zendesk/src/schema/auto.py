from urllib.parse import urljoin
from elt.schema import Schema
from elt.schema.serializers import MeltanoSerializer
from config import manifest_file_path

PG_SCHEMA = "zendesk"
PRIMARY_KEY = "id"
INCREMENTAL_ENTITIES = ("tickets", "organizations", "users")

def describe_schema(args) -> Schema:
    serializer = MeltanoSerializer(args.schema or PG_SCHEMA)

    with open(manifest_file_path(args), 'r') as reader:
        serializer.load(reader)

    return serializer.schema


def entity_url(endpoint, entity_name):
    base_url = endpoint
    path = "{}.json".format(entity_name.lower())

    if is_entity_incremental(entity_name):
        base_url = urljoin(base_url, "incremental/")

    return urljoin(base_url, path)


def is_entity_incremental(entity_name):
    return entity_name in INCREMENTAL_ENTITIES
