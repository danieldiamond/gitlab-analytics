import os
import argparse
import getpass


class Password:
    DEFAULT = 'PG_PASSWORD environment variable.'

    def __init__(self, value):
        if value == self.DEFAULT:
            value = os.getenv('PG_PASSWORD', None)
        if not value:
            value = getpass.getpass()
        self.value = value

    def __str__(self):
        return self.value


def parser_db_conn(parser, required=True):
    current_user = os.getenv('USER')

    parser.add_argument('-S', '--schema', required=required,
                        help="Database schema to use.")

    parser.add_argument('-T', '--table', dest='table_name',
                        help="Table to import the data to.")

    parser.add_argument('-d', '--db', dest='database', required=required,
                        default=os.getenv('PG_DATABASE', current_user),
                        help="Database to import the data to.")

    parser.add_argument('-H', '--host', default=os.getenv('PG_ADDRESS', 'localhost'),
                        help="Database host address.")

    parser.add_argument('-p', '--port', default=os.getenv('PG_PORT', 5432),
                        help="Database port.")

    parser.add_argument('-u', '--user', default=os.getenv('PG_USERNAME', current_user),
                        help="Specifies the user to connect to the database with.")

    parser.add_argument('-W', '--password', type=Password, help='Specify password',
                        default=Password.DEFAULT)
