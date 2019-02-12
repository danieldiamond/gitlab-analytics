import logging
from datetime import datetime

import pandas as pd
import singer
from elt.cli import DateWindow
from elt.db import DB

from netsuite.src.soap_api.netsuite_soap_client import NetsuiteClient
from netsuite.src.soap_api.transaction import Transaction


def extract_transaction_type(args):
    """
    Extract the Transaction {internalId, type} for a given date interval
      and update the stored Transactions with the fetched type.
    """
    transaction_types = {}
    window = DateWindow(args, formatter=datetime.date)
    start_time, end_time = window.formatted_range()

    logging.info("NetSuite Extract Transaction Types Started")
    logging.info("Date interval = [{},{}]".format(start_time, end_time))

    # Initialize the SOAP client and fetc the wsdl
    client = NetsuiteClient()

    # Login
    if client.login():
        entity = Transaction(client)

        # Fetch all the transaction types for the given date intervall
        response = entity.extract_type(start_time, end_time)

        while (
            response is not None
            and response.status.isSuccess
            and response.searchRowList is not None
        ):

            # records = response.recordList.record
            records = response.searchRowList.searchRow

            for record in records:
                internal_id = record["basic"]["internalId"][0]["searchValue"][
                    "internalId"
                ]
                trans_type = record["basic"]["type"][0]["searchValue"]

                transaction_types[internal_id] = trans_type

            response = entity.extract_type(start_time, end_time, response)

        # Create a dataframe of the transaction types
        transaction_types_clean: Dict[str, str] = {
            "id": list(transaction_types.keys()),
            "name": list(transaction_types.values()),
        }
        transaction_types_df = pd.DataFrame(data=transaction_types_clean)

        # Send the transaction types to Stitch
        table_name = "netsuite_transaction_types"
        primary_key = "id"
        table_schema = {"id": {"type": "string"}, "name": {"type": "string"}}

        singer.write_schema(
            stream_name=table_name,
            schema={"properties": table_schema},
            key_properties=[primary_key],
        )
        singer.write_records(
            stream_name=table_name,
            records=transaction_types_df.to_dict(orient="records"),
        )

        logging.info("Extraction of Transaction Types completed Successfully")
    else:
        logging.info("Could NOT login to NetSuite - Script Failed")
