from elt.db import DB


def test_connect():
    db_conn = DB.default.open()
    assert db_conn
