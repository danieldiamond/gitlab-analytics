#!/usr/bin/env python3
import os
import psycopg2
import psycopg2.sql

from psycopg2.sql import SQL
from enum import Enum
from functools import partial


class Roles(str, Enum):
    CLOUDSQLSUPERUSER = "cloudsqlsuperuser"
    ANALYTICS = "analytics"
    READONLY = "readonly"
    LOOKER = "looker"


class User:
    @classmethod
    def create(self, _tuple):
        return self(*_tuple)

    def __init__(self, name, login, memberof=[]):
        self.name = name
        self.login = login
        self.memberof = set(memberof)

    def member_of(self, role):
        return role in self.memberof

    def __str__(self):
        return self.name


def list_users(cursor):
    """
    Returns the current roles
    """
    cursor.execute(
        """
    SELECT
        r.rolname AS name,
        r.rolcanlogin AS login,
        ARRAY(
        SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid
        ) AS memberof
    FROM pg_catalog.pg_roles r
    """
    )

    return map(User.create, cursor.fetchall())


def member_of(roles, user: User):
    """
    Returns true if the user is member of the role any of the role.
    """
    return any(map(user.member_of, roles))


def apply_role(cursor, user: User):
    """
    Apply the login to the user
    """
    result = "OK"
    role_identifier = psycopg2.sql.Identifier(user.name)

    try:
        cursor.execute(
            psycopg2.sql.SQL("REVOKE {} FROM {}").format(
                psycopg2.sql.Identifier(Roles.READONLY.value), role_identifier
            )
        )
        cursor.execute(
            psycopg2.sql.SQL("GRANT {} TO {}").format(
                psycopg2.sql.Identifier(Roles.ANALYTICS.value), role_identifier
            )
        )
    except psycopg2.Error as e:
        result = e.pgerror

    return (user, result)


def db_config():
    return {
        "host": os.getenv("PG_ADDRESS"),
        "port": os.getenv("PG_PORT"),
        "database": os.getenv("PG_DATABASE"),
        "user": os.getenv("PG_USERNAME"),
        "password": os.getenv("PG_PASSWORD"),
    }


def main():
    readonly_roles = partial(member_of, [Roles.READONLY])

    with psycopg2.connect(**db_config()) as db:
        with db.cursor() as cursor:
            users_in_readonly = filter(readonly_roles, list_users(cursor))
            applied_roles = map(partial(apply_role, cursor), users_in_readonly)

            for role, result in applied_roles:
                print("Changed role to analytics for {} → {}.".format(role, result))


if __name__ == "__main__":
    main()
