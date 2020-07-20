import os

import pytest
from dags.kubernetes_helpers import get_toleration, get_affinity


@pytest.fixture(autouse=True)
def run_around_tests():
    if "NAMESPACE" in os.environ:
        current_namespace = os.environ["NAMESPACE"]
    else:
        current_namespace = ""
    yield
    os.environ["NAMESPACE"] = current_namespace


def set_testing_env():
    os.environ["NAMESPACE"] = "testing"


def get_affinity_name_from_value(affinity):
    return affinity["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][
        "nodeSelectorTerms"
    ][0]["matchExpressions"][0]["key"]


def test_affinity():
    scd_affinity = get_affinity(True)
    prod_affinity = get_affinity(False)

    set_testing_env()
    test_affinity = get_affinity(False)

    assert get_affinity_name_from_value(scd_affinity) == "pgp"
    assert get_affinity_name_from_value(prod_affinity) == "production"
    assert get_affinity_name_from_value(test_affinity) == "test"


def get_toleration_name_from_value(toleration):
    return toleration[0]["key"]


def test_toleration():
    scd_toleration = get_toleration(True)
    prod_toleration = get_toleration(False)

    set_testing_env()
    test_toleration = get_toleration(False)

    assert get_toleration_name_from_value(scd_toleration) == "scd"
    assert get_toleration_name_from_value(test_toleration) == "test"
    assert get_toleration_name_from_value(prod_toleration) == "production"
