from os import environ as env


def get_affinity_with_key_value(key, values):
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {"key": key, "operator": "In", "values": values}
                        ]
                    }
                ]
            }
        }
    }


def get_toleration_with_value(value):
    return [
        {"key": value, "operator": "Equal", "value": "true", "effect": "NoSchedule"}
    ]


scd_affinity = get_affinity_with_key_value("pgp", ["scd"])

scd_tolerations = get_toleration_with_value("scd")

test_affinity = get_affinity_with_key_value("test", ["true"])

test_tolerations = get_toleration_with_value("test")

production_affinity = get_affinity_with_key_value("production", ["true"])

production_tolerations = get_toleration_with_value("production")


def is_local_test():
    return "NAMESPACE" in env and env["NAMESPACE"] == "testing"


def get_affinity(is_scd):
    if is_local_test():
        return test_affinity
    if is_scd:
        return scd_affinity
    return production_affinity


def get_toleration(is_scd):
    if is_local_test():
        return test_tolerations
    if is_scd:
        return scd_tolerations
    return production_tolerations
