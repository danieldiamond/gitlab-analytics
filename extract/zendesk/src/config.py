import os

def manifest_file_path(args):
    return os.path.join(os.path.dirname(__file__), "..", "zendesk_manifest.yaml")
