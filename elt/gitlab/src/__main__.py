import argparse
from importer import Importer

def finished_download(importer):
  importer.download_csvs()

def main():
  print("1. Looking for GitLab CSC files...")
  Importer(finished_download)

main()
