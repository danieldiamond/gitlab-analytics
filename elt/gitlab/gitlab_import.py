#!/usr/bin/python3
import urllib.request, json
from importer import Importer

if __name__ == '__main__':
  importer = Importer()
  print(importer.download_csv())