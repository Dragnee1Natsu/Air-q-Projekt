import subprocess

class LibraryImporter:
    def __init__(self):
        # Eine Liste aller benötigten Bibliotheken (Module)
        self.libraries = [
            "numpy",
            "pandas",
            "matplotlib",
            "seaborn",
            "mariadb",
            "mysql-connector-python",
            "pycryptodome",
            "http.client",
            "json",
            "sqlalchemy"
        ]

    def install_libraries(self):
        """
        Installiert die benötigten Bibliotheken (Module), falls noch nicht vorhanden.
        """
        for lib in self.libraries:
            try:
                __import__(lib)
            except ImportError:
                print(f"{lib} wird installiert...")
                subprocess.check_call(["pip", "install", lib])
        
    def import_libraries(self):
        """
        Importiert die benötigten Bibliotheken (Module) in die Entwicklungsumgebung.
        """
        # Math und Pandas
        import numpy as np
        import pandas as pd 

        # Visualization Stuff
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import seaborn as sn

        # DB stuff
        import mariadb
        import sys
        import mysql.connector

        # Encryption Stuff
        import base64
        from Crypto.Cipher import AES
        import http.client
        from Crypto import Random

        # Other
        import json
        import time
        import datetime
        from datetime import datetime, timedelta

        import pandas as pd
        from sqlalchemy import create_engine

        print("Alle benötigten Bibliotheken wurden erfolgreich importiert.")
    

