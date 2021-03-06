#! /usr/bin/env python
"""Runs either the cli or gui version of the program depending on AEVI_GUI environment variable."""
from dotenv import load_dotenv
from os import environ

load_dotenv()

GUI = environ.get("AEVI_GUI", "True").lower() in ("true", '1', 't')
LOCAL = environ.get("AEVI_LOCAL", "True").lower() in ("true", '1', 't')
if GUI:
    import gui
    gui.run(LOCAL)
else:
    import cli
    cli.run(LOCAL)
