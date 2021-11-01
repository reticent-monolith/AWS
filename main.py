"""Runs either the cli or gui version of the program depending on AEVI_GUI environment variable."""
from dotenv import load_dotenv
import gui, cli
import os

load_dotenv()

GUI = os.environ.get("AEVI_GUI", "false")
if GUI == "false":
    cli.run()
elif GUI == "true":
    gui.run()
else:
    raise Exception("GUI environment variable should be set to either true or false.")