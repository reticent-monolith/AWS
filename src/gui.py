from view.aeviquerywindow import AeviQueryWindow
from aevirepo import AeviRepo
from PySide6.QtWidgets import QApplication
import sys

def run(local):
    app = QApplication([])
    view = AeviQueryWindow()
    model = AeviRepo(local)
    sys.exit(app.exec())
