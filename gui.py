from view.aeviquerywindow import AeviQueryWindow
from aevirepo import AeviRepo
from PySide6.QtWidgets import QApplication
import sys

def run():
    app = QApplication([])
    view = AeviQueryWindow()
    model = AeviRepo()
    sys.exit(app.exec())
