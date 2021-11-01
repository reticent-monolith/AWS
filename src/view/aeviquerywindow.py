from PySide6.QtWidgets import QGroupBox, QMainWindow, QTabWidget, QVBoxLayout

class AeviQueryWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Aevi Query")
        self.resize(800, 600)
        self.setCentralWidget(self._tabWidget())
        self._indexSelection()
        # self._filterSelection()
        # self._resultsArea()
        # self._cacheArea()
        self.show()

    def _tabWidget(self):
        tabWidget = QTabWidget()
        tabs = ["Query", "Results","Cache"]
        self.tabs = {}
        return tabWidget

    def _indexSelection(self):
        box = QGroupBox("Query Index")
        layout = QVBoxLayout()
        box.setLayout(layout)
        self.indexSelectionComponents = dict()
        labels = ["Index", "Index Value", "Limit", "Apply Filter"]

