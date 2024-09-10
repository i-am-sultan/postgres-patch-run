import sys
import re
import configparser
from threading import Thread
import psycopg2
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *

CONFIG_FILE = 'config.ini'

class DatabaseThread(QThread):
    databases_fetched = pyqtSignal(list)
    error_occurred = pyqtSignal(str)
    query_executed = pyqtSignal(str)

    def __init__(self, credentials, query=None, databases=None):
        super().__init__()
        self.credentials = credentials
        self.query = query
        self.databases = databases

    def run(self):
        if self.query:
            self.execute_query()
        else:
            self.fetch_databases()

    def fetch_databases(self):
        try:
            conn = psycopg2.connect(
                dbname='postgres',
                host=self.credentials['host'],
                port=self.credentials['port'],
                user=self.credentials['user'],
                password=self.credentials['password']
            )
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datname NOT LIKE '%azure%' AND datname <> 'template0' AND datname <> 'template1' AND datname<>'postgres' ORDER BY datname desc")
            databases = cursor.fetchall()
            conn.commit()
            conn.close()
            self.databases_fetched.emit([db[0] for db in databases])
        except Exception as e:
            self.error_occurred.emit(f"Error fetching databases: {str(e)}")

    def execute_query(self):
        results = []
        for db in self.databases:
            try:
                conn = psycopg2.connect(
                    dbname=db,
                    host=self.credentials['host'],
                    port=self.credentials['port'],
                    user=self.credentials['user'],
                    password=self.credentials['password']
                )
                cursor = conn.cursor()
                cursor.execute(self.query)
                conn.commit()
                conn.close()
                results.append(f"Patch successfully applied to database {db}.")
            except Exception as e:
                results.append(f"Error from database {db}: {str(e)}")
        self.query_executed.emit("\n".join(results))

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle('Ginesys-PG-Patch-Executor')
        self.setGeometry(300, 200, 1000, 500)

        main_layout = QVBoxLayout()

        # Layout for credentials
        grid_layout = QGridLayout()

        self.pgHostLabel = QLabel('Host:')
        self.pgHostInput = QLineEdit()
        self.pgHostInput.setPlaceholderText('Enter Host')
        grid_layout.addWidget(self.pgHostLabel, 0, 0)
        grid_layout.addWidget(self.pgHostInput, 0, 1)

        self.pgPortLabel = QLabel('Port:')
        self.pgPortInput = QLineEdit()
        self.pgPortInput.setPlaceholderText('Enter Port')
        grid_layout.addWidget(self.pgPortLabel, 0, 2)
        grid_layout.addWidget(self.pgPortInput, 0, 3)

        self.pgUserLabel = QLabel('User:')
        self.pgUserInput = QLineEdit()
        self.pgUserInput.setPlaceholderText('Enter User')
        grid_layout.addWidget(self.pgUserLabel, 1, 0)
        grid_layout.addWidget(self.pgUserInput, 1, 1)

        self.pgPasswordLabel = QLabel('Password:')
        self.pgPasswordInput = QLineEdit()
        self.pgPasswordInput.setPlaceholderText('Enter Password')
        self.pgPasswordInput.setEchoMode(QLineEdit.Password)
        grid_layout.addWidget(self.pgPasswordLabel, 1, 2)
        grid_layout.addWidget(self.pgPasswordInput, 1, 3)

        main_layout.addLayout(grid_layout)

        # Create a splitter for the bottom part
        bottom_splitter = QSplitter(Qt.Horizontal)

        # First part (1/3): Database list and fetch button
        left_widget = QWidget()
        left_layout = QVBoxLayout()

        self.db_list_widget = QListWidget()
        self.db_list_widget.setSelectionMode(QAbstractItemView.MultiSelection)

        self.fetch_dbname_button = QPushButton('Fetch Databases')
        self.fetch_dbname_button.clicked.connect(self.fetchDatabases)

        # left_layout.addWidget(QLabel("Select Databases:"))
        left_layout.addWidget(self.db_list_widget)
        left_layout.addWidget(self.fetch_dbname_button)

        left_widget.setLayout(left_layout)

        # Second part (2/3): Split into two panels - query input and log window
        right_splitter = QSplitter(Qt.Vertical)

        # Top (query input 2/3 of the right part)
        query_widget = QWidget()
        query_layout = QVBoxLayout()

        self.queryInput = QTextEdit()
        self.queryInput.setPlaceholderText('Enter patch here...')

        self.run_query_button = QPushButton('Execute')
        self.run_query_button.clicked.connect(self.runQuery)

        query_layout.addWidget(self.queryInput)
        query_layout.addWidget(self.run_query_button)

        query_widget.setLayout(query_layout)

        # Bottom (log 1/3 of the right part)
        log_widget = QWidget()
        log_layout = QVBoxLayout()

        self.logWindow = QTextEdit()
        self.logWindow.setReadOnly(True)

        log_layout.addWidget(self.logWindow)
        log_widget.setLayout(log_layout)

        # Add the two sections to the right splitter
        right_splitter.addWidget(query_widget)
        right_splitter.addWidget(log_widget)
        right_splitter.setStretchFactor(0, 2)
        right_splitter.setStretchFactor(1, 1)

        # Add the left and right sections to the bottom splitter
        bottom_splitter.addWidget(left_widget)
        bottom_splitter.addWidget(right_splitter)
        bottom_splitter.setStretchFactor(0, 1)
        bottom_splitter.setStretchFactor(1, 3)

        main_layout.addWidget(bottom_splitter)

        self.setLayout(main_layout)
        self.loadcredentials()
        self.apply_styles()
        self.show()

    def loadcredentials(self):
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        if 'PostgreSQL' in config:
            self.pgHostInput.setText(config['PostgreSQL'].get('host', ''))
            self.pgPortInput.setText(config['PostgreSQL'].get('port', ''))
            self.pgUserInput.setText(config['PostgreSQL'].get('user', ''))
            self.pgPasswordInput.setText(config['PostgreSQL'].get('password', ''))
        else:
            self.logWindow.append("No existing configuration found. Please enter your PostgreSQL credentials.")

    def savecredentials(self):
        config = configparser.ConfigParser()
        config['PostgreSQL'] = {
            'host': self.pgHostInput.text(),
            'port': self.pgPortInput.text(),
            'user': self.pgUserInput.text(),
            'password': self.pgPasswordInput.text()
        }
        with open(CONFIG_FILE, 'w') as configfile:
            config.write(configfile)

    def fetchDatabases(self):
        self.savecredentials()
        self.logWindow.append("Fetching databases...")
        credentials = {
            'host': self.pgHostInput.text(),
            'port': self.pgPortInput.text(),
            'user': self.pgUserInput.text(),
            'password': self.pgPasswordInput.text()
        }
        self.db_thread = DatabaseThread(credentials)
        self.db_thread.databases_fetched.connect(self.updateDatabaseList)
        self.db_thread.error_occurred.connect(self.displayError)
        self.db_thread.start()

    def runQuery(self):
        self.savecredentials()
        selected_db = [item.text() for item in self.db_list_widget.selectedItems()]
        if not selected_db:
            QMessageBox.critical(self, "Warning!", 'No database has been selected')
            return
        query = self.queryInput.toPlainText()
        if not query:
            QMessageBox.critical(self, "Warning!", 'Please enter an SQL Query.')
            return

        self.logWindow.append("Running query...")
        credentials = {
            'host': self.pgHostInput.text(),
            'port': self.pgPortInput.text(),
            'user': self.pgUserInput.text(),
            'password': self.pgPasswordInput.text()
        }
        self.query_thread = DatabaseThread(credentials, query, selected_db)
        self.query_thread.query_executed.connect(self.displayResults)
        self.query_thread.error_occurred.connect(self.displayError)
        self.query_thread.start()

    def updateDatabaseList(self, databases):
        self.db_list_widget.clear()
        for db in databases:
            self.db_list_widget.addItem(db)
        self.logWindow.append("Databases fetched successfully.")

    def displayResults(self, results):
        self.logWindow.append(results)

    def displayError(self, error):
        QMessageBox.critical(self, "Error", error)

    def apply_styles(self):
        style_sheet = """
        QWidget {
            font-family: 'Arial';
            font-size: 14px;
        }
        QLineEdit, QTextEdit, QListWidget {
            border: 1px solid #c5c5c5;
            border-radius: 5px;
            padding: 5px;
        }
        QPushButton {
            background-color: #4CAF50;
            color: white;
            padding: 8px 16px;
            border: none;
            border-radius: 5px;
            font-weight: bold;
        }
        QPushButton:hover {
            background-color: #45a049;
        }
        QLabel {
            font-weight: bold;
        }
        QScrollBar:vertical {
            background-color: #F1F1F1;
            width: 8px;
        }
        """
        self.setStyleSheet(style_sheet)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    obj = MainWindow()
    sys.exit(app.exec_())
