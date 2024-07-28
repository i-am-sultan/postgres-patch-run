import sys
import re
import psycopg2
import logging
from PyQt5.QtWidgets import (QApplication,QGridLayout, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QListWidget, QAbstractItemView, QPushButton, QTextEdit, QMessageBox)
from PyQt5.QtCore import QThread, pyqtSignal

pgcon_path = r'C:\Users\sultan.m\Documents\Ginesys\PatchRun\pgcon.txt'

# Configure logging
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseWorker(QThread):
    databases_fetched = pyqtSignal(list)
    query_executed = pyqtSignal(list)

    def __init__(self, operation, credentials, query=None, databases=None):
        super().__init__()
        self.operation = operation
        self.credentials = credentials
        self.query = query
        self.databases = databases

    def run(self):
        if self.operation == 'fetch':
            self.fetch_databases()
        elif self.operation == 'execute':
            self.execute_query()

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
            cursor.execute("SELECT datname FROM pg_database WHERE datname NOT LIKE '%azure%' AND datname <> 'template0' AND datname <> 'template1' AND datname <> 'postgres'")
            databases = cursor.fetchall()
            conn.commit()
            cursor.close()
            conn.close()
            self.databases_fetched.emit([db[0] for db in databases])
        except Exception as e:
            logging.error(f"Error fetching databases: {e}")
            self.databases_fetched.emit([])

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
                cursor.close()
                conn.close()
                results.append(f'Patch successfully applied to database {db}.')
            except Exception as e:
                logging.error(f"Error executing query on database {db}: {e}")
                results.append(f"Error executing query on database {db}: {str(e)}")
        self.query_executed.emit(results)

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Ginesys-PG-Patch-Executor')
        self.setGeometry(300, 200, 1000, 500)

        main_layout = QVBoxLayout()
        grid_layout = QGridLayout()

        # Layout for credentials
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

        # Layout for database select lists
        self.db_list_widget = QListWidget()
        self.db_list_widget.setSelectionMode(QAbstractItemView.MultiSelection)

        # Button to fetch database names
        self.fetch_dbname_button = QPushButton('Fetch Databases')
        self.fetch_dbname_button.clicked.connect(self.fetchDatabases)

        # Layout for writing query
        self.queryInput = QTextEdit()
        self.queryInput.setPlaceholderText('Enter patch here...')

        # Button to run the query
        self.run_query_button = QPushButton('Execute')
        self.run_query_button.clicked.connect(self.runQuery)

        # Layout for log window
        self.logWindow = QTextEdit()
        self.logWindow.setReadOnly(True)

        main_layout.addLayout(grid_layout)
        main_layout.addWidget(QLabel("Select Databases:"))
        main_layout.addWidget(self.db_list_widget)
        main_layout.addWidget(self.fetch_dbname_button)
        main_layout.addWidget(self.queryInput)
        main_layout.addWidget(self.run_query_button)
        main_layout.addWidget(self.logWindow)
        self.setLayout(main_layout)
        self.loadCredentials()
        self.show()

    def loadCredentials(self):
        try:
            with open(pgcon_path, 'r') as f:
                content = f.read()
            pghost_match = re.search(r'Server=([^;]+);', content)
            pgport_match = re.search(r'Port=([^;]+);', content)
            pgpass_match = re.search(r'Password=([^;]+);', content)
            pguser_match = re.search(r'User Id=([^;]+);', content)

            if pghost_match and pgport_match and pguser_match and pgpass_match:
                self.pgHostInput.setText(pghost_match.group(1))
                self.pgPortInput.setText(pgport_match.group(1))
                self.pgPasswordInput.setText(pgpass_match.group(1))
                self.pgUserInput.setText(pguser_match.group(1))
            else:
                self.logWindow.append("Error: PostgreSQL Credentials not found in pgCon.txt")
        except Exception as e:
            self.logWindow.append(f'Error loading credentials from file: {e}')
            logging.error(f'Error loading credentials from file: {e}')

    def fetchDatabases(self):
        credentials = {
            'host': self.pgHostInput.text(),
            'port': self.pgPortInput.text(),
            'user': self.pgUserInput.text(),
            'password': self.pgPasswordInput.text()
        }
        self.db_worker = DatabaseWorker(operation='fetch', credentials=credentials)
        self.db_worker.databases_fetched.connect(self.onDatabasesFetched)
        self.db_worker.start()

    def onDatabasesFetched(self, databases):
        if databases:
            self.db_list_widget.clear()
            for db in databases:
                self.db_list_widget.addItem(db)
            self.logWindow.append("Databases fetched successfully.")
            logging.info("Databases fetched successfully.")
        else:
            self.logWindow.append("Error fetching databases. Check the log for details.")
            logging.error("Error fetching databases. Check the log for details.")

    def runQuery(self):
        selected_dbs = [item.text() for item in self.db_list_widget.selectedItems()]
        if not selected_dbs:
            QMessageBox.critical(self, "Warning!", 'No database has been selected')
            return
        query = self.queryInput.toPlainText()
        if not query:
            QMessageBox.critical(self, "Warning!", 'Please enter an SQL Query.')
            return
        credentials = {
            'host': self.pgHostInput.text(),
            'port': self.pgPortInput.text(),
            'user': self.pgUserInput.text(),
            'password': self.pgPasswordInput.text()
        }
        self.query_worker = DatabaseWorker(operation='execute', credentials=credentials, query=query, databases=selected_dbs)
        self.query_worker.query_executed.connect(self.onQueryExecuted)
        self.query_worker.start()

    def onQueryExecuted(self, results):
        for result in results:
            self.logWindow.append(result)
        logging.info("Query executed. Check the log window for results.")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    obj = MainWindow()
    sys.exit(app.exec_())
