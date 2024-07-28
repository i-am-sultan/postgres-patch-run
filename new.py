import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, QHBoxLayout, QWidget, QTextEdit, QLabel, QLineEdit, QListWidget, QAbstractItemView, QMessageBox)
import psycopg2

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Postgres Patch Run Application")

        # Load initial credentials from config.py
        self.load_db_credentials()

        main_layout = QVBoxLayout()

        # Database credentials input fields
        credentials_layout = QHBoxLayout()

        self.user_edit = QLineEdit()
        self.user_edit.setPlaceholderText("User")
        self.user_edit.setText(self.db_credentials['user'])
        credentials_layout.addWidget(QLabel("User:"))
        credentials_layout.addWidget(self.user_edit)

        self.password_edit = QLineEdit()
        self.password_edit.setPlaceholderText("Password")
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.password_edit.setText(self.db_credentials['password'])
        credentials_layout.addWidget(QLabel("Password:"))
        credentials_layout.addWidget(self.password_edit)

        self.host_edit = QLineEdit()
        self.host_edit.setPlaceholderText("Host")
        self.host_edit.setText(self.db_credentials['host'])
        credentials_layout.addWidget(QLabel("Host:"))
        credentials_layout.addWidget(self.host_edit)

        self.port_edit = QLineEdit()
        self.port_edit.setPlaceholderText("Port")
        self.port_edit.setText(self.db_credentials['port'])
        credentials_layout.addWidget(QLabel("Port:"))
        credentials_layout.addWidget(self.port_edit)

        main_layout.addLayout(credentials_layout)

        # Database selection list
        self.db_list_widget = QListWidget()
        self.db_list_widget.setSelectionMode(QAbstractItemView.MultiSelection)
        main_layout.addWidget(QLabel("Select Databases:"))
        main_layout.addWidget(self.db_list_widget)
        

        # Fetch databases button
        self.fetch_dbs_button = QPushButton("Fetch Databases")
        self.fetch_dbs_button.clicked.connect(self.fetch_databases)
        main_layout.addWidget(self.fetch_dbs_button)

        # Query input field
        self.query_edit = QTextEdit()
        self.query_edit.setPlaceholderText("Enter your SQL query here")
        main_layout.addWidget(self.query_edit)

        # Result display field
        self.result_edit = QTextEdit()
        self.result_edit.setReadOnly(True)
        main_layout.addWidget(self.result_edit)

        # Run query button
        self.run_button = QPushButton("Run Query")
        self.run_button.clicked.connect(self.run_query)
        main_layout.addWidget(self.run_button)



        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

    def load_db_credentials(self):
        try:
            with open('config.py', 'r') as f:
                config_data = f.read()
            exec(config_data, globals())
            self.db_credentials = DB_CONFIG
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load database credentials: {e}")

    def save_db_credentials(self):
        self.db_credentials['user'] = self.user_edit.text()
        self.db_credentials['password'] = self.password_edit.text()
        self.db_credentials['host'] = self.host_edit.text()
        self.db_credentials['port'] = self.port_edit.text()

        config_content = f"""DB_CONFIG = {{
            'dbname': '',
            'user': '{self.db_credentials['user']}',
            'password': '{self.db_credentials['password']}',
            'host': '{self.db_credentials['host']}',
            'port': '{self.db_credentials['port']}'
        }}
        """
        try:
            with open('config.py', 'w') as f:
                f.write(config_content)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save database credentials: {e}")

    def fetch_databases(self):
        self.save_db_credentials()
        self.load_db_credentials()
        try:
            conn = psycopg2.connect(
                dbname='postgres',  # Connect to the default database to fetch the list
                user=self.user_edit.text(),
                password=self.password_edit.text(),
                host=self.host_edit.text(),
                port=self.port_edit.text()
            )
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datname NOT LIKE '%azure%' AND datname <> 'template0' AND datname <> 'template1'")
            databases = cursor.fetchall()
            conn.commit()
            cursor.close()
            conn.close()

            self.db_list_widget.clear()
            for db in databases:
                self.db_list_widget.addItem(db[0])

        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def run_query(self):
        selected_dbs = [item.text() for item in self.db_list_widget.selectedItems()]
        if not selected_dbs:
            QMessageBox.warning(self, "Warning", "Please select at least one database.")
            return
        query = self.query_edit.toPlainText()
        if not query:
            QMessageBox.warning(self, "Warning", "Please enter an SQL query.")
            return
        results = []
        for db in selected_dbs:
            try:
                conn = psycopg2.connect(
                    dbname=db,
                    user=self.user_edit.text(),
                    password=self.password_edit.text(),
                    host=self.host_edit.text(),
                    port=self.port_edit.text()
                )
                cursor = conn.cursor()
                cursor.execute(query)
                results.append(f"Patch successfully applied to {db}.")
                conn.commit()
                cursor.close()
                conn.close()

            except Exception as e:
                results.append(f"Error from {db}:\n{str(e)}")
        self.result_edit.setPlainText("\n\n".join(results))
app = QApplication(sys.argv)
window = MainWindow()
window.show()
app.exec()