import json
from database.db_structure import Database, Table
from datetime import date


class StateManagement:
    """Klasa do zarządzania stanem bazy danych."""

    @staticmethod
    def save_state(filename):
        """
        Zapisuje aktualny stan bazy danych do pliku.

        Parametry:
            filename (str): Nazwa pliku, do którego ma być zapisany stan bazy danych.

        Zapisuje:
            Plik JSON zawierający stan bazy danych.
        """
        db_instance = Database.get_instance()  # Get the shared database instance

        def default_converter(o):
            if isinstance(o, date):
                return o.isoformat()
            raise TypeError(f'Object of type {o.__class__.__name__} is not JSON serializable')

        state = {
            table_name: {
                'columns': table.columns,
                'data': table.data
            }
            for table_name, table in db_instance.tables.items()
        }

        with open(filename, 'w') as file:
            json.dump(state, file, default=default_converter)
        print(f"Database state saved to {filename}")

    @staticmethod
    def load_state(filename):
        """
        Wczytuje stan bazy danych z pliku.

        Parametry:
            filename (str): Nazwa pliku, z którego ma być wczytany stan bazy danych.

        Wczytuje:
            Stan bazy danych z pliku JSON i aktualizuje obiekt Database.
        """
        with open(filename, 'r') as file:
            state = json.load(file)

        db_instance = Database.get_instance()  # Get the shared database instance

        db_instance.tables = {
            table_name: Table(table_name, table_info['columns'])
            for table_name, table_info in state.items()
        }

        for table_name, table_info in state.items():
            table = db_instance.tables[table_name]
            table.data = table_info['data']
            for column, values in table.data.items():
                if table.columns[column] == 'DATE':
                    table.data[column] = [date.fromisoformat(value) for value in values]

        print(f"Database state loaded from {filename}")
