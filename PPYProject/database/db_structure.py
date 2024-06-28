from datetime import date, time


class Table:
    def __init__(self, name, columns):
        """
        Inicjalizuje tabelę z nazwą i słownikiem kolumn.

        Parametry:
        name (str): Nazwa tabeli.
        columns (dict): Słownik, gdzie klucze to nazwy kolumn, a wartości to typy kolumn.
        """
        self.name = name
        self.columns = columns
        # Inicjalizuje pustą listę dla każdej kolumny w danych tabeli
        self.data = {column: [] for column in columns}

    def add_column(self, column_name, column_type):
        """
        Dodaje nową kolumnę do tabeli.

        Parametry:
        column_name (str): Nazwa nowej kolumny.
        column_type (str): Typ nowej kolumny.
        """
        self.columns[column_name] = column_type
        self.data[column_name] = []

    @staticmethod
    def get_type(python_type):
        """
        Zwraca typ SQL odpowiadający podanemu typowi Pythona.

        Parametry:
        python_type (str): Typ danych w Pythonie.

        Zwraca:
        str: Odpowiadający typ SQL.
        """
        type_names = {
            'int': 'INTEGER',
            'str': 'TEXT',
            'float': 'FLOAT',
            'bool': 'BOOLEAN',
            'date': 'DATE',
            'time': 'TIME'
        }
        return type_names.get(python_type, 'UNKNOWN')


class Database:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        """
        Inicjalizuje bazę danych zawierającą słownik tabel.
        """
        self.tables = {}

    def get_table(self, name):
        """
        Zwraca tabelę o podanej nazwie.

        Parametry:
        name (str): Nazwa tabeli.

        Zwraca:
        Table: Tabela o podanej nazwie.

        Podnosi:
        ValueError: Jeśli tabela o podanej nazwie nie istnieje.
        """
        if name not in self.tables:
            raise ValueError(f"Table {name} does not exist.")
        return self.tables[name]

    def validate_data(self, table_name, data):
        """
        Waliduje dane wstawiane do tabeli.

        Parametry:
        table_name (str): Nazwa tabeli.
        data (dict): Słownik danych do walidacji, gdzie klucze to nazwy kolumn, a wartości to wstawiane dane.

        Podnosi:
        ValueError: Jeśli tabela o podanej nazwie nie istnieje.
        TypeError: Jeśli typ danych w kolumnie jest nieprawidłowy.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table {table_name} does not exist.")
        table = self.tables[table_name]
        columns = table.columns
        for column, value in data.items():
            if column in columns:
                expected_type = columns[column]
                # Sprawdzanie typu INTEGER
                if 'INTEGER' in expected_type and not isinstance(value, int):
                    raise TypeError(
                        f"Invalid type for column {column}: expected INTEGER, got {type(value).__name__}")
                # Sprawdzanie typu i długości TEXT
                elif 'TEXT' in expected_type:
                    max_length = int(expected_type.strip()[5:-1]) if len(expected_type.strip()) > 4 else None
                    if not isinstance(value, str):
                        raise TypeError(
                            f"Invalid type for column {column}: expected TEXT, got {type(value).__name__}")
                    if max_length and len(value) > max_length:
                        raise ValueError(
                            f"Invalid length for column {column}: expected max {max_length}, got {len(value)}")
                # Sprawdzanie typu FLOAT
                elif 'FLOAT' in expected_type and not isinstance(value, float):
                    raise TypeError(
                        f"Invalid type for column {column}: expected FLOAT, got {type(value).__name__}")
                # Sprawdzanie typu BOOLEAN
                elif 'BOOLEAN' in expected_type and not isinstance(value, bool):
                    raise TypeError(
                        f"Invalid type for column {column}: expected BOOLEAN, got {type(value).__name__}")
                # Sprawdzanie typu DATE
                elif 'DATE' in expected_type and not isinstance(value, date):
                    raise TypeError(
                        f"Invalid type for column {column}: expected DATE, got {type(value).__name__}")
                # Sprawdzanie typu TIME
                elif 'TIME' in expected_type and not isinstance(value, time):
                    raise TypeError(
                        f"Invalid type for column {column}: expected TIME, got {type(value).__name__}")
