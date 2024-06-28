from database.db_structure import Database, Table

class DDL:
    def __init__(self, db_instance=None):
        """
        Inicjalizuje DDL z instancją bazy danych.

        Parametry:
        db_instance (Database, opcjonalnie): Instancja bazy danych. Domyślnie None.
        """
        self.db_instance = db_instance or Database.get_instance()

    def create_table(self, name, columns):
        """
        Tworzy nową tabelę o podanej nazwie i kolumnach.

        Parametry:
        name (str): Nazwa tabeli.
        columns (dict): Słownik kolumn, gdzie klucze to nazwy kolumn, a wartości to typy kolumn.

        Podnosi:
        Exception: Jeśli tabela o podanej nazwie już istnieje.
        """
        if name in self.db_instance.tables:
            raise Exception(f"Table {name} already exists")
        self.db_instance.tables[name] = Table(name, columns)
        print(f"Table {name} created successfully with columns: {columns}")

    def drop_table(self, name):
        """
        Usuwa tabelę o podanej nazwie.

        Parametry:
        name (str): Nazwa tabeli do usunięcia.

        Podnosi:
        Exception: Jeśli tabela o podanej nazwie nie istnieje.
        """
        if name not in self.db_instance.tables:
            raise Exception(f"Table {name} does not exist")
        del self.db_instance.tables[name]
        print(f"Table {name} dropped successfully")

    def add_column(self, table_name, column_name, column_type):
        """
        Dodaje nową kolumnę do istniejącej tabeli.

        Parametry:
        table_name (str): Nazwa tabeli, do której ma zostać dodana kolumna.
        column_name (str): Nazwa nowej kolumny.
        column_type (str): Typ nowej kolumny.

        Podnosi:
        Exception: Jeśli tabela o podanej nazwie nie istnieje.
        """
        if table_name not in self.db_instance.tables:
            raise Exception(f"Table {table_name} does not exist")
        table = self.db_instance.tables[table_name]
        table.add_column(column_name, column_type)
        print(f"Column {column_name} of type {column_type} added to table {table_name}")

    def drop_column(self, table_name, column_name):
        """
        Usuwa kolumnę z istniejącej tabeli.

        Parametry:
        table_name (str): Nazwa tabeli, z której ma zostać usunięta kolumna.
        column_name (str): Nazwa kolumny do usunięcia.

        Podnosi:
        Exception: Jeśli tabela o podanej nazwie nie istnieje.
        Exception: Jeśli kolumna o podanej nazwie nie istnieje w tabeli.
        """
        if table_name not in self.db_instance.tables:
            raise Exception(f"Table {table_name} does not exist")
        table = self.db_instance.tables[table_name]
        if column_name not in table.columns:
            raise Exception(f"Column {column_name} does not exist in table {table_name}")
        del table.columns[column_name]
        del table.data[column_name]
        print(f"Column {column_name} dropped from table {table_name}")
