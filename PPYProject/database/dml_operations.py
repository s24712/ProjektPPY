import re
from database.db_structure import Database, Table
from datetime import date, time

class DataModificationLanguage:
    """Klasa do obsługi operacji języka manipulacji danymi (DML)."""

    __insert = 'INSERT'
    __into = 'INTO'
    __values = 'VALUES'
    __update = 'UPDATE'
    __set = 'SET'
    __delete = 'DELETE'
    __from = 'FROM'
    __where = 'WHERE'

    def __init__(self, instruction, db_instance=None):
        """
        Inicjalizacja DataModificationLanguage z instrukcjami i instancją bazy danych.

        Parametry:
        instruction (str): Instrukcja w stylu SQL.
        db_instance (Database, opcjonalnie): Instancja bazy danych. Domyślnie None.

        Podnosi:
            TypeError: Jeśli instrukcja nie jest ciągiem znaków.
        """
        if not isinstance(instruction, str):  # check if instructions are strings
            raise TypeError(f'{instruction} is not a string')

        self.allInstructions = instruction.split(';')  # in case of many instructions in one string
        self.allInstIdx = 0  # index of instruction currently read
        self.currentInstruction = self.allInstructions[self.allInstIdx]
        self.instArr = self.currentInstruction.split(' ')  # current instruction split to find key words
        self.table = None  # will be later found based on instruction
        self.db_instance = db_instance or Database.get_instance()  # shared database instance

    def read_instruction(self):
        """
        Parsowanie i wykonanie instrukcji.

        Zwraca:
            bool: True jeśli instrukcja została wykonana pomyślnie, w przeciwnym razie False.
        """
        # looking for type of instruction: insert, update, delete
        if self.instArr[0].upper() == self.__insert and self.instArr[1].upper() == self.__into:  # syntax: INSERT INTO table_name ...
            self.table = self.db_instance.get_table(self.instArr[2])  # table index in this case
            self.instArr = self.instArr[3:]  # deleting already read instructions
            self.insert()

        elif self.instArr[0].upper() == self.__update:  # syntax: UPDATE table_name ...
            self.table = self.db_instance.get_table(self.instArr[1])  # table index in this case
            self.instArr = self.instArr[2:]  # deleting already read instructions
            self.update()

        elif self.instArr[0].upper() == self.__delete and self.instArr[1].upper() == self.__from:
            self.table = self.db_instance.get_table(self.instArr[2])  # table index in this case
            self.instArr = self.instArr[3:]  # deleting already read instructions
            self.delete()
        else:
            return False

        if len(self.allInstructions) > (self.allInstIdx + 1):  # checking for next instruction (if exists)
            self.allInstIdx += 1
            self.currentInstruction = self.allInstructions[self.allInstIdx]
            self.instArr = self.currentInstruction.split(' ')
            self.read_instruction()
        else:
            return True

    def insert(self):  # syntax: column1, column2, ... VALUES value1, value2;
        """
        Wykonanie operacji INSERT.

        Podnosi:
            Exception: Jeśli liczba kolumn nie zgadza się z liczbą wartości.
            TypeError: Jeśli kolumna ma niepoprawny typ.
        """
        # Use regex to match the columns and values
        match = re.match(r"INSERT INTO (\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\);?", self.currentInstruction, re.IGNORECASE)
        if not match:
            raise Exception(f"Invalid INSERT statement: {self.currentInstruction}")

        table_name, columns_str, values_str = match.groups()

        # Split columns and values
        columns = [col.strip() for col in columns_str.split(',')]
        values = [val.strip().strip("'") for val in re.split(r",\s*(?=(?:[^']*'[^']*')*[^']*$)", values_str)]

        # Debug: Print columns and values
        print(f"Debug: Columns: {columns}")
        print(f"Debug: Values: {values}")

        # checking if amount of columns and values is correct
        if len(columns) != len(values):
            raise Exception(f'Column count does not match value count in {self.currentInstruction}')

        self.table = self.db_instance.get_table(table_name)

        data = dict(zip(columns, values))  # creating dictionary of pairs - columns and values
        # converting types
        for col, val in data.items():
            if self.table.columns[col] == 'INTEGER':
                data[col] = int(val)
            elif self.table.columns[col] == 'DATE':
                data[col] = date.fromisoformat(val)
            elif self.table.columns[col] == 'TEXT':
                data[col] = str(val)
            elif self.table.columns[col] == 'FLOAT':
                data[col] = float(val)
            elif self.table.columns[col] == 'BOOLEAN':
                data[col] = bool(val)
            elif self.table.columns[col] == 'TIME':
                data[col] = time.fromisoformat(val)
            else:
                raise TypeError(f'Column {col} is not given the right type in value {val}')

        # adding data to the table
        self.db_instance.validate_data(self.table.name, data)
        for col, val in data.items():
            self.table.data[col].append(val)

    def update(self):  # syntax: UPDATE
        """
        Wykonanie operacji UPDATE.

        Podnosi:
            ValueError: Jeśli nie znaleziono żadnych poprawnych aktualizacji.
        """
        set_index = self.instArr.index(self.__set) + 1
        where_index = self.instArr.index(self.__where) + 1

        updates = self.instArr[set_index:where_index - 1]
        conditions = self.instArr[where_index:]

        updates_dict = {}
        for i in range(0, len(updates), 2):
            if i + 1 < len(updates):
                key = updates[i].strip(',').strip('=')
                value = updates[i + 1].strip(',').strip('=').strip("'")
                updates_dict[key] = value

        if not updates_dict:
            raise ValueError(f"No valid updates found in {self.currentInstruction}")

        for row_idx in range(len(self.table.data[next(iter(self.table.data))])):
            row = {col: self.table.data[col][row_idx] for col in self.table.columns}
            if self.check_condition(conditions, row):
                for col, val in updates_dict.items():
                    if self.table.columns[col] == 'INTEGER':
                        self.table.data[col][row_idx] = int(val)
                    elif self.table.columns[col] == 'DATE':
                        self.table.data[col][row_idx] = date.fromisoformat(val)
                    else:
                        self.table.data[col][row_idx] = val

    def delete(self):
        """
        Wykonanie operacji DELETE.
        """
        where_index = self.instArr.index(self.__where) + 1
        conditions = self.instArr[where_index:]

        rows_to_delete = []
        for row_idx in range(len(self.table.data[next(iter(self.table.data))])):
            row = {col: self.table.data[col][row_idx] for col in self.table.columns}
            if self.check_condition(conditions, row):
                rows_to_delete.append(row_idx)

        for row_idx in sorted(rows_to_delete, reverse=True):
            for col in self.table.columns:
                del self.table.data[col][row_idx]

    def check_condition(self, conditions, row):
        """
        Sprawdzenie, czy wiersz spełnia podane warunki.

        Parametry:
            conditions (list): Lista warunków do sprawdzenia.
            row (dict): Dane wiersza.

        Zwraca:
            bool: True jeśli wiersz spełnia warunki, w przeciwnym razie False.

        Podnosi:
            ValueError: Jeśli podano nieprawidłowy warunek.
        """
        condition_str = ' '.join(conditions)
        if "==" in condition_str:
            key, value = condition_str.split("==")
            key = key.strip()
            value = value.strip().strip("'")
            if key in row:
                return row[key] == value
        elif "!=" in condition_str:
            key, value = condition_str.split("!=")
            key = key.strip()
            value = value.strip().strip("'")
            if key in row:
                return row[key] != value
        elif ">" in condition_str:
            key, value = condition_str.split(">")
            key = key.strip()
            value = value.strip().strip("'")
            if key in row:
                return row[key] > value
        elif "<" in condition_str:
            key, value = condition_str.split("<")
            key = key.strip()
            value = value.strip().strip("'")
            if key in row:
                return row[key] < value
        elif ">=" in condition_str:
            key, value = condition_str.split(">=")
            key = key.strip()
            value = value.strip().strip("'")
            if key in row:
                return row[key] >= value
        elif "<=" in condition_str:
            key, value = condition_str.split("<=")
            key = key.strip()
            value = value.strip().strip("'")
            if key in row:
                return row[key] <= value
        raise ValueError(f"Invalid condition: {condition_str}")

    @staticmethod
    def has_comma(string):
        """
        Sprawdzenie, czy ciąg kończy się przecinkiem.

        Parametry:
            string (str): Ciąg znaków do sprawdzenia.

        Zwraca:
            bool: True jeśli ciąg kończy się przecinkiem, w przeciwnym razie False.
        """
        return string[-1] == ',' if string else False

    @staticmethod
    def has_semicolon(string):
        """
        Sprawdzenie, czy ciąg kończy się średnikiem.

        Parametry:
            string (str): Ciąg znaków do sprawdzenia.

        Zwraca:
            bool: True jeśli ciąg kończy się średnikiem, w przeciwnym razie False.
        """
        return string[-1] == ';' if string else False
