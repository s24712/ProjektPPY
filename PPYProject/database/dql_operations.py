from database.db_structure import Database


class DataQueryLanguage:
    """Klasa do obsługi operacji zapytania danych (DQL)."""

    __select = 'SELECT'
    __from = 'FROM'
    __where = 'WHERE'

    def __init__(self, instruction, db_instance=None):
        """
        Inicjalizacja DataQueryLanguage z instrukcją.

        Parametry:
            instruction (str): Instrukcja w stylu SQL.
            db_instance (Database, opcjonalnie): Instancja bazy danych. Domyślnie None.

        Podnosi:
            TypeError: Jeśli instrukcja nie jest ciągiem znaków.
        """
        if not isinstance(instruction, str):
            raise TypeError(f'{instruction} is not a string')

        self.allInstructions = instruction.split(';')
        self.currentInstruction = self.allInstructions[0]
        self.instArr = self.currentInstruction.split(' ')
        self.table = None
        self.db_instance = db_instance or Database.get_instance()  # shared database instance

    def read_instruction(self):
        """
        Parsowanie i wykonanie instrukcji.

        Zwraca:
            list: Wyniki zapytania SELECT.
        """
        if self.instArr[0].upper() == self.__select:  # finding SELECT in instruction
            return self.select()
        else:
            return False

    def select(self):
        """
        Wykonanie operacji SELECT.

        Zwraca:
            list: Lista wierszy spełniających warunki zapytania.
        """
        columns = []
        condition = []
        index_counter = 1

        # Extract columns
        while index_counter < len(self.instArr) and self.instArr[index_counter].upper() != self.__from:
            columns.append(self.instArr[index_counter].strip(','))
            index_counter += 1

        # Move past the FROM keyword
        if index_counter < len(self.instArr) and self.instArr[index_counter].upper() == self.__from:
            index_counter += 1
        else:
            raise ValueError(f"Invalid SELECT statement: {self.currentInstruction}")

        # Extract table name
        if index_counter < len(self.instArr):
            table_name = self.instArr[index_counter]
            index_counter += 1
        else:
            raise ValueError(f"Invalid SELECT statement: {self.currentInstruction}")

        # Debug: Print the table name
        print(f"Debug: Table name: {table_name}")

        self.table = self.db_instance.get_table(table_name)

        # Extract condition if present
        if index_counter < len(self.instArr) and self.instArr[index_counter].upper() == self.__where:
            index_counter += 1
            condition = self.instArr[index_counter:]

        results = []
        for row_idx in range(len(self.table.data[next(iter(self.table.data))])):
            row = {col: self.table.data[col][row_idx] for col in self.table.columns}
            if not condition or self.check_condition(condition, row):
                result_row = {col: row[col] for col in columns}
                results.append(result_row)

        return results

    def check_condition(self, condition, row):
        """
        Sprawdzenie, czy wiersz spełnia podane warunki.

        Parametry:
            condition (list): Lista warunków do sprawdzenia.
            row (dict): Dane wiersza.

        Zwraca:
            bool: True jeśli wiersz spełnia warunki, w przeciwnym razie False.

        Podnosi:
            ValueError: Jeśli podano nieprawidłowy warunek.
        """
        condition_str = ' '.join(condition)
        condition_str = condition_str.replace("=", "==")
        try:
            return eval(condition_str, {}, row)
        except:
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
