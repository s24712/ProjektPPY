import unittest
from datetime import date

from database.ddl_operations import DDL
from database.db_structure import Table, Database
from database.dml_operations import DataModificationLanguage


class TestDDL(unittest.TestCase):
    def setUp(self):
        """
        Przygotowanie instancji DDL przed każdym testem.
        """
        self.db_instance = Database.get_instance()
        self.ddl = DDL(self.db_instance)

    def tearDown(self):
        """
        Resetuje instancję bazy danych po każdym teście.
        """
        self.db_instance.tables.clear()

    def test_create_table(self):
        """
        Testuje tworzenie nowej tabeli.
        """
        self.ddl.create_table('test_table', {'id': 'INTEGER', 'name': 'TEXT'})
        self.assertIn('test_table', self.db_instance.tables)
        self.assertEqual(self.db_instance.tables['test_table'].name, 'test_table')
        self.assertEqual(self.db_instance.tables['test_table'].columns, {'id': 'INTEGER', 'name': 'TEXT'})

    def test_create_table_already_exists(self):
        """
        Testuje próbę utworzenia tabeli, która już istnieje.
        """
        self.ddl.create_table('test_table', {'id': 'INTEGER', 'name': 'TEXT'})
        with self.assertRaises(Exception):
            self.ddl.create_table('test_table', {'id': 'INTEGER', 'name': 'TEXT'})

    def test_drop_table(self):
        """
        Testuje usuwanie istniejącej tabeli.
        """
        self.ddl.create_table('test_table', {'id': 'INTEGER', 'name': 'TEXT'})
        self.ddl.drop_table('test_table')
        self.assertNotIn('test_table', self.db_instance.tables)

    def test_drop_table_non_existent(self):
        """
        Testuje próbę usunięcia nieistniejącej tabeli.
        """
        with self.assertRaises(Exception):
            self.ddl.drop_table('non_existent_table')

    def test_add_column(self):
        """
        Testuje dodawanie nowej kolumny do tabeli.
        """
        self.ddl.create_table('test_table', {'id': 'INTEGER', 'name': 'TEXT'})
        self.ddl.add_column('test_table', 'age', 'INTEGER')
        self.assertIn('age', self.db_instance.tables['test_table'].columns)
        self.assertEqual(self.db_instance.tables['test_table'].columns['age'], 'INTEGER')

    def test_add_column_to_non_existent_table(self):
        """
        Testuje próbę dodania kolumny do nieistniejącej tabeli.
        """
        with self.assertRaises(Exception):
            self.ddl.add_column('non_existent_table', 'age', 'INTEGER')

    def test_drop_column(self):
        """
        Testuje usuwanie kolumny z tabeli.
        """
        self.ddl.create_table('test_table', {'id': 'INTEGER', 'name': 'TEXT'})
        self.ddl.add_column('test_table', 'age', 'INTEGER')
        self.ddl.drop_column('test_table', 'age')
        self.assertNotIn('age', self.db_instance.tables['test_table'].columns)

    def test_drop_column_non_existent(self):
        """
        Testuje próbę usunięcia nieistniejącej kolumny z tabeli.
        """
        self.ddl.create_table('test_table', {'id': 'INTEGER', 'name': 'TEXT'})
        with self.assertRaises(Exception):
            self.ddl.drop_column('test_table', 'age')

    def test_drop_column_from_non_existent_table(self):
        """
        Testuje próbę usunięcia kolumny z nieistniejącej tabeli.
        """
        with self.assertRaises(Exception):
            self.ddl.drop_column('non_existent_table', 'age')


class TestDataModificationLanguage(unittest.TestCase):
    def setUp(self):
        """
        Przygotowanie bazy danych i tabeli przed każdym testem.
        """
        self.db_instance = Database.get_instance()
        self.ddl = DDL(self.db_instance)
        self.table_name = 'students'
        self.columns = {
            'id': 'INTEGER',
            'name': 'TEXT',
            'age': 'INTEGER',
        }
        self.ddl.create_table(self.table_name, self.columns)

    def tearDown(self):
        """
        Czyszczenie bazy danych po każdym teście.
        """
        self.db_instance.tables.clear()

    def test_insert_operation(self):
        """
        Testuje operację INSERT.
        """
        dml = DataModificationLanguage("INSERT INTO students (id, name, age) VALUES (1, 'John_Doe', 21);", self.db_instance)
        dml.read_instruction()

        # Sprawdzenie czy dane zostały poprawnie wstawione do tabeli
        self.assertEqual(len(self.db_instance.tables[self.table_name].data['id']), 1)
        self.assertEqual(self.db_instance.tables[self.table_name].data['name'][0], 'John_Doe')
        self.assertEqual(self.db_instance.tables[self.table_name].data['age'][0], 21)

    def test_update_operation(self):
        """
        Testuje operację UPDATE.
        """
        dml = DataModificationLanguage("INSERT INTO students (id, name, age) VALUES (1, 'John_Doe', 21);", self.db_instance)
        dml.read_instruction()

        dml = DataModificationLanguage("UPDATE students SET age = 21 WHERE id == 1;", self.db_instance)
        dml.read_instruction()

        # Sprawdzenie czy dane zostały poprawnie zaktualizowane w tabeli
        self.assertEqual(self.db_instance.tables[self.table_name].data['age'][0], 21)

    def test_delete_operation(self):
        """
        Testuje operację DELETE.
        """
        dml = DataModificationLanguage("INSERT INTO students (id, name, age) VALUES (1, 'John_Doe', 20);", self.db_instance)
        dml.read_instruction()

        dml = DataModificationLanguage("DELETE FROM students WHERE id == 1;", self.db_instance)
        dml.read_instruction()

        # Sprawdzenie czy dane zostały poprawnie usunięte z tabeli
        self.assertEqual(len(self.db_instance.tables[self.table_name].data['id']), 1)

    def test_invalid_instruction_type(self):
        """
        Testuje obsługę błędu dla niepoprawnego typu instrukcji.
        """
        with self.assertRaises(TypeError):
            DataModificationLanguage(123, self.db_instance)

    def test_invalid_column_value_count(self):
        """
        Testuje obsługę błędu dla niezgodności liczby kolumn i wartości w instrukcji INSERT.
        """
        with self.assertRaises(Exception):
            dml = DataModificationLanguage("INSERT INTO students (id, name) VALUES (1, 'John_Doe', 20);", self.db_instance)
            dml.read_instruction()


if __name__ == '__main__':
    unittest.main()
