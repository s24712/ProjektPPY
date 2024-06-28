from database import DataModificationLanguage, DDL, DataQueryLanguage, Database
from state_management import StateManagement


def main():
    # Create a shared instance of the Database class
    db_instance = Database.get_instance()

    # Create an instance of the DDL class
    ddl = DDL(db_instance)

    # Create a new table
    table_name = 'students'
    columns = {
        'id': 'INTEGER',
        'name': 'TEXT',
        'age': 'INTEGER',
        'enrollment_date': 'DATE'
    }
    print(f"Creating table '{table_name}' with columns: {columns}")
    ddl.create_table(table_name, columns)

    # Insert data into the table
    insert_instruction = "INSERT INTO students (id, name, age, enrollment_date) VALUES (1, 'John Doe', 20, '2022-09-01');"
    print(f"Executing: {insert_instruction}")
    dml = DataModificationLanguage(insert_instruction, db_instance)
    dml.read_instruction()

    insert_instruction = "INSERT INTO students (id, name, age, enrollment_date) VALUES (2, 'Jane Smith', 22, '2021-09-01');"
    print(f"Executing: {insert_instruction}")
    dml = DataModificationLanguage(insert_instruction, db_instance)
    dml.read_instruction()

    # Update data in the table
    update_instruction = "UPDATE students SET age = 21 WHERE id == 1;"
    print(f"Executing: {update_instruction}")
    dml = DataModificationLanguage(update_instruction, db_instance)
    dml.read_instruction()

    # Query data from the table
    select_instruction = "SELECT id, name, age, enrollment_date FROM students WHERE age > 20;"
    print(f"Executing: {select_instruction}")
    dql = DataQueryLanguage(select_instruction, db_instance)
    results = dql.read_instruction()
    print("Query results:")
    for row in results:
        print(row)

    # Delete data from the table
    delete_instruction = "DELETE FROM students WHERE id == 2;"
    print(f"Executing: {delete_instruction}")
    dml = DataModificationLanguage(delete_instruction, db_instance)
    dml.read_instruction()

    # Save the current state of the database
    state_filename = 'db_state.json'
    print(f"Saving database state to '{state_filename}'")
    StateManagement.save_state(state_filename)

    # Load the state of the database
    print(f"Loading database state from '{state_filename}'")
    StateManagement.load_state(state_filename)

    # Delete the table after all operations are done
    print(f"Dropping table '{table_name}'")
    ddl.drop_table(table_name)


if __name__ == '__main__':
    main()
