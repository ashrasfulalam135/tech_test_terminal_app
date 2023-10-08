import unittest
import hashlib
import sqlparse
from sql_parser import is_select_statement, validate_sql_query, hash_column_names, parse_statement, map_original_hashed_column_name, modified_query


class TestSQLParser(unittest.TestCase):
    def test_is_select_statement_valid(self):
        select_statement = "SELECT * FROM table1 WHERE column1 = 'value';"
        self.assertTrue(is_select_statement(select_statement))

    def test_is_select_statement_false(self):
        insert_statement = "INSERT INTO Customers (CustomerName, ContactName) VALUES ('Cardinal','Tom B. Erichsen');"
        self.assertFalse(is_select_statement(insert_statement))

    def test_validate_sql_query_valid(self):
        valid_sql = "SELECT * FROM table1 WHERE column1 = 'value';"
        self.assertTrue(validate_sql_query(valid_sql))

    def test_validate_sql_query_invalid(self):
        invalid_sql = "SELECT * FROM WHERE column1 = 'value';"
        self.assertFalse(validate_sql_query(invalid_sql))

    def test_hash_column_names(self):
        # Test hashing of column names
        sql_query = "SELECT name, age FROM user WHERE age > 18 And father_name = 'abc'"
        expected_mapping = {
            "name": hashlib.sha256("name".encode()).hexdigest(),
            "age": hashlib.sha256("age".encode()).hexdigest(),
            "father_name": hashlib.sha256("father_name".encode()).hexdigest(),
        }
        column_name_mapping = hash_column_names(sql_query)
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_wildcard(self):
        # Test parsing of SQL statement
        sql_query = "SELECT * FROM user"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "*": hashlib.sha256("*".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_identifier(self):
        # Test parsing of SQL statement
        sql_query = "SELECT name FROM user"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "name": hashlib.sha256("name".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_identifierlist(self):
        # Test parsing of SQL statement
        sql_query = "SELECT name, age FROM user"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "name": hashlib.sha256("name".encode()).hexdigest(),
            "age": hashlib.sha256("age".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_singel_where_clause(self):
        # Test parsing of SQL statement
        sql_query = "SELECT name, age FROM user where department = 'HR'"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "name": hashlib.sha256("name".encode()).hexdigest(),
            "age": hashlib.sha256("age".encode()).hexdigest(),
            "department": hashlib.sha256("department".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_multiple_where_clause(self):
        # Test parsing of SQL statement
        sql_query = "SELECT name, age FROM employee where department = 'HR' AND working_year > 3"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "name": hashlib.sha256("name".encode()).hexdigest(),
            "age": hashlib.sha256("age".encode()).hexdigest(),
            "department": hashlib.sha256("department".encode()).hexdigest(),
            "working_year": hashlib.sha256("working_year".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_nested_query(self):
        # Test parsing of SQL statement
        sql_query = "SELECT name FROM customers WHERE country = 'USA' AND customer_id IN (SELECT customer_id FROM Orders);"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "name": hashlib.sha256("name".encode()).hexdigest(),
            "country": hashlib.sha256("country".encode()).hexdigest(),
            "customer_id": hashlib.sha256("customer_id".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_inner_join_query(self):
        # Test parsing of SQL statement
        sql_query = "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "OrderID": hashlib.sha256("OrderID".encode()).hexdigest(),
            "CustomerName": hashlib.sha256("CustomerName".encode()).hexdigest(),
            "OrderDate": hashlib.sha256("OrderDate".encode()).hexdigest(),
            "CustomerID": hashlib.sha256("CustomerID".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_left_join_query(self):
        # Test parsing of SQL statement
        sql_query = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers LEFT JOIN Orders ON Customers.CustomerID=Orders.CustomerID ORDER BY Customers.CustomerName;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "CustomerName": hashlib.sha256("CustomerName".encode()).hexdigest(),
            "OrderID": hashlib.sha256("OrderID".encode()).hexdigest(),
            "CustomerID": hashlib.sha256("CustomerID".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_right_join_query(self):
        # Test parsing of SQL statement
        sql_query = "SELECT Orders.OrderID, Employees.LastName, Employees.FirstName FROM Orders RIGHT JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID ORDER BY Orders.OrderID;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "OrderID": hashlib.sha256("OrderID".encode()).hexdigest(),
            "LastName": hashlib.sha256("LastName".encode()).hexdigest(),
            "FirstName": hashlib.sha256("FirstName".encode()).hexdigest(),
            "EmployeeID": hashlib.sha256("EmployeeID".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_full_outer_join_query(self):
        # Test parsing of SQL statement
        sql_query = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers FULL OUTER JOIN Orders ON Customers.CustomerID=Orders.CustomerID ORDER BY Customers.CustomerName;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "CustomerName": hashlib.sha256("CustomerName".encode()).hexdigest(),
            "OrderID": hashlib.sha256("OrderID".encode()).hexdigest(),
            "CustomerID": hashlib.sha256("CustomerID".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_full_outer_join_query(self):
        # Test parsing of SQL statement
        sql_query = "SELECT Customers.CustomerName, Orders.OrderID FROM Customers FULL OUTER JOIN Orders ON Customers.CustomerID=Orders.CustomerID ORDER BY Customers.CustomerName;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "CustomerName": hashlib.sha256("CustomerName".encode()).hexdigest(),
            "OrderID": hashlib.sha256("OrderID".encode()).hexdigest(),
            "CustomerID": hashlib.sha256("CustomerID".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_function(self):
        # Test parsing of SQL statement
        sql_query = "SELECT COUNT(CustomerID), Country FROM Customers GROUP BY Country;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "CustomerID": hashlib.sha256("CustomerID".encode()).hexdigest(),
            "Country": hashlib.sha256("Country".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_insert(self):
        # Test parsing of SQL statement
        sql_query = "INSERT INTO Customers (CustomerName, ContactName) VALUES ('Cardinal','Tom B. Erichsen');"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "CustomerName": hashlib.sha256("CustomerName".encode()).hexdigest(),
            "ContactName": hashlib.sha256("ContactName".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_update(self):
        # Test parsing of SQL statement
        sql_query = "UPDATE Customers SET ContactName='Alfred Schmidt', City='Frankfurt' WHERE CustomerID=1;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "ContactName": hashlib.sha256("ContactName".encode()).hexdigest(),
            "City": hashlib.sha256("City".encode()).hexdigest(),
            "CustomerID": hashlib.sha256("CustomerID".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_delete(self):
        # Test parsing of SQL statement
        sql_query = "DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "CustomerName": hashlib.sha256("CustomerName".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_parse_statement_for_is_null(self):
        # Test parsing of SQL statement
        sql_query = "SELECT CustomerName, ContactName, Address FROM Customers WHERE Address IS NULL;"
        parsed = sqlparse.parse(sql_query)
        column_name_mapping = {}
        for statement in parsed:
            parse_statement(statement, column_name_mapping, True)

        expected_mapping = {
            "CustomerName": hashlib.sha256("CustomerName".encode()).hexdigest(),
            "ContactName": hashlib.sha256("ContactName".encode()).hexdigest(),
            "Address": hashlib.sha256("Address".encode()).hexdigest(),
        }
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_map_original_hashed_column_name(self):
        original_column_name = "Country"
        column_name_mapping = {}
        expected_mapping = {
            "Country": hashlib.sha256("Country".encode()).hexdigest(),
        }
        map_original_hashed_column_name(
            original_column_name, column_name_mapping)
        self.assertEqual(column_name_mapping, expected_mapping)

    def test_modified_query(self):
        sql_query = "SELECT COUNT(CustomerID), Country FROM Customers GROUP BY Country HAVING COUNT(CustomerID) > 5;"
        map = {
            "CustomerID": hashlib.sha256("CustomerID".encode()).hexdigest(),
            "Country": hashlib.sha256("Country".encode()).hexdigest(),
        }
        expected_modified_sql = "SELECT COUNT(" + hashlib.sha256("CustomerID".encode()).hexdigest() + "), " + hashlib.sha256("Country".encode()).hexdigest(
        ) + " FROM Customers GROUP BY " + hashlib.sha256("Country".encode()).hexdigest() + " HAVING COUNT(" + hashlib.sha256("CustomerID".encode()).hexdigest() + ") > 5;"
        modified_sql = modified_query(sql_query, map)
        self.assertEqual(modified_sql, expected_modified_sql)


if __name__ == "__main__":
    unittest.main()
