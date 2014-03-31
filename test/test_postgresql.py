import unittest

from postgresql import Database

NAME = 'test_postgresql'

class DatabaseTests(unittest.TestCase):
    # A basic level of functionality is required
    # for setup/teardown, but it makes this much nicer.

    @classmethod
    def setUpClass(cls):
        cls.postgres = Database(name='postgres')

        try:
            cls.postgres('CREATE DATABASE ' + NAME)
        except cls.postgres.ExecutionError:
            cls.postgres('DROP DATABASE ' + NAME)
            cls.postgres('CREATE DATABASE ' + NAME)

    def test_host(self):
        self.assertIs(self.postgres.host, None)

    def test_name(self):
        self.assertIsInstance(self.postgres.name, str)

    def test_user(self):
        self.assertIsInstance(self.postgres.user, str)

    def test_call_text(self):
        db = Database(name=NAME)
        db('CREATE TABLE season ('
           ' name TEXT'
           ');')

        db('INSERT INTO season VALUES (\'Spring\')')
        db('INSERT INTO season VALUES (\'Summer\')')
        db('INSERT INTO season VALUES (\'Fall\')')
        db('INSERT INTO season VALUES (\'Winter\')')

        result = db('SELECT * FROM season')

        self.assertEqual(len(result), 4)

        for row in result:
            self.assertEqual(len(row), 1)
            self.assertIsInstance(row[0], str)

    def test_integer_and_bool(self):
        db = Database(name=NAME)
        db('CREATE TABLE numbers ('
           ' value INTEGER,'
           ' prime BOOL'
           ');')

        db('INSERT INTO numbers VALUES (2, TRUE)')
        db('INSERT INTO numbers VALUES (3, TRUE)')
        db('INSERT INTO numbers VALUES (4, FALSE)')
        db('INSERT INTO numbers VALUES (5, TRUE)')
        db('INSERT INTO numbers VALUES (6, FALSE)')
        db('INSERT INTO numbers VALUES (7, TRUE)')

        result = db('SELECT * FROM numbers ORDER BY value')

        self.assertEqual(len(result), 6)

        self.assertEqual(result[0][0], 2)
        self.assertIs(   result[0][1], True)

        self.assertEqual(result[1][0], 3)
        self.assertIs(   result[1][1], True)

        self.assertEqual(result[2][0], 4)
        self.assertIs(   result[2][1], False)

        self.assertEqual(result[3][0], 5)
        self.assertIs(   result[3][1], True)

        self.assertEqual(result[4][0], 6)
        self.assertIs(   result[4][1], False)

        self.assertEqual(result[5][0], 7)
        self.assertIs(   result[5][1], True)

    def test_text(self):
        db = Database(name=NAME)
        db('CREATE TABLE test_text ('
           ' x TEXT'
           ');')

        STRINGS = ('abc', '123')

        for string in STRINGS:
            db('INSERT INTO test_text VALUES ($1)', string)

        result = db('SELECT * FROM test_text')

        self.assertEqual(len(result), len(STRINGS))

        for row in result:
            self.assertIn(row[0], STRINGS)

    def test_int2(self):
        db = Database(name=NAME)
        db('CREATE TABLE test_int2 ('
           ' x INT2'
           ');')

        for x in (1, 0, -1):
            db('INSERT INTO test_int2 VALUES ($1)', x)
            result = db('SELECT * FROM test_int2')

            self.assertEqual(len(result), 1)

            y = result[0][0]

            self.assertIsInstance(y, int)
            self.assertEqual(x, y)

            db('DELETE FROM test_int2')

    def test_int4_positive(self):
        db = Database(name=NAME)
        db('CREATE TABLE test_int4_positive ('
           ' x INT4'
           ');')

        for x in (2**17, 2**22):
            db('INSERT INTO test_int4_positive VALUES ($1)', x)
            result = db('SELECT * FROM test_int4_positive')

            self.assertEqual(len(result), 1)

            y = result[0][0]

            self.assertIsInstance(y, int)
            self.assertEqual(x, y)

            db('DELETE FROM test_int4_positive')

    def test_int4_negative(self):
        db = Database(name=NAME)
        db('CREATE TABLE test_int4_negative ('
           ' x INT4'
           ');')

        for x in (-2**17, -2**22):
            db('INSERT INTO test_int4_negative VALUES ($1)', x)
            result = db('SELECT * FROM test_int4_negative')

            self.assertEqual(len(result), 1)

            y = result[0][0]

            self.assertIsInstance(y, int)
            self.assertEqual(x, y)

            db('DELETE FROM test_int4_negative')

    def test_int8_positive(self):
        db = Database(name=NAME)
        db('CREATE TABLE test_int8_positive ('
           ' x INT8'
           ');')

        for x in (2**42, 2**50):
            db('INSERT INTO test_int8_positive VALUES ($1)', x)
            result = db('SELECT * FROM test_int8_positive')

            self.assertEqual(len(result), 1)

            y = result[0][0]

            self.assertIsInstance(y, int)
            self.assertEqual(x, y)

            db('DELETE FROM test_int8_positive')

    def test_int8_negative(self):
        db = Database(name=NAME)
        db('CREATE TABLE test_int8_negative ('
           ' x INT8'
           ');')

        for x in (-2**42, -2**50):
            db('INSERT INTO test_int8_negative VALUES ($1)', x)
            result = db('SELECT * FROM test_int8_negative')

            self.assertEqual(len(result), 1)

            y = result[0][0]

            self.assertIsInstance(y, int)
            self.assertEqual(x, y)

            db('DELETE FROM test_int8_negative')

    @unittest.expectedFailure
    def test_row_equal(self):
        db = Database(name=NAME)
        db('CREATE TABLE asdf ('
           ' x INTEGER,'
           ' y INTEGER,'
           ' z INTEGER'
           ');')

        db('INSERT INTO asdf VALUES (1,2,3)')

        # Implementation detail:
        # whether indexing a result caches the row
        # (currently it does not)

        row1 = db('SELECT * FROM asdf')[0]
        row2 = db('SELECT * FROM asdf')[0]

        self.assertEqual(row1, row2)

    @unittest.expectedFailure
    def test_row_hash(self):
        db = Database(name=NAME)
        db('CREATE TABLE test_row_hash ('
           ' x INTEGER,'
           ' y INTEGER,'
           ' z INTEGER'
           ');')

        db('INSERT INTO test_row_hash VALUES (1,2,3)')

        # Implementation detail:
        # whether indexing a result caches the row
        # (currently it does not)

        row1 = db('SELECT * FROM test_row_hash')[0]
        row2 = db('SELECT * FROM test_row_hash')[0]

        d = {}

        d[row1] = True

        self.assertIn(d, row2)
