class DataQualityQueries:
    @staticmethod
    def table_not_empty(table):
        """
        Tests if a given table has at least one row.
        :param table: Name of the table.
        :return: SQL statement that returns a single column 'result' with
            either the value 0 if the table has at least one entry, otherwise
            it returns 0.
        """
        return """
        SELECT (CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END) as result
        FROM {table:}
        """.format(**dict(table=table))

    @staticmethod
    def table_not_empty_test(res):
        return res['result'][0] == 0

    @staticmethod
    def col_does_not_contain_null(table, col):
        """
        Tests if a given column in a table

        :param table: Name of the table.
        :param col: Name of the column.

        :return: SQL statement that returns a single how many rows are null.
        """
        return """
        SELECT COUNT({col:}) as count
        FROM {table:}
        WHERE "{col:}" is NULL
        """.format(**dict(table=table, col=col))

    @staticmethod
    def col_does_not_contain_null_test(res):
        return res['count'][0] == 0