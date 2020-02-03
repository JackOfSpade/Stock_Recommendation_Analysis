import pyodbc as po

if __name__ == "__main__":
    for driver  in po.drivers():
        print(driver)

    sqlite_connection_string = """
    driver=SQLite3 ODBC Driver;
    database=finviz.sqlite;
    """
    connection = po.connect(sqlite_connection_string)
    cursor = connection.cursor()
    query = """
    SELECT .....................
    """