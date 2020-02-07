import pandas as pd
import pyodbc as po

class analyst():
    def __init__(self, name):
        self.name = str(name);
        self.twenty_business_days_average_rate_of_return = 0
        self.forty_business_days_average_rate_of_return = 0
        self.sixty_business_days_average_rate_of_return = 0

        sqlite_connection_string = """
            driver=SQLite3 ODBC Driver;
            database=finviz.sqlite;
            """
        connection = po.connect(sqlite_connection_string)
        cursor = connection.cursor()
        count_query = """
                    SELECT COUNT(*) AS COUNT
                    FROM recommendations
                    WHERE (updown = 'Upgrade ' OR updown = 'Downgrade') AND data_date >= '2018-07-18' AND analyst = ?;
                    """
        cursor.execute(count_query, (self.name,))

        self.total_recommendations = cursor.fetchval()
        self.average_rate_of_return_for_all_periods = 0

    def add_gross_profits_as_averaged_profits(self, rate_of_return_tuple):
        self.twenty_business_days_average_rate_of_return += rate_of_return_tuple[0] / self.total_recommendations
        self.forty_business_days_average_rate_of_return += rate_of_return_tuple[1] / self.total_recommendations
        self.sixty_business_days_average_rate_of_return += rate_of_return_tuple[2] / self.total_recommendations

    def calculate_average_rate_of_return_for_all_periods(self):
        self.average_rate_of_return_for_all_periods = (self.twenty_business_days_average_rate_of_return + self.forty_business_days_average_rate_of_return + self.sixty_business_days_average_rate_of_return) / 3

    def __lt__(self, other):
        if self.average_rate_of_return_for_all_periods < other.average_rate_of_return_for_all_periods:
            return True
        else:
            return False

    def __le__(self, other):
        if self.average_rate_of_return_for_all_periods <= other.average_rate_of_return_for_all_periods:
            return True
        else:
            return False

    def __gt__(self, other):
        if self.average_rate_of_return_for_all_periods > other.average_rate_of_return_for_all_periods:
            return True
        else:
            return False

    def __ge__(self, other):
        if self.average_rate_of_return_for_all_periods >= other.average_rate_of_return_for_all_periods:
            return True
        else:
            return False