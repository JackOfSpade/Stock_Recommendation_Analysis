import pyodbc as po
import pandas as pd
import numpy as np
import analyst

# Find the analyst object if it exists or else create one and return it.
def find_analyst(name, analyst_object_list):
    for x in analyst_object_list:
        if isinstance(x, analyst.analyst) and x.name == "name":
            return x

    analyst_object = analyst.analyst(name)
    analyst_object_list.append(analyst_object)
    return analyst_object

# Only run code below if it's not being used as a module.
if __name__ == "__main__":
    # Connect to SQLLite file.
    sqlite_connection_string = """
    driver=SQLite3 ODBC Driver;
    database=finviz.sqlite;
    """
    connection = po.connect(sqlite_connection_string)

    recommendations_query = """
            SELECT a.data_date, a.ticker, a.price, b.analyst, b.updown
            FROM v_fundamentals AS a JOIN recommendations AS b ON (a.ticker = b.ticker AND a.data_date = b.data_date)
            WHERE updown = 'Upgrade ' OR updown = 'Downgrade'
            ORDER BY a.data_date ASC;
            """

    # Stores analyst obejects, each corresonding to an analyst
    analyst_object_list = []

    # Get total # of recommendations for progress report.
    sqlite_connection_string = """
                driver=SQLite3 ODBC Driver;
                database=finviz.sqlite;
                """
    connection = po.connect(sqlite_connection_string)
    cursor = connection.cursor()
    count_query = """
                    SELECT COUNT(*) AS COUNT
                    FROM recommendations
                     """
    cursor.execute(count_query)
    total_recommendations = cursor.fetchval()
    chunksize = 1000

    # TEST WITH CHUNKSIZE 10------------------------
    # For each 1000 recommendations:
    for df in pd.read_sql(sql = recommendations_query, con=connection, chunksize=chunksize):
        # For each recommendation out of those 1000 recommendations:
        for (index_label, row_series) in df.iterrows():
            # Find the analyst object if it exists or else create one and return it.
            analyst_object = find_analyst(row_series["analyst"], analyst_object_list)
            print(analyst_object)
            print(type(analyst_object_list[0]))

            stock_query = """
            SELECT price
            FROM v_fundamentals
            WHERE data_date >= ? AND ticker = ?
            ORDER BY data_date ASC
            """

            df_60 = next(pd.read_sql(sql=stock_query, params=(row_series["data_date"], row_series["ticker"]), con=connection, chunksize=60))
            current_price = row_series["price"]
            business_day_20_price = df_60.loc[19].copy()["price"]
            business_day_40_price = df_60.loc[39].copy()["price"]
            business_day_60_price = df_60.loc[59].copy()["price"]


            # Calculate rate of return for each time period
            if row_series["updown"] == "Upgrade":
                 twenty_business_days_rate_of_return = (business_day_20_price - current_price) / business_day_20_price
                 forty_business_days_rate_of_return = (business_day_40_price - current_price) / business_day_40_price
                 sixth_business_days__rate_of_return = (business_day_60_price - current_price) / business_day_60_price
            elif row_series["updown"] == "Downgrade":
                twenty_business_days_rate_of_return = - (business_day_20_price - current_price) / business_day_20_price
                forty_business_days_rate_of_return = - (business_day_40_price - current_price) / business_day_40_price
                sixth_business_days__rate_of_return = - (business_day_60_price - current_price) / business_day_60_price

            # Add average rate of return to this analyst's object
            analyst_object.add_gross_profits_as_averaged_profits((twenty_business_days_rate_of_return, forty_business_days_rate_of_return, sixth_business_days__rate_of_return))
            pass

        # Get progress report
        percent_complete = np.round(chunksize / total_recommendations * 100)
        if percent_complete > 100:
            percent_complete = 100
        print(percent_complete + "%")


