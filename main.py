import pyodbc as po
import pandas as pd
import numpy as np
import analyst
import matplotlib.pyplot as plt

# Find the analyst object if it exists or else create one and return it.
def find_analyst(name, analyst_object_list):
    # Do not create analyst object if it exists already.
    for x in analyst_object_list:
        if isinstance(x, analyst.analyst) and x.name == name:
            return x

    analyst_object = analyst.analyst(name)

    # Must have at least 10 recommendations
    if analyst_object.total_recommendations >= 10:
        analyst_object_list.append(analyst_object)
        return analyst_object
    else:
        return None

# Plot horizontal bar graphs
def plot_barh(figure, axes, analyst_object_list, number_of_months):
    axes.clear()
    if number_of_months == 1:
        axes.set_title("Top Average RoR in 1 Month")
    else:
        axes.set_title("Top Average RoR in " + str(number_of_months) + " Months")

    if number_of_months == 1:
        analyst_object_list.sort(key=lambda x: x.twenty_business_days_average_rate_of_return, reverse=True)
    elif number_of_months == 2:
        analyst_object_list.sort(key=lambda x: x.forty_business_days_average_rate_of_return, reverse=True)
    else:
        analyst_object_list.sort(key=lambda x: x.sixty_business_days_average_rate_of_return, reverse=True)

    average_rate_of_return = []
    names = []
    for x in analyst_object_list:
        names.append(x.name)
        if number_of_months == 1:
            average_rate_of_return.append(x.twenty_business_days_average_rate_of_return)
        elif number_of_months == 2:
            average_rate_of_return.append(x.forty_business_days_average_rate_of_return)
        else:
            average_rate_of_return.append(x.sixty_business_days_average_rate_of_return)


    axes.barh(y=np.arange(start=len(analyst_object_list), stop=0, step=-1), width=average_rate_of_return)
    axes.set_yticks(np.arange(start=len(analyst_object_list), stop=0, step=-1))
    axes.set_yticklabels(names)
    axes.set_xlabel("Average Rate of Return")
    figure.savefig(fname="barh_graph"+ str(number_of_months) +".png",  bbox_inches="tight")

# Graph top 9 analysts according to average rate of return for all periods
def graph_top_9(analyst_object_list):
    analyst_object_list.sort(key=get_average_rate_of_return_for_all_periods, reverse=True)
    analyst_object_list = analyst_object_list[0:10]

    figure, axes = plt.subplots(nrows=1, ncols=1)
    axes.set_title("Top Average RoR Over Time")
    for x in analyst_object_list:
        axes.plot([1, 2, 3], [x.twenty_business_days_average_rate_of_return, x.forty_business_days_average_rate_of_return, x.sixty_business_days_average_rate_of_return], marker=".", linestyle="-", label=x.name)

    axes.set_ylabel("Average Rate of Return")
    axes.set_xlabel("Number of Months")
    legend = axes.legend(bbox_to_anchor=(1.1, 1))
    figure.savefig(fname="scatter_plot.png", bbox_extra_artists=(legend,), bbox_inches="tight")

    plot_barh(figure, axes, analyst_object_list, 1)
    plot_barh(figure, axes, analyst_object_list, 2)
    plot_barh(figure, axes, analyst_object_list, 3)

def get_average_rate_of_return_for_all_periods(analyst_object):
    return analyst_object.average_rate_of_return_for_all_periods

def main():
    # Connect to SQLLite file.
    sqlite_connection_string = """
        driver=SQLite3 ODBC Driver;
        database=finviz.sqlite;
        """
    connection = po.connect(sqlite_connection_string)

    recommendations_query = """
                    SELECT data_date, ticker, analyst, updown
                    FROM recommendations
                    WHERE (updown = 'Upgrade' OR updown = 'Downgrade') AND data_date >= '2018-07-18'
                    ORDER BY data_date ASC;
                    """

    # Small Sample Test
    # recommendations_query = """
    #                 SELECT data_date, ticker, analyst, updown
    #                 FROM recommendations
    #                 WHERE (updown = 'Upgrade' OR updown = 'Downgrade') AND data_date >= '2018-07-18' AND analyst = 'Cantor Fitzgerald'
    #                 ORDER BY data_date ASC;
    #                 """

    # Stores analyst obejects, each corresponding to an analyst
    analyst_object_list = []

    # Get total # of recommendations for progress report.
    cursor = connection.cursor()
    count_query = """
                            SELECT COUNT(*) AS COUNT
                            FROM recommendations
                            WHERE (updown = 'Upgrade' OR updown = 'Downgrade') AND data_date >= '2018-07-18';
                            """

    # Small Sample Test
    # count_query = """
    #                         SELECT COUNT(*) AS COUNT
    #                         FROM recommendations
    #                         WHERE (updown = 'Upgrade' OR updown = 'Downgrade') AND data_date >= '2018-07-18' AND analyst = 'Cantor Fitzgerald';
    #                         """

    cursor.execute(count_query)
    total_recommendations = cursor.fetchval()

    chunksize = 500
    iteration = 0

    # For each chunksize recommendations:
    for df in pd.read_sql(sql=recommendations_query, con=connection, chunksize=chunksize):
        iteration += 1

        # For each recommendation out of those chunksize recommendations:
        for (index_label, row_series) in df.iterrows():
            # Find the analyst object if it exists or else create one and return it.
            analyst_object = find_analyst(row_series["analyst"], analyst_object_list)

            # A return of None means this analyst has less than 10 recommendations. Discard.
            if analyst_object is not None:
                previous_existing_day_query = """
                                            SELECT data_date
                                            FROM v_fundamentals 
                                            WHERE data_date <= ? AND ticker = ? AND price IS NOT NULL
                                            ORDER BY data_date DESC
                                            LIMIT 1;
                                             """

                cursor.execute(previous_existing_day_query, (row_series["data_date"], row_series["ticker"]))
                previous_existing_day_date = cursor.fetchval()

                # Use previous business day's price if analyst post recommendation on a non-business day.
                if previous_existing_day_date is not None:
                    stock_query = """
                        SELECT price
                        FROM v_fundamentals
                        WHERE data_date >= ? AND ticker = ? AND price IS NOT NULL
                        ORDER BY data_date ASC;
                        """

                    df_61 = next(
                        pd.read_sql(sql=stock_query, params=(previous_existing_day_date, row_series["ticker"]), con=connection,
                                    chunksize=61))

                    # Discard if recommendation is too recent or too old
                    if len(df_61) == 61:
                        current_price = df_61.loc[0].copy()["price"]
                        business_day_20_price = df_61.loc[20].copy()["price"]
                        business_day_40_price = df_61.loc[40].copy()["price"]
                        business_day_60_price = df_61.loc[60].copy()["price"]

                        # Calculate rate of return for each time period
                        if row_series["updown"] == "Upgrade":
                            twenty_business_days_rate_of_return = (business_day_20_price - current_price) / current_price
                            forty_business_days_rate_of_return = (business_day_40_price - current_price) / current_price
                            sixth_business_days__rate_of_return = (business_day_60_price - current_price) / current_price
                        elif row_series["updown"] == "Downgrade":
                            twenty_business_days_rate_of_return = - (business_day_20_price - current_price) / current_price
                            forty_business_days_rate_of_return = - (business_day_40_price - current_price) / current_price
                            sixth_business_days__rate_of_return = - (business_day_60_price - current_price) / current_price

                        # Add average rate of return to this analyst's object
                        analyst_object.add_gross_profits_as_averaged_profits((twenty_business_days_rate_of_return,
                                                                              forty_business_days_rate_of_return,
                                                                              sixth_business_days__rate_of_return))

                        # Test
                        # print(str(previous_existing_day_date) + ", " + str(row_series["ticker"]) + ", " +  str(current_price) + ", " + str(business_day_20_price) + ", " + str(business_day_40_price) + ", " + str(business_day_60_price))

        # Get progress report in console
        percent_complete = np.round(iteration * chunksize / total_recommendations * 100, decimals=2)
        if percent_complete > 100:
            percent_complete = 100
        print(str(percent_complete) + "%")

        # Small Sample Test
        # if iteration == 1:
        #     break

    for x in analyst_object_list:
        x.calculate_average_rate_of_return_for_all_periods()

    graph_top_9(analyst_object_list)

# Only run code below if it's not being used as a module.
if __name__ == "__main__":
    main()

    # Performance Tests
    # import timeit
    # print(timeit.timeit(stmt="main()", setup="from __main__ import main", number=2)/2)

    # import cProfile
    # import pstats
    # cProfile.run("main()", "profiler_data")
    # p = pstats.Stats("profiler_data")
    # p.strip_dirs().sort_stats("cumtime").print_stats()

