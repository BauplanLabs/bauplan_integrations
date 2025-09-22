import bauplan

@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.10', pip={'duckdb': '1.2.0'})
def ny_taxi_trips_and_zones(
        zones = bauplan.Model(
            'taxi_zones'
        ),
        trips=bauplan.Model(
            'taxi_fhvhv',
            columns=[
                'PULocationID',
                'trip_time',
                'trip_miles',
                'base_passenger_fare',
                'pickup_datetime'
            ],
            filter="pickup_datetime >= '2022-12-15T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'"

        )
):
    """
    Joins NYC taxi trip records with zone metadata using DuckDB, and logs dataset size.
    """
    import duckdb

    joined_table = duckdb.sql("""
        SELECT * 
        FROM trips
        JOIN zones ON trips.PULocationID = zones.LocationID
    """).arrow()

    size_in_gb = joined_table.nbytes / (1024 ** 3)
    print(f'\n number of rows {joined_table.num_rows}\n')
    print(f'\n size in GB {size_in_gb} \n')

    return joined_table


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.10', pip={'pandas': '2.2.0'})
def top_pickup_locations(data=bauplan.Model('ny_taxi_trips_and_zones')):
    """
    Computes the most popular NYC taxi pickup locations by aggregating trip counts.
    """
    import pandas as pd

    df = data.to_pandas()
    top_pickup_table = (
        df
        .groupby(['PULocationID', 'Borough', 'Zone'])
        .agg(number_of_trips=('pickup_datetime', 'count'))
        .reset_index()
        .sort_values(by='number_of_trips', ascending=False)
    )

    return top_pickup_table