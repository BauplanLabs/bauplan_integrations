import bauplan


# models.py
@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'pandas': '2.2.0'})
def filtered_taxi_rides_parameters(
        data=bauplan.Model(
            "taxi_fhvhv",
            columns=['pickup_datetime'],
            filter="pickup_datetime >= $start_time"
        ),
):
    data = data.to_pandas()

    print(f"\nEarliest pickup in result:, {data['pickup_datetime'].min()}")
    print(f"\nRows returned:, {len(data)}\n")

    return data

