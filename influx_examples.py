from influxdb import DataFrameClient
import influxdb
import pandas as pd
from timeit import default_timer as timer

host = '0.0.0.0'
port = 8086
user = 'admin'
password = ''
test_db = 'test'
iroc_db = 'iroc_test'  # This has to match, of course, the name of the database the  data was put into


def create_db_add_points_example(num_points, batch_size):
    client = DataFrameClient(host, port, user, password, test_db)
    x = pd.DatetimeIndex(start=pd.Timestamp("2010-01-01"), periods=num_points, freq="S")
    y = pd.DataFrame(index=x, data=pd.np.random.randn(num_points, 1))
    client.create_database(test_db)
    client.write_points(y, "perf-test", batch_size=batch_size, protocol='line')
    client.drop_database(test_db)


def example_run_write_test():
    print("InfluxDB python version: {}".format(influxdb.__version__))
    num_points_list = [1000, 10000, 100000]
    batch_size = 10000
    for num_points in num_points_list:
        print("Number of points: {}".format(num_points))
        start = timer()
        create_db_add_points_example(num_points, batch_size)
        end = timer()
        elapsed = end - start
        print("Time: {}".format(elapsed))


def example_read_iroc_data():
    client = DataFrameClient(host, port, user, password, iroc_db)
    get_well_list = 'SHOW TAG VALUES WITH KEY = "well"'
    res = client.query(get_well_list)


    # fifth element in list, fifth well, you can also iterate over this with: for element in res['iroc_test']:
    all_wells_as_list = list(res[iroc_db])
    fifth_well_result = all_wells_as_list[4]
    wellname = fifth_well_result['value']
    print("Print fifth wellname in list")
    print(wellname)

    # Find all tags for a given well (wellname)
    get_tags_for_well = 'SHOW TAG VALUES WITH KEY = "tag" WHERE well =~ /{}/'.format(wellname)
    tag_result = client.query(get_tags_for_well)
    all_tags_as_list = list(tag_result[iroc_db])
    first_tag = all_tags_as_list[0]['value']
    print("Number of tags (sensors) for well {} is {}".format(wellname, len(all_tags_as_list)))

    # Now, give me the dataframe for this tag. In this case, the tag is unique, but if not,
    # you could combine several tags (well, sensor, facility...)
    # In the query, you can select mean, or omit it. If you omit, you'll get raw values. If you use mean() you also need
    # a group by time(), to tell the interval.
    # This query fetch all data, but you can also use "AND time >= from_time" type syntax (see grafana query inspector)
    data_query = 'SELECT mean("value") as "{}" FROM {} WHERE ("tag" =~ /^{}$/) GROUP BY time(10m) fill(previous)'.\
        format(first_tag, iroc_db, first_tag)
    data_result = client.query(data_query)

    dataframe = list(data_result.values())[0]
    print(dataframe.describe())
    print(dataframe.head())


if __name__ == '__main__':
    example_run_write_test()
    example_read_iroc_data()