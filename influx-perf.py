import influxdb
from influxdb import DataFrameClient
import pandas as pd
from timeit import default_timer as timer

host = '0.0.0.0'
port = 8086
user = 'admin'
password = ''
db_name = 'test'

def simple_test(num_points, batch_size):
    client = DataFrameClient(host, port, user, password, db_name)
    x = pd.DatetimeIndex(start=pd.Timestamp("2010-01-01"), periods=num_points, freq="S")
    y = pd.DataFrame(index=x, data=pd.np.random.randn(num_points, 1))

    client.create_database(db_name)
    client.write_points(y, "perf-test", batch_size=batch_size, protocol='line')
    client.drop_database(db_name)

if __name__=='__main__':
    print("InfluxDB python version: {}".format(influxdb.__version__))
    num_points_list = [10000, 100000, 1000000]
    batch_size = 10000
    for num_points in num_points_list:
        print("Number of points: {}".format(num_points))
        start = timer()
        simple_test(num_points, batch_size)
        end = timer()
        elapsed = end - start
        print("Time: {}".format(elapsed))
