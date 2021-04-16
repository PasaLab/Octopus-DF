import dask.dataframe as dd
from dask.distributed import Client

__all__ = ['client', 'dd']

print("*************************************************")
print("************ Loading Dask Environment **********")
print("*************************************************")

client = Client(n_workers=4, threads_per_worker=24, processes=True, memory_limit="25GB")
