class IndexManager(object):
    def __init__(self, sdf, df_id):
        self.index_rdd = None
        self.sdf = sdf
        self.df_id = df_id
        self.df_min = None
        self.df_max = None
        self.df_sum = None

    # def __del__(self):
    #     self.drop_index()

    def create_index(self, index_col):
        columns = self.sdf.columns
        df_id = self.df_id

        def f(split_index, iterator):
            import pandas as pd
            import pyarrow as pa
            from pyarrow import plasma

            client = plasma.connect("/tmp/plasma", "", 0)
            pdf = pd.DataFrame.from_records(list(iterator), columns=columns)
            record_batch = pa.RecordBatch.from_pandas(pdf)

            import time
            timeStamp = str(time.time()).replace('.', str(split_index)) * 3
            object_id = plasma.ObjectID(bytes(timeStamp[20:40], encoding="utf8"))
            # object_id = plasma.ObjectID(np.random.bytes(20))
            if client.contains(object_id):
                client.delete([object_id])

            mock_sink = pa.MockOutputStream()
            stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
            stream_writer.write_batch(record_batch)
            stream_writer.close()
            data_size = mock_sink.size()
            buf = client.create(object_id, data_size)

            # Write the PyArrow RecordBatch to Plasma
            stream = pa.FixedSizeBufferWriter(buf)
            stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
            stream_writer.write_batch(record_batch)
            stream_writer.close()

            # Seal the Plasma object
            client.seal(object_id)
            yield object_id

        from pyspark.storagelevel import StorageLevel
        self.index_rdd = self.sdf.rdd.mapPartitionsWithIndex(f).persist(storageLevel=StorageLevel.MEMORY_ONLY)
        self.index_rdd.collect()
        # self.index_rdd.persist(storageLevel=StorageLevel(False, True, False, False)))

    def drop_index(self):   # 析构时 调用，以保证plasma store 不浪费
        if self.index_rdd is None:
            return

        rdd = self.index_rdd

        def f(iterator):
            object_id = next(iterator)
            from pyarrow import plasma
            client = plasma.connect("/tmp/plasma", "", 0)
            print("connect plasma successfully")
            if not client.contains(object_id):
                raise Exception("The node does not contain object_id of ", str(object_id))
            else:
                client.delete([object_id])
            return []

        rdd.mapPartitions(f).collect()
        self.index_rdd.unpersist()
        self.index_rdd = None

    def get_aggregation(self):
        def f3(split_index, iter):
            object_id = next(iter)
            from pyarrow import plasma
            import pyarrow as pa
            client = plasma.connect("/tmp/plasma", "", 0)

            if not client.contains(object_id):
                raise Exception("The node does not contain object_id of ", str(object_id))

            # Fetch the Plasma object
            [data] = client.get_buffers([object_id])  # Get PlasmaBuffer from ObjectID
            buffer = pa.BufferReader(data)

            # Convert object back into an Arrow RecordBatch
            reader = pa.RecordBatchStreamReader(buffer)
            record_batch = reader.read_next_batch()

            # Convert back into Pandas
            pdf = record_batch.to_pandas()

            max_series = pdf.max(numeric_only=True).to_dict()
            max_series['type'] = 'max'

            min_series = pdf.min(numeric_only=True).to_dict()
            min_series['type'] = 'min'

            sum_series = pdf.sum(numeric_only=True).to_dict()
            sum_series['type'] = 'sum'

            res = [max_series, min_series, sum_series]

            from pyspark.sql.types import Row

            def to_row(row_data):
                row = Row(*row_data)
                row = row(*row_data.values())
                return row

            res = list(map(to_row, res))
            return res

        import pandas as pd
        res = self.index_rdd.mapPartitionsWithIndex(f3).collect()
        columns = self.sdf.columns
        columns.append('type')

        if res is not []:
            pdf = pd.DataFrame(res, columns=res[0].asDict().keys())

            df_min = pdf[pdf['type'] == 'min']
            df_max = pdf[pdf['type'] == 'max']
            df_sum = pdf[pdf['type'] == 'sum']

            self.df_min = df_min.min(numeric_only=True)
            self.df_max = df_max.max(numeric_only=True)
            self.df_sum = df_sum.sum(numeric_only=True)
