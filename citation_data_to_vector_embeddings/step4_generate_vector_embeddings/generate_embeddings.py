from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer, util
import torch
from functools import lru_cache
from collections import defaultdict
import pandas as pd
from itertools import islice

spark_path = "/home/shsa3327"
spark = SparkSession.builder\
    .config('spark.driver.memory', '40g')\
    .config('spark.executor.memory', '20g')\
    .config('spark.executor.cores', '30')\
    .config('spark.local.dir', f'{spark_path}/tmp') \
    .config('spark.driver.maxResultSize', '40g')\
    .config("spark.driver.bindAddress", "0.0.0.0")\
    .config("spark.sql.parquet.columnarReaderBatchSize", "1024") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config('spark.driver.extraJavaOptions', f'-Djava.io.tmpdir={spark_path}/tmp') \
    .config('spark.executor.extraJavaOptions', f'-Djava.io.tmpdir={spark_path}/tmp') \
    .config('hive.exec.scratchdir', f'{spark_path}/tmp/hive') \
    .enableHiveSupport() \
    .getOrCreate()


combined_df_with_q_c_metadata = '/home/shsa3327/mypetalibrary/pmoa-cite-dataset/aggregated_dateset/combined_pubmed.parquet'
combined_df_read = spark.read.parquet(combined_df_with_q_c_metadata)
# combined_df_read.count()

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)
@lru_cache(maxsize=None)
def cosine_similarity(feature1, feature2):
    # model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    feature1_embedding = model.encode(feature1)
    feature2_embedding = model.encode(feature2)
    cosine_similarity = util.cos_sim(feature1_embedding, feature2_embedding)
    return cosine_similarity.item()


grouped_records = defaultdict(list)
collected_data = combined_df_read.collect()
for record in collected_data:
    grouped_records[record['pmid']].append(record)
print(f"Total groups: {len(grouped_records)}")


import numpy as np
from tqdm import tqdm
import time
embedded_groups = []
cosine_pairs = [
    ('q_title','c_title'),
    ('q_title','c_abstract'),
    ('q_abstract','c_title'),
    ('q_abstract','c_abstract'),
    ('sentence','c_abstract'),
    ('sentence','c_title')
]
'''
year_difference = q_year-c_year
len_c_title = len(c_title)
len_c_abstract = len(c_abstract)
log_c_in_citations = np.log2(c_in_citations)
'''
grouped_records = dict(islice(grouped_records.items(), 1000))
start = time.perf_counter()
for k,v in tqdm(grouped_records.items()):
    for record in v:
        embedding_arr = []
        record = record.asDict()
        for a,b in cosine_pairs:
            if not record.get(a):
                record[a]=''
            if not record.get(b):
                record[b]=''
            embedding_arr.append(cosine_similarity(record.get(a,''),record.get(b,'')))

        if not record.get('c_publication_year'):
            record['c_publication_year']=0
        if not record.get('q_publication_year'):
            record['q_publication_year']=0    
        embedding_arr.append(record.get('q_publication_year')-record.get('c_publication_year'))
        embedding_arr.append(len(record.get('q_title','')))
        embedding_arr.append(len(record.get('c_title','')))
        embedding_arr.append(len(record.get('q_abstract','')))
        embedding_arr.append(len(record.get('c_abstract','')))
        if record.get('c_in_citations'):
            embedding_arr.append(np.log2(record.get('c_in_citations')))
        else:
            embedding_arr.append(0)
        embedding_arr.append(record.get('relevance_score'))
        embedded_groups.append([k] + embedding_arr)
end = time.perf_counter()
print(f"Time taken to create embeddings for {len(embedded_groups)} groups: {end-start} seconds")


df = spark.createDataFrame(pd.DataFrame(embedded_groups))
df.write.parquet("data.parquet")