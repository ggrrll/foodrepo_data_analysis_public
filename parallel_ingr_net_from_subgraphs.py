
# import useful modules
import pandas as pd
import numpy as np
import re

# modules for network analysis
import itertools as itt
import multiprocessing
import networkx as nx


# custom function
def my_graph_parallel(chunk_df):
    
    
    # create the main graph 		
    my_subgraph = nx.Graph()	

    # for row in ingredients_df.loc[:100,:].itertuples():
    for row in chunk_df.itertuples():


        partial_list = row.ingredients.strip('[').strip(']').split(',') 
        new_list = [re.sub(r'\s*\'\s*','',i.strip()) for i in partial_list]

        # create fully connected sub-graph for each publication        
        sub_graph = nx.complete_graph(len(new_list))
        nx.relabel_nodes(sub_graph,dict(enumerate(new_list)),copy=False)
        
        # add new graph to main one         
        my_subgraph = nx.compose(my_subgraph,sub_graph)


    return my_subgraph	


# get and clean dataset
ingredients_df = pd.DataFrame.from_csv('./important_datasets/ingredients_df.csv')


# define the pool
pool_size  = multiprocessing.cpu_count()
pool = multiprocessing.Pool(pool_size)
print("we are using, max N threads on cpu =",pool_size) 


# calculate the chunk size as an integer
chunk_size = int(ingredients_df.shape[0] / pool_size)

# split original df in chuncks 
chunks = [ingredients_df.ix[ingredients_df.index[i:i + chunk_size]] for i in range(0, ingredients_df.shape[0], chunk_size)]


# run the parallel loop!
subgraph_list =  pool.map(my_graph_parallel,chunks)

# make new empty graph
ingredients_graph = nx.Graph()	
# merge n = pool_size subgraphs
for g in subgraph_list:
	ingredients_graph = nx.compose(ingredients_graph,g)



# save the final GRAPH!
nx.write_gpickle(ingredients_graph,'./important_datasets/ingredients_graph')

# on my mac, with 8 threads (4 cpus) took:
# real    4m30.237s
# user    24m52.829s
# sys 0m4.291s

# 6/4/2016 - after improved ingredients parsing
# real    5m32.328s
# user    31m39.960s
# sys 0m5.070s