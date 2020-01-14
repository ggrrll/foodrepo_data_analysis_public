
##### ##### ##### ##### ##### module load ##### ##### ##### ##### 

# basic python modules 
import pandas as pd
import numpy as np

# modules used here for network analysis/manipulation
import itertools as itt
import multiprocessing
import networkx as nx
import pickle

# ##### ##### ##### #####  START HERE ##### ##### ##### ##### ##### 

# get dataset
food_df = pd.DataFrame.from_csv('./important_datasets/ingredients_df.csv')


# define dict for graph nodes - only FIRST 1000!
products_dict = dict.fromkeys(food_df.barcode.unique()[:1000])

# custom function to clear ingredients list
def str_to_list(my_string):

    return [e.strip('[').strip(']').strip().strip('\'') for e in my_string.split(',')]

# custom function to add edges and weights
def my_loop(c):
    
    c0_ingrs = str_to_list(food_df[food_df.barcode==c[0]].ingredients.values[0])
    c1_ingrs = str_to_list(food_df[food_df.barcode==c[1]].ingredients.values[0])

    n_shared_ingrs = len(set(c0_ingrs) & set(c1_ingrs))
    
    if n_shared_ingrs!=0:

        
        return (c[0],c[1],{'weight':n_shared_ingrs})


# define the pool
pool_size  = multiprocessing.cpu_count()
pool = multiprocessing.Pool(pool_size)
print("we are using, max N threads on cpu =",pool_size) 

# running the parallel loop!
edge_list =  pool.map(my_loop,itt.combinations(products_dict.keys(),2))


# save edge_list
with open('products_edges.txt', 'wb') as fp:
    pickle.dump(edge_list, fp)


# run with:
# --->  time python3 parallel_products_net.py <---


#  TIMING:

# (on my 4 cpus)
# real    7m30.233s
# user    57m40.162s
# sys 0m13.885s


# #### ##### #####  OTHER CODE ##### ##### ##### 

# define global vars for parallel
# my_manager = multiprocessing.Manager()
# glob_dic = my_manager.dict(nodes_dict)

# define graph
# runner_graph = nx.Graph() # define graph
# runner_graph.add_nodes_from(nodes_dict.keys()) # add nodes

# save the GRAPH!
# nx.write_gpickle(runner_graph,'runner_graph')
