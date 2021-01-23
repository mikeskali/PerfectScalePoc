#!/usr/bin/env python
# coding: utf-8

# In[1]:


from ortools.linear_solver import pywraplp
from dataclasses import dataclass
import pandas as pd   
from datetime import datetime
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import concurrent


# ## Load Pods
# 

# In[2]:


pods_data_src = pd.read_csv("DEV_10_dec - pods.csv")

pods_data_src['req_cpu_milli_core'] = pd.to_numeric(pods_data_src['req_cpu_milli_core'])

pods_data_src['req_mem_mb'] = pd.to_numeric(pods_data_src['req_mem_byte']) / 1000000


pods_data = pods_data_src[(pods_data_src.node_group == 3) &  
                          (pods_data_src.owner_kind != 'DaemonSet') & 
                          (pods_data_src.req_cpu_milli_core>0) &
                          (pods_data_src.req_mem_byte>0) ][['namespace', 'pod_name', 'req_mem_mb', 'req_cpu_milli_core' ]]

daemonset = pods_data_src[(pods_data_src.node_group == 3) & (pods_data_src.owner_kind == 'DaemonSet')].            groupby("owner_name").agg({'req_cpu_milli_core':'mean', 'req_mem_byte':'mean'})

overhead = {'cpu': daemonset.req_cpu_milli_core.sum(), 'memory': daemonset.req_mem_byte.sum()/1000000 }


# ## Load Nodes

# In[3]:


nodes_data = pd.read_csv("instances.csv")

nodes_data['cpu']  = pd.to_numeric(nodes_data.vCPUs, errors='coerce')*1000

nodes_data['memory'] = pd.to_numeric(nodes_data.Memory, errors='coerce')*1000

nodes_data['cost'] =  pd.to_numeric(nodes_data['Linux Reserved cost'], errors='coerce')

nodes_data = nodes_data[['API Name','Name','cpu', 'memory', 'cost']]

total_node_types = len(nodes_data)

nodes_data = nodes_data[(nodes_data.cpu >= pods_data.req_cpu_milli_core.max()) & (nodes_data.memory >= pods_data.req_mem_mb.max()) ]

suitable_node_types = len(nodes_data)

nodes_data.cpu = nodes_data.cpu - overhead['cpu']
nodes_data.memory = nodes_data.cpu - overhead['memory']

print(f"Detected {suitable_node_types} suitable nodes out of {total_node_types} total")


# In[4]:


#Create a data model
#
def create_data_model(cpu, memory, pods):
    """Create the data for the example."""
    data = {}    
    data['req_cpu'] = pods['req_cpu_milli_core'].tolist()
    data['req_memory'] = pods['req_mem_mb'].tolist()
    data['items'] = list(range(len( pods)))
    data['bins'] = data['items']
    data['cpu_capacity'] = cpu
    data['memory_capacity'] = memory
    return data


# In[5]:


def create_solver(data):
    x = {}
    y = {}
    # Create the mip solver with the SCIP backend.
    solver = pywraplp.Solver.CreateSolver('SCIP')
    #solver.SetNumThreads(6)
    
   # solver.SetTimeLimit(30000)

    # Variables
    # x[i, j] = 1 if item i is packed in bin j.

    for i in data['items']:
        for j in data['bins']:
            x[(i, j)] = solver.IntVar(0, 1, 'x_%i_%i' % (i, j))

    # y[j] = 1 if bin j is used.

    for j in data['bins']:
        y[j] = solver.IntVar(0, 1, 'y[%i]' % j)
    
  
    # Constraints
    # Each item must be in exactly one bin.
    for i in data['items']:
        solver.Add(sum(x[i, j] for j in data['bins']) == 1)

    # The amount packed in each bin cannot exceed its cpu capacity.
    for j in data['bins']:
        solver.Add(
            sum(x[(i, j)] * data['req_cpu'][i] for i in data['items']) <= y[j] *
            data['cpu_capacity'])


    # The amount packed in each bin cannot exceed its capacity.
    for j in data['bins']:
        solver.Add(
            sum(x[(i, j)] * data['req_cpu'][i] for i in data['items']) <= y[j] * data['cpu_capacity'])


    for j in data['bins']:
        solver.Add(
            sum(x[(i, j)] * data['req_memory'][i] for i in data['items']) <= y[j] * data['memory_capacity'])
    
    
    return solver, x, y


# In[8]:


def solve(solver, data, x, y, pods):
    solver.Minimize(solver.Sum([y[j] for j in data['bins']]))
    status = solver.Solve()
    solution = []
    if status == pywraplp.Solver.OPTIMAL:
        num_bins = 0.
        for j in data['bins']:
            if y[j].solution_value() == 1:
                bin_items = []
                bin_pods = pd.DataFrame(columns = pods.columns, index = pods.index).sort_index().truncate(-1, -1 ).reindex()
                bin_cpu = 0
                bin_memory = 0
                for i in data['items']:
                    if x[i, j].solution_value() > 0:
                        bin_items.append(i)
                       # print(pods.iloc[[i]])
                        bin_pods = bin_pods.append(pods.iloc[[i]])
                        bin_cpu += data['req_cpu'][i]
                        bin_memory += data['req_memory'][i]
                if bin_cpu > 0 or bin_memory>0:
                    solution.append(bin_pods.copy())
                    num_bins += 1
                    #print('Bin number', j)
                    #print('  Items packed:', bin_items)
                    #print('  Pods packed:', bin_pods)
                    #print(f"  Total cpu: {bin_cpu}, cpu utilization: {round(bin_cpu / data['cpu_capacity'] * 100, 2)}%")
                    #print(f"  Total memory: {bin_memory}, memory utilization {round(bin_memory / data['memory_capacity'] * 100, 2)}%")
                    #print()
        #print()        
        #print('Number of bins used:', num_bins)
        #print('Time = ', solver.WallTime()/1000/60, ' mins')
    else:
        print('The problem does not have an optimal solution.')
        return []
    return solution


# In[9]:


def get_solution(curr_node, pods_data):
    global solutions
    print (f"{datetime.now().strftime('%D %H:%M:%S')}: Solving for {curr_node['API Name']} (cpu: {curr_node.cpu}, memory: {curr_node.memory})")
    
    if (len(solutions[(solutions.cpu == curr_node.cpu) & (solutions.memory == curr_node.memory)]) == 0):
        
        data = create_data_model(curr_node.cpu, curr_node.memory, pods_data)
        solver,x,y = create_solver(data)
        solution = solve(solver, data, x, y, pods_data)
        
        if len(solution) > 0:
            print (f"{datetime.now().strftime('%D %H:%M:%S')}: Solution for {curr_node['API Name']}: {len(solution)} nodes, cost: {len(solution) * curr_node.cost}")
        else:
            print (f"{datetime.now().strftime('%D %H:%M:%S')}: No solution found for {curr_node['API Name']}: {len(solution)} nodes, cost: {len(solution) * curr_node.cost}")
    else:
        old_node = solutions[(solutions.cpu == curr_node.cpu) & (solutions.memory == curr_node.memory)].iloc[0]
        solution = old_node.solution
        print (f"{datetime.now().strftime('%D %H:%M:%S')}: Reusing solution for {old_node['name']}")
            
    
    
    #solutions = solutions.append({'name': curr_node['API Name'], 'cpu': curr_node['cpu'], 'memory': curr_node['memory'], 
    #                 'num_nodes': len(solution), 'cost': curr_cost, 
    #                 'solution': solution}, ignore_index = True)
    
    #if curr_cost < min_cost:
    #    best_node = nodes_data.iloc[i]
    #    min_cost = curr_cost
    #    best_solution = solution
    
    
    
    return pd.DataFrame(
        np.array([curr_node['API Name'], curr_node['cpu'], curr_node['memory'],
                len(solution), curr_node.cost * len(solution), solution]).reshape(1,6),
        columns = solutions.columns)


# In[10]:


from multiprocessing.pool import ThreadPool

solutions = pd.DataFrame(columns = ["name", "cpu", "memory", "num_nodes", "cost", "solution"])


min_cost = float('inf')
best_solution = []
best_node = None

pods = pods_data[:900]

print(f"Starting... {datetime.now().strftime('%D %H:%M:%S')}")

executor = ThreadPoolExecutor(max_workers=12)
futures = []

for i in range(1,  len(nodes_data)):
    curr_node = nodes_data.iloc[i]    
    solution = get_solution(curr_node, pods)
    solutions = solutions.append(solution, ignore_index = True)

    #futures.append(executor.submit(solve_wrapper, curr_node, pods_data))
    
#for future in concurrent.futures.as_completed(futures):
        
#        to_append = pd.DataFrame(np.array(future.result()).reshape(1,6), columns = solutions.columns)
        #print(to_append.columns)
#        solutions = solutions.append(to_append, ignore_index = True)
        #print (future.result())
        
print("All solutions found")

#20 nodes - 1 hour with 6 threads
#20 nodes - 1 hour with 3 threads
#20 nodes - 1 hour with 12 threads


#Number of Pods
#200 pods  7 sec
#400 pods - 64 sec
#600 pods - 9 mins
#800 pods - 36 mins
#1000

#2000 pods - stopped after 18 hours?


#on parallelism https://github.com/google/or-tools/issues/1656


# In[11]:


#solutions[solutions.columns[:-1]]
len(solutions)
#display(solutions[["name", "cpu", "memory", "num_nodes", "cost"]].sort_values(by = "cost",
#                    ascending = "False").reset_index(drop = True))

solutions[["name", "cpu", "memory", "num_nodes", "cost"]].sort_values(by = "cost",
                    ascending = "False").reset_index(drop = True).to_csv("solutions.csv", index=False, )



solutions = solutions[solutions.cost>0].sort_values(by = "cost",
                    ascending = "False").reset_index(drop = True)


# In[12]:


best_solution = solutions.iloc[1]

print(f"Best solution: \n")    
print(f"Node: {best_solution['name']}, cpu: {best_solution.cpu},          memory: {best_solution.memory}, hourly cost: {best_solution.cost}, number of nodes: {len(best_solution.solution)}")
print("Pod placement:\n")
for i in range(0, len(best_solution.solution)):
    print (f"Node {i}:")
    print (best_solution.solution[i]['pod_name'])
    print (f"Utilization: cpu: {round(best_solution.solution[i].req_cpu_milli_core.sum()/best_solution.cpu*100,2)}%,                memory: {round(best_solution.solution[i].req_mem_mb.sum()/best_solution.memory*100, 2)}%")
    


# In[ ]:




