import networkx as nx
import numpy as np

def assign_wcet_and_deadline(G, num_processors):
    # Assigning WCET (Worst Case Execution Time) to each node
    for node in G.nodes():
        G.nodes[node]['wcet'] = np.random.randint(50, 100)

    # Computing the total WCET of the graph
    total_wcet = sum(nx.get_node_attributes(G, 'wcet').values())

    # Computing the utilization for the task
    utilization = total_wcet / (num_processors * 100)

    # Assigning a deadline for the task
    # Assuming Li (release time) is 0 for simplicity
    Li = 0
    Ci = total_wcet
    gamma_value = np.random.gamma(2, 1)
    deadline = Li + Ci + 0.5 * utilization * (1 + 0.25 * gamma_value)

    return G, deadline

# Example usage
num_nodes = 10
p = 0.3
num_processors = 4

# Create a graph G using the Erdős–Rényi model
G = nx.erdos_renyi_graph(num_nodes, p)

# Assign WCET and deadline to the graph
G, deadline = assign_wcet_and_deadline(G, num_processors)

# Displaying the WCET for each node and the deadline for the task
wcet_values = nx.get_node_attributes(G, 'wcet')
print("WCET for each node:", wcet_values)
print("Deadline for the task:", deadline)