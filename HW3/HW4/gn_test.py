from collections import deque


def bfs(g, vertex):
    queue = deque([vertex])
    visited = {vertex: 1}
    level = {vertex: 0}
    parent = {vertex: None}
    while queue:
        v = queue.popleft()
        for n in g[v]:
            if n not in visited:
                queue.append(n)
                visited[n] = 1
                level[n] = level[v] + 1
                parent[n] = [v]
            elif n not in parent[v] and level[v] != level[n]:
                parent[n].append(v)
                visited[n] += 1
    return visited


def print_graph(g):
    for row in g:
        print(row)


def add_edge(node_1, node_2):
    graph[node_1][node_2] = 1
    graph[node_2][node_1] = 1


vertices = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
dictionary_vertices = dict(zip(vertices, range(len(vertices))))
edges = []
graph = []
for i in range(len(vertices)):  # make empty adjacency graph
    graph.append([0 for j in range(len(vertices))])

add_edge(dictionary_vertices['A'], dictionary_vertices['B'])
add_edge(dictionary_vertices['A'], dictionary_vertices['C'])
add_edge(dictionary_vertices['B'], dictionary_vertices['D'])
add_edge(dictionary_vertices['B'], dictionary_vertices['C'])
add_edge(dictionary_vertices['D'], dictionary_vertices['E'])
add_edge(dictionary_vertices['D'], dictionary_vertices['F'])
add_edge(dictionary_vertices['D'], dictionary_vertices['G'])
add_edge(dictionary_vertices['G'], dictionary_vertices['F'])
add_edge(dictionary_vertices['E'], dictionary_vertices['F'])

edges.append(('A', 'B'))
edges.append(('A', 'C'))
edges.append(('B', 'D'))
edges.append(('B', 'C'))
edges.append(('D', 'E'))
edges.append(('D', 'F'))
edges.append(('D', 'G'))
edges.append(('G', 'F'))
edges.append(('E', 'F'))

converted_list = {vertices[i]: [vertices[j] for j in range(len(graph[i])) if graph[i][j]] for i in range(len(vertices))}
for each in converted_list:
    print("{0}: {1}".format(each, converted_list[each]))
print(bfs(converted_list, 'E'))
print(edges)

