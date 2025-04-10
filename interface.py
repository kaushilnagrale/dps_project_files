# interface.py
from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def pagerank(self, max_iterations, weight_property):
        """
        Runs the PageRank algorithm (using GDS if desired),
        and returns a list of exactly TWO dictionaries:
        [
          { 'name': <nodeWithMaxPageRank>, 'score': <maxPageRankScore> },
          { 'name': <nodeWithMinPageRank>, 'score': <minPageRankScore> }
        ]
        """
        with self._driver.session() as session:

            session.run("CALL gds.graph.drop('myGraph', false) YIELD graphName "
                        "RETURN graphName")
            create_graph = """
            CALL gds.graph.project(
                'myGraph',
                'Location',
                {
                  TRIP: {
                    orientation: 'NATURAL',
                    properties: {
                      %s: {
                        defaultValue: 1.0
                      }
                    }
                  }
                }
            )
            """ % weight_property
            session.run(create_graph)

            query = f"""
            CALL gds.pageRank.stream('myGraph', {{
                maxIterations: $maxIters,
                relationshipWeightProperty: $weightProp
            }})
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).name AS name, score
            ORDER BY score DESC
            """
            results = session.run(query, maxIters=max_iterations, weightProp=weight_property)
            records = results.data()  


            if not records:
                return []

            max_rec = records[0]
            min_rec = records[-1]

            return [
                {'name': max_rec['name'], 'score': max_rec['score']},
                {'name': min_rec['name'], 'score': min_rec['score']}
            ]

    def bfs(self, start_node, last_node):
        """
        Return a single BFS path from start_node to last_node.
        We can do this via the Cypher shortestPath for an unweighted BFS.

        The test expects something like:
          [ {
              'path': [
                  {'name': 159},
                  {'name': ... },
                  ...
                  {'name': 212}
              ]
            } ]

        If no path is found, return empty list.
        """
        with self._driver.session() as session:
            query = """
            MATCH p = shortestPath(
              (start:Location { name: $start_node })-[:TRIP*]-(end:Location { name: $last_node })
            )
            RETURN [n IN nodes(p) | n.name] AS path
            """
            result = session.run(query, start_node=start_node, last_node=last_node)
            record = result.single()
            if not record:
                return []


            path_list = record["path"]


            output_path = [{"name": loc} for loc in path_list]

            return [{"path": output_path}]
