import pyarrow.parquet as pq
import pandas as pd
from neo4j import GraphDatabase


class DataLoader:
    def __init__(self, uri, user, password):
        """
        Connect to the Neo4j database and other init steps
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self.driver.verify_connectivity()

    def close(self):
        """
        Close the connection to the Neo4j database
        """
        self.driver.close()

    def load_transform_file(self, file_path):
        """
        Load the parquet file, transform it, and use LOAD CSV in Neo4j.
        """

        trips = pq.read_table(file_path).to_pandas()


        trips = trips[[
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'trip_distance',
            'fare_amount'
        ]]

        bronx = [
            3, 18, 20, 31, 32, 46, 47, 51, 58, 59, 60, 69, 78, 81, 94,
            119, 126, 136, 147, 159, 167, 168, 169, 174, 182, 183, 184,
            185, 199, 200, 208, 212, 213, 220, 235, 240, 241, 242,
            247, 248, 250, 254, 259
        ]
        trips = trips[trips['PULocationID'].isin(bronx) & trips['DOLocationID'].isin(bronx)]
        trips = trips[trips['trip_distance'] > 0.1]
        trips = trips[trips['fare_amount'] > 2.5]
        trips['tpep_pickup_datetime'] = pd.to_datetime(trips['tpep_pickup_datetime'])
        trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])

        trips['tpep_pickup_datetime'] = trips['tpep_pickup_datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        trips['tpep_dropoff_datetime'] = trips['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        print("Number of rows after filtering:", len(trips))


        csv_filename = file_path.split(".")[0] + ".csv"
        trips.to_csv("/var/lib/neo4j/import/" + csv_filename, index=False)


        with self.driver.session() as session:


            session.run("""
                CREATE CONSTRAINT location_name_unique
                IF NOT EXISTS
                FOR (l:Location)
                REQUIRE l.name IS UNIQUE
            """)

            load_query = f"""
            LOAD CSV WITH HEADERS FROM 'file:///{csv_filename}' AS row
            MERGE (start:Location {{name: toInteger(row.PULocationID)}})
            MERGE (end:Location   {{name: toInteger(row.DOLocationID)}})
            MERGE (start)-[t:TRIP {{
                distance: toFloat(row.trip_distance),
                fare: toFloat(row.fare_amount),
                pickup_dt: datetime(row.tpep_pickup_datetime),
                dropoff_dt: datetime(row.tpep_dropoff_datetime)
            }}]->(end)
            """
            session.run(load_query)


def main():
    try:
        data_loader = DataLoader("neo4j://localhost:7687", "neo4j", "project1phase1")
        data_loader.load_transform_file("yellow_tripdata_2022-03.parquet")
        data_loader.close()
        print("Data loaded successfully.")
    except Exception as e:
        print("ERROR loading data:", e)
        raise


if __name__ == "__main__":
    main()
