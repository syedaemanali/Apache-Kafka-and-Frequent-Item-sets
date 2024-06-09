# Apache-Kafka-and-Frequent-Item-sets

Recomendation System for Amazon

Topic: Streaming Data Insights
Title: Frequent Itemset Analysis on Amazon

Group Members:
Syeda Eman Ali (Roll Number: i221936)
Aliza Saadi (Roll Number: i221871)
Vanya Shafiq (Roll Number: i221953)


Producer Implementation:

-> Developed a Python script for a Kafka producer.
-> Reads data from a JSON file and extracts relevant information.
-> Sends extracted data to a Kafka topic for further processing.

Consumer Implementations:

1. Apriori Algorithm Consumer (Consumer 1):
      -> Implements the Apriori algorithm for association rule mining.
      -> Generates insights into frequent itemsets in real-time.
      -> Presents associations through print statements.
   
2. PCY Algorithm Consumer (Consumer 2):
     -> Implements the PCY algorithm for mining frequent itemsets.
     -> Utilizes hashing for efficient handling of large datasets.
     -> Displays real-time insights and associations.
   
3. Hybrid Recommendation System Consumer (Consumer 3):
    -> Implements a hybrid recommendation system.
    -> Utilizes graph analysis for personalized recommendations.
    -> Accepts user input for liked products, asks user to products purchased by him and generates recommendations accordingly.
    -> Displays recommendations in a user-friendly format.

Kafka Clusters on MS Azure:
-> Created Apache Kafka clusters to manage producer and consumer tasks efficiently.
-> Utilized Kafka clusters to handle large datasets.
-> MS Azure was utilised to form Kafka clusters and run producer and consumers.

MongoDB:
-> We created a database on mongoDB to write and store data, the following credentials can be used to access the created Database:

Database Name: Assosiation_Rules
Collectin_Name: Rules
Localhost:27017





