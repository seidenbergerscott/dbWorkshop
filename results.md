# Results

Run on a Macbook Pro M1 

## Ingestion

Flat file ingestion time: 27.66 seconds
Database ingestion time: 842.20 seconds

Flat file ingestion is 2944.81% faster than database ingestion.

## Querying

Flat file query time: 0.59 seconds
Database query time: 2.39 seconds

Flat file query is 306.97% faster than database query.

Flat File Query Result:
iSex
0    1.594939
1    0.865532
Name: dIncome1, dtype: float64

Database Query Result:
   iSex  avg_income
0     0    1.594939
1     1    0.865532

## Aggregation Visualization

Flat file aggregation time: 0.04 seconds
Database aggregation time: 1.91 seconds

Flat file aggregation is 4217.52% faster than database aggregation.

## Update

Flat file update time: 18.90 seconds
Database update time: 15.17 seconds

Database update is 19.75% faster than flat file update.

## Indexing

Database search time without index: 1.68 seconds
Index on 'dAge' created successfully.
Database search time with index: 1.40 seconds

Search with index is 16.61% faster than search without index.

## NoSQL vs SQL

SQL query time: 2.20 seconds
MongoDB aggregation time: 4.13 seconds

SQL query is 87.79% faster than MongoDB aggregation.

SQL Query Result:
   iSex  avg_income
0     0    1.594939
1     1    0.865532

MongoDB Aggregation Result:
[{'_id': 1, 'avg_income': 0.8655321021791457}, {'_id': 0, 'avg_income': 1.5949389247950483}]

## 