# app.py

import streamlit as st
import pandas as pd
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import plotly.express as px
from pymongo import MongoClient
import pymysql  # For MySQL connection
import traceback

# MongoDB connection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['demo_db']
mongo_census_collection = mongo_db['census_data']
mongo_products_collection = mongo_db['products']

# PostgreSQL connection (for data queries)
pg_engine = create_engine('postgresql://user:password@localhost:5432/demo_db')

# MySQL connection (for query logs)
mysql_engine = create_engine(
    'mysql+pymysql://user:password@127.0.0.1:3306/demo_db',
    echo=True,  # Enable SQL statement logging
    isolation_level='AUTOCOMMIT'  # Enable autocommit mode
)

# Function to test MySQL connection
def test_mysql_connection():
    st.write("Testing MySQL connection...")
    try:
        with mysql_engine.connect() as conn:
            result = conn.execute(text("SELECT VERSION();"))
            version = result.fetchone()
            st.write(f"MySQL Version: {version[0]}")
    except Exception as e:
        st.error(f"Error connecting to MySQL: {e}")
        st.error(traceback.format_exc())

# Call the test_mysql_connection function
test_mysql_connection()

# Create query_logs table in MySQL if it doesn't exist
def create_query_logs_table():
    st.write("Ensuring query_logs table exists in MySQL...")
    try:
        with mysql_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS query_logs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_source VARCHAR(255),
                    query_complexity VARCHAR(255),
                    dataset VARCHAR(255),  -- Added dataset column
                    duration FLOAT
                );
            """))
        st.write("Successfully ensured the query_logs table exists in MySQL.")
    except SQLAlchemyError as e:
        st.error(f"An error occurred while creating the query_logs table: {e}")
        st.error(traceback.format_exc())
        st.stop()  # Stop execution if table creation fails

create_query_logs_table()  # Ensure the table is created

# Function to log query execution times
def log_query(data_source, query_complexity, dataset, duration):
    st.write(f"Attempting to log query: data_source={data_source}, query_complexity={query_complexity}, dataset={dataset}, duration={duration}")
    st.write(f"Duration type: {type(duration)}")
    try:
        with mysql_engine.connect() as conn:
            insert_query = text("""
                INSERT INTO query_logs (timestamp, data_source, query_complexity, dataset, duration)
                VALUES (NOW(), :data_source, :query_complexity, :dataset, :duration)
            """)
            result = conn.execute(insert_query, {
                'data_source': data_source,
                'query_complexity': query_complexity,
                'dataset': dataset,
                'duration': duration
            })
            st.write(f"Insert result: {result.rowcount} rows inserted.")
        st.write("Successfully logged the query.")
    except Exception as e:
        st.error(f"An error occurred while logging the query: {e}")
        st.error(traceback.format_exc())

# Caching the flat file data
@st.cache_data
def load_census_data_flat():
    st.write("Loading census data from flat file...")
    df = pd.read_csv('USCensus1990.data.txt', header=0)
    if 'caseid' in df.columns:
        df = df.drop(columns=['caseid'])
    st.write("Census data loaded.")
    return df

def load_data_db(query, params):
    st.write("Executing query on PostgreSQL...")
    with pg_engine.connect() as conn:
        result = pd.read_sql(query, conn, params=params)
    st.write("Query executed.")
    return result

def get_postgres_stats():
    st.write("Retrieving PostgreSQL stats...")
    with pg_engine.connect() as conn:
        # Get number of current connections
        conn_count_result = conn.execute(text("SELECT COUNT(*) FROM pg_stat_activity;"))
        conn_count = conn_count_result.scalar()

        # Get read/write operations and latency
        stats_result = conn.execute(text("""
            SELECT sum(xact_commit+xact_rollback) as total_transactions,
                   sum(blks_read) as total_reads,
                   sum(blks_hit) as total_hits,
                   sum(tup_returned) as total_returned,
                   sum(tup_fetched) as total_fetched,
                   sum(tup_inserted) as total_inserted,
                   sum(tup_updated) as total_updated,
                   sum(tup_deleted) as total_deleted
            FROM pg_stat_database;
        """))
        stats = stats_result.fetchone()
        if stats is not None:
            stats_dict = dict(stats._mapping)
        else:
            stats_dict = {}
        stats_dict['current_connections'] = conn_count
    st.write("PostgreSQL stats retrieved.")
    return stats_dict

def get_mongo_stats():
    st.write("Retrieving MongoDB stats...")
    server_status = mongo_db.command("serverStatus")
    # Extract metrics
    connections_current = server_status['connections']['current']
    opcounters = server_status['opcounters']
    network_bytes_in = server_status['network']['bytesIn']
    network_bytes_out = server_status['network']['bytesOut']
    # Latency can be complex to compute; for simplicity, we'll focus on available metrics
    stats = {
        'current_connections': connections_current,
        'opcounters': opcounters,
        'network_bytes_in': network_bytes_in,
        'network_bytes_out': network_bytes_out
    }
    st.write("MongoDB stats retrieved.")
    return stats

def query_mongo(collection, query_complexity, filters):
    st.write(f"Querying MongoDB collection '{collection}' with complexity '{query_complexity}'...")
    if collection == 'census_data':
        # Extract filters
        age_min = filters['age_min']
        age_max = filters['age_max']
        income_threshold = filters['income_threshold']
        sex_options = filters['sex_options']
        if query_complexity == "Simple":
            pipeline = [
                {'$match': {
                    'dAge': {'$gte': age_min, '$lte': age_max},
                    'dIncome1': {'$gte': income_threshold},
                    'iSex': {'$in': sex_options}
                }},
                {'$group': {'_id': '$iSex', 'count': {'$sum': 1}}}
            ]
        elif query_complexity == "Moderate":
            pipeline = [
                {'$match': {
                    'dAge': {'$gte': age_min, '$lte': age_max},
                    'dIncome1': {'$gte': income_threshold},
                    'iSex': {'$in': sex_options}
                }},
                {'$group': {'_id': '$iSex', 'avg_income': {'$avg': '$dIncome1'}}}
            ]
        else:  # Complex
            pipeline = [
                {'$match': {
                    'dAge': {'$gte': age_min, '$lte': age_max},
                    'dIncome1': {'$gte': income_threshold},
                    'iSex': {'$in': sex_options}
                }},
                {'$group': {
                    '_id': {'iSex': '$iSex', 'iMarital': '$iMarital'},
                    'mean': {'$avg': '$dIncome1'},
                    'count': {'$sum': 1}
                }}
            ]
        result = list(mongo_census_collection.aggregate(pipeline))
    elif collection == 'products':
        price_min = filters['price_min']
        price_max = filters['price_max']
        categories = filters['categories']
        if query_complexity == "Simple":
            pipeline = [
                {'$match': {'price': {'$gte': price_min, '$lte': price_max}}},
                {'$group': {'_id': '$category', 'average_price': {'$avg': '$price'}}}
            ]
        elif query_complexity == "Moderate":
            pipeline = [
                {'$match': {
                    'price': {'$gte': price_min, '$lte': price_max},
                    'category': {'$in': categories}
                }},
                {'$unwind': '$reviews'},
                {'$group': {'_id': '$category', 'average_rating': {'$avg': '$reviews.rating'}}}
            ]
        else:  # Complex
            pipeline = [
                {'$match': {
                    'price': {'$gte': price_min, '$lte': price_max},
                    'category': {'$in': categories}
                }},
                {'$unwind': '$reviews'},
                {'$group': {
                    '_id': {
                        'category': '$category',
                        'color': '$attributes.color'
                    },
                    'average_rating': {'$avg': '$reviews.rating'},
                    'average_price': {'$avg': '$price'}
                }}
            ]
        result = list(mongo_products_collection.aggregate(pipeline))
    else:
        result = []
    st.write("MongoDB query executed.")
    return pd.DataFrame(result)

# Sidebar
st.sidebar.title("Settings")

# Dataset Selection
dataset = st.sidebar.selectbox(
    "Select Dataset",
    ("Census Data", "E-commerce Data")
)

# Data Source Selection
data_source = st.sidebar.selectbox(
    "Select Data Source",
    ("Flat File", "PostgreSQL", "MongoDB")
)

# Query Complexity Selection
query_complexity = st.sidebar.selectbox(
    "Select Query Complexity",
    ("Simple", "Moderate", "Complex")
)

# Adjust query parameters based on the dataset
if dataset == "Census Data":
    # Query Parameters for Census data
    age_min, age_max = st.sidebar.slider(
        'Select Age Range (dAge)',
        min_value=0,
        max_value=100,  # Adjust based on actual data
        value=(0, 100)
    )

    income_threshold = st.sidebar.number_input(
        'Income Threshold (dIncome1)',
        min_value=0,
        value=0
    )

    # Additional Filters
    sex_options = st.sidebar.multiselect(
        'Select Sex (iSex)',
        options=[0, 1],
        default=[0, 1]
    )
else:
    # Query Parameters for E-commerce data
    price_min, price_max = st.sidebar.slider('Price Range', 0.0, 1000.0, (0.0, 1000.0))
    categories = st.sidebar.multiselect(
        'Categories',
        ['Electronics', 'Books', 'Clothing', 'Home', 'Toys'],
        default=['Electronics', 'Books']
    )

# Main Page Title
st.title("Database Performance Demo")

# Create tabs
tab1, tab2 = st.tabs(["App", "Database Dashboard"])

with tab1:
    if st.button("Execute Query"):
        st.write(f"**Dataset:** {dataset}")
        st.write(f"**Data Source:** {data_source}")
        st.write(f"**Query Complexity:** {query_complexity}")

        # Start Timer
        start_time = time.time()

        try:
            if data_source == "Flat File":
                st.write("Loading data from flat file...")
                if dataset == "Census Data":
                    df = load_census_data_flat()

                    # Apply Filters
                    df_filtered = df[
                        (df['dAge'] >= age_min) &
                        (df['dAge'] <= age_max) &
                        (df['dIncome1'] >= income_threshold) &
                        (df['iSex'].isin(sex_options))
                    ]

                    # Perform Query Based on Complexity
                    if query_complexity == "Simple":
                        result_df = df_filtered['iSex'].value_counts().reset_index()
                        result_df.columns = ['iSex', 'count']
                    elif query_complexity == "Moderate":
                        result_df = df_filtered.groupby('iSex')['dIncome1'].mean().reset_index()
                        result_df.columns = ['iSex', 'avg_income']
                    else:  # Complex
                        result_df = df_filtered.groupby(['iSex', 'iMarital'])['dIncome1'].agg(['mean', 'count']).reset_index()
                else:
                    st.error("Flat file for E-commerce data not available.")
                    result_df = pd.DataFrame()
            elif data_source == "PostgreSQL":
                st.write("Executing query on PostgreSQL...")
                if dataset == "Census Data":
                    # Build SQL Query for Census Data
                    if query_complexity == "Simple":
                        query = text("""
                            SELECT "iSex", COUNT(*) as count
                            FROM census_data
                            WHERE "dAge" BETWEEN :age_min AND :age_max
                            AND "dIncome1" >= :income_threshold
                            AND "iSex" = ANY(:sex_options)
                            GROUP BY "iSex";
                        """)
                    elif query_complexity == "Moderate":
                        query = text("""
                            SELECT "iSex", AVG("dIncome1") as avg_income
                            FROM census_data
                            WHERE "dAge" BETWEEN :age_min AND :age_max
                            AND "dIncome1" >= :income_threshold
                            AND "iSex" = ANY(:sex_options)
                            GROUP BY "iSex";
                        """)
                    else:  # Complex
                        query = text("""
                            SELECT "iSex", "iMarital", AVG("dIncome1") as mean, COUNT(*) as count
                            FROM census_data
                            WHERE "dAge" BETWEEN :age_min AND :age_max
                            AND "dIncome1" >= :income_threshold
                            AND "iSex" = ANY(:sex_options)
                            GROUP BY "iSex", "iMarital";
                        """)
                    # Execute Query
                    params = {
                        'age_min': age_min,
                        'age_max': age_max,
                        'income_threshold': income_threshold,
                        'sex_options': sex_options
                    }
                    result_df = load_data_db(query, params)
                else:
                    # Build SQL Query for E-commerce Data
                    if query_complexity == "Simple":
                        query = text("""
                            SELECT category, AVG(price) as average_price
                            FROM products
                            WHERE price BETWEEN :price_min AND :price_max
                            GROUP BY category;
                        """)
                        params = {
                            'price_min': price_min,
                            'price_max': price_max
                        }
                        result_df = load_data_db(query, params)
                    elif query_complexity == "Moderate":
                        query = text("""
                            SELECT p.category, AVG(r.rating) as average_rating
                            FROM products p
                            JOIN reviews r ON p.product_id = r.product_id
                            WHERE p.price BETWEEN :price_min AND :price_max
                            AND p.category = ANY(:categories)
                            GROUP BY p.category;
                        """)
                        params = {
                            'price_min': price_min,
                            'price_max': price_max,
                            'categories': categories
                        }
                        result_df = load_data_db(query, params)
                    else:  # Complex
                        # Implement complex query for E-commerce data in PostgreSQL
                        query = text("""
                            SELECT
                                p.category,
                                p.color,
                                AVG(r.rating) as average_rating,
                                AVG(p.price) as average_price
                            FROM
                                products p
                            JOIN
                                reviews r ON p.product_id = r.product_id
                            WHERE
                                p.price BETWEEN :price_min AND :price_max
                                AND p.category = ANY(:categories)
                            GROUP BY
                                p.category,
                                p.color;
                        """)
                        params = {
                            'price_min': price_min,
                            'price_max': price_max,
                            'categories': categories
                        }
                        result_df = load_data_db(query, params)
                st.write("PostgreSQL query executed.")
            elif data_source == "MongoDB":
                if dataset == "Census Data":
                    filters = {
                        'age_min': age_min,
                        'age_max': age_max,
                        'income_threshold': income_threshold,
                        'sex_options': sex_options
                    }
                    result_df = query_mongo('census_data', query_complexity, filters)
                else:
                    filters = {
                        'price_min': price_min,
                        'price_max': price_max,
                        'categories': categories
                    }
                    result_df = query_mongo('products', query_complexity, filters)
            else:
                st.error("Invalid Data Source selected.")
                result_df = pd.DataFrame()
                duration = 0

            # End Timer
            end_time = time.time()
            duration = end_time - start_time

            st.write(f"Query executed in {duration:.4f} seconds.")

            # Log the query
            st.write("About to log query.")
            log_query(data_source, query_complexity, dataset, duration)
            st.write("Finished logging query.")

            # Store result in session state
            st.session_state['result_df'] = result_df
            st.session_state['duration'] = duration
            st.session_state['data_source'] = data_source
            st.session_state['query_complexity'] = query_complexity
            st.session_state['dataset'] = dataset

        except Exception as e:
            st.error(f"An error occurred during query execution: {e}")
            st.error(traceback.format_exc())
            result_df = pd.DataFrame()
            st.session_state['result_df'] = result_df

    # Display results if available in session state
    if 'result_df' in st.session_state and not st.session_state['result_df'].empty:
        result_df = st.session_state['result_df']
        duration = st.session_state.get('duration', 0)
        data_source = st.session_state.get('data_source', '')
        query_complexity = st.session_state.get('query_complexity', '')
        dataset = st.session_state.get('dataset', '')

        st.write(f"**Dataset:** {dataset}")
        st.write(f"**Data Source:** {data_source}")
        st.write(f"**Query Complexity:** {query_complexity}")
        st.write("**Query Results:**")
        st.write(result_df)

        # Display Performance Metrics
        st.write(f"**Time Taken:** {duration:.4f} seconds ({duration * 1000:.2f} milliseconds)")

        # Visualization
        if dataset == "Census Data":
            if query_complexity == "Simple":
                x_col = 'iSex' if 'iSex' in result_df.columns else '_id'
                fig = px.bar(result_df, x=x_col, y='count', labels={x_col: 'Sex', 'count': 'Count'})
            elif query_complexity == "Moderate":
                x_col = 'iSex' if 'iSex' in result_df.columns else '_id'
                fig = px.bar(result_df, x=x_col, y='avg_income', labels={x_col: 'Sex', 'avg_income': 'Average Income'})
            else:  # Complex
                if '_id' in result_df.columns:
                    result_df['iSex'] = result_df['_id'].apply(lambda x: x['iSex'])
                    result_df['iMarital'] = result_df['_id'].apply(lambda x: x['iMarital'])
                fig = px.bar(
                    result_df,
                    x='iMarital',
                    y='mean',
                    color='iSex',
                    barmode='group',
                    labels={'mean': 'Average Income', 'iMarital': 'Marital Status', 'iSex': 'Sex'}
                )
            st.plotly_chart(fig)
        else:
            if query_complexity == "Simple":
                x_col = '_id' if '_id' in result_df.columns else 'category'
                y_col = 'average_price'
                fig = px.bar(result_df, x=x_col, y=y_col, labels={x_col: 'Category', y_col: 'Average Price'})
            elif query_complexity == "Moderate":
                x_col = '_id' if '_id' in result_df.columns else 'category'
                y_col = 'average_rating'
                fig = px.bar(result_df, x=x_col, y=y_col, labels={x_col: 'Category', y_col: 'Average Rating'})
            else:
                if '_id' in result_df.columns:
                    result_df['category'] = result_df['_id'].apply(lambda x: x['category'])
                    result_df['color'] = result_df['_id'].apply(lambda x: x['color'])
                x_col = 'color'
                y_col = 'average_rating'
                fig = px.bar(
                    result_df,
                    x=x_col,
                    y=y_col,
                    color='category',
                    barmode='group',
                    labels={x_col: 'Color', y_col: 'Average Rating', 'category': 'Category'}
                )
            st.plotly_chart(fig)
    else:
        st.write("No results to display.")

with tab2:
    st.header("Database Dashboard")

    # Add a refresh button
    if st.button("Refresh Dashboard"):
        st.session_state['dashboard_refresh'] = True

    if 'dashboard_refresh' not in st.session_state:
        st.session_state['dashboard_refresh'] = False

    if st.session_state['dashboard_refresh']:
        # Clear the refresh state
        st.session_state['dashboard_refresh'] = False

        # Get stats
        postgres_stats = get_postgres_stats()
        mongo_stats = get_mongo_stats()

        # Display stats
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("PostgreSQL Stats")
            # Display metrics
            st.write(f"Current Connections: {postgres_stats.get('current_connections', 'N/A')}")
            st.write(f"Total Transactions: {postgres_stats.get('total_transactions', 'N/A')}")
            st.write(f"Total Reads: {postgres_stats.get('total_reads', 'N/A')}")
            st.write(f"Total Inserts: {postgres_stats.get('total_inserted', 'N/A')}")
            st.write(f"Total Updates: {postgres_stats.get('total_updated', 'N/A')}")
            st.write(f"Total Deletes: {postgres_stats.get('total_deleted', 'N/A')}")

            # Create graphs
            pg_ops_df = pd.DataFrame({
                'Operation': ['Reads', 'Inserts', 'Updates', 'Deletes'],
                'Count': [
                    postgres_stats.get('total_reads', 0),
                    postgres_stats.get('total_inserted', 0),
                    postgres_stats.get('total_updated', 0),
                    postgres_stats.get('total_deleted', 0)
                ]
            })
            fig_pg_ops = px.bar(pg_ops_df, x='Operation', y='Count', title='PostgreSQL Read/Write Operations')
            st.plotly_chart(fig_pg_ops)

        with col2:
            st.subheader("MongoDB Stats")
            # Display metrics
            st.write(f"Current Connections: {mongo_stats['current_connections']}")
            st.write(f"Operation Counters: {mongo_stats['opcounters']}")
            st.write(f"Network Bytes In: {mongo_stats['network_bytes_in']}")
            st.write(f"Network Bytes Out: {mongo_stats['network_bytes_out']}")

            # Create graphs
            mongo_ops_df = pd.DataFrame({
                'Operation': list(mongo_stats['opcounters'].keys()),
                'Count': list(mongo_stats['opcounters'].values())
            })
            fig_mongo_ops = px.bar(mongo_ops_df, x='Operation', y='Count', title='MongoDB Read/Write Operations')
            st.plotly_chart(fig_mongo_ops)

        # Display Query Execution Times
        st.write("Attempting to retrieve query logs...")
        try:
            with mysql_engine.connect() as conn:
                query_logs_df = pd.read_sql(text('SELECT * FROM query_logs'), conn)
            st.write(f"Retrieved {len(query_logs_df)} query logs.")
        except Exception as e:
            st.error(f"An error occurred while retrieving query logs: {e}")
            st.error(traceback.format_exc())
            query_logs_df = pd.DataFrame()

        if not query_logs_df.empty:
            # Convert 'duration' to milliseconds for finer resolution
            query_logs_df['duration_ms'] = query_logs_df['duration'] * 1000

            # Create separate boxplots for each dataset
            datasets = query_logs_df['dataset'].unique()
            for ds in datasets:
                st.subheader(f"Query Execution Times for {ds}")
                ds_df = query_logs_df[query_logs_df['dataset'] == ds]
                fig_boxplot = px.box(
                    ds_df,
                    x='data_source',
                    y='duration_ms',
                    color='query_complexity',
                    points='all',
                    title=f'Query Execution Times by Data Source and Complexity ({ds})',
                    labels={
                        'duration_ms': 'Duration (milliseconds)',
                        'data_source': 'Data Source',
                        'query_complexity': 'Query Complexity'
                    }
                )
                st.plotly_chart(fig_boxplot)

                # Display a summary table
                st.write(f"**Query Performance Summary for {ds}:**")
                summary_df = ds_df.groupby(['data_source', 'query_complexity'])['duration_ms'].describe().reset_index()
                st.dataframe(summary_df)
        else:
            st.write("No query logs to display.")

    else:
        st.write("Press the 'Refresh Dashboard' button to view the latest database statistics.")
