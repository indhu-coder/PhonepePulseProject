import os
import pandas as pd
import json
import pymysql
import streamlit as st

# This script processes aggregated transaction data from the PhonePe Pulse dataset.
path="C:/Users/Indhu/phonepe/pulse/data/map/insurance/country/india/state"
path_1="C:/Users/Indhu/phonepe/pulse/data/map/insurance/hover/country/india/state"
Agg_state_list=os.listdir(path)
Agg_state_list1=os.listdir(path_1)


#Creating a dataframe for the aggregated transaction data

clm={'State':[], 'Year':[],'Quarter':[],'latitude':[], 'longitude':[],'metric':[],'district':[]}
clm1={'State':[], 'Year':[],'Quarter':[],'District':[], 'Insurance_count':[],'Insurance_amount':[]}

for state in Agg_state_list:
    Agg_yr=os.listdir(os.path.join(path, state)) #To get a list of all files and directories within that joined constructed path.
    for j in Agg_yr:
        Agg_quarter_list=os.listdir(os.path.join(path, state, j))
        for k in Agg_quarter_list:
            with open(os.path.join(path, state, j, k), 'r') as Data:
                 D=json.load(Data) # Load the JSON data from the file
           
                # Extracting transaction data from the loaded JSON
                try:
                    pack_list= [x for x in D['data']['data']['data']] #packing of list of values from the file
                    for latitude,longitude,metric,district in pack_list: #unpacking to the respective columns from the list
                        clm['latitude'].append(latitude)
                        clm['longitude'].append(longitude)
                        clm['metric'].append(metric)
                        clm['district'].append(district)
                        clm['State'].append(state)
                        clm['State'] = [state.replace('-', ' ').title() for state in clm['State']] 
                        clm['Year'].append(j)
                        clm['Quarter'].append(int(k.strip('.json')))
                        
                except Exception as e:
                    print('error=',e)
for state in Agg_state_list1:
     Agg_yr=os.listdir(os.path.join(path_1, state)) #To get a list of all files and directories within that joined constructed path.
    for a in Agg_yr:
        Agg_quarter_list=os.listdir(os.path.join(path_1, state, a))
        for b in Agg_quarter_list:
            with open(os.path.join(path_1, state, a, b), 'r') as Data_1:
            D1=json.load(Data_1) # Load the JSON data from the file   
            try:  
                    hover_data = D1["data"]["hoverDataList"]
                    # Extracting the first item from the hoverDataList
                    for x in hover_data:
                        district = x["name"]
                        for metric in x["metric"]:
                            count = metric["count"]
                            amount = metric["amount"]
                            clm1['District'].append(district)
                            clm1['Insurance_count'].append(count)
                            clm1['Insurance_amount'].append(amount)
                            clm1['State'].append(state)
                            clm1['State'] = [state.replace('-', ' ').title() for state in clm1['State']]
                            clm1['Year'].append(a)
                            clm1['Quarter'].append(int(b.strip('.json')))
            except Exception as e:
                print('error=',e)                                    

# Succesfully created a dataframe
df_map=pd.DataFrame(clm)
df_map_insurance=pd.DataFrame(clm1)


# Function definitions for MySQL database operations
@st.cache_resource
def create_database(cursor, connection, db_name):
    """
    This function creates a database in the MySQL server if it does not already exist.
    
    Parameters:
    cursor: The cursor object to execute SQL commands.
    connection: The connection object to the MySQL database.
    db_name: The name of the database to be created.
    """
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {db_name};"
    cursor.execute(create_db_query) # Execute the query to create the database
    connection.commit() # Commit the changes to the database
    print(f"Database {db_name} created successfully.")
def use_database(cursor, db_name):
    use_db_query = f"USE {db_name};"
    cursor.execute(use_db_query)
    print(f"Using database {db_name}.")     
def creation_of_table(cursor,connection,table_name,table_type_declaration):
    """
    This function creates a table in the MySQL database if it does not already exist.
    
    Parameters:
    cursor: The cursor object to execute SQL commands.
    connection: The connection object to the MySQL database.
    table_name: The name of the table to be created.
    table_type_declaration: The SQL declaration for the table's columns and types.
    """
    create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name}{table_type_declaration};'
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table {table_name} created successfully.")
def insertion_table(cursor,connection,table_name,insert_declaration,value_to_be_inserted):
    insertion_query=f"INSERT INTO {table_name} {insert_declaration};"
    print("insertion_query = ", insertion_query)
    cursor.executemany(insertion_query,value_to_be_inserted) 
    print("Data Inserted Successfully") 
    connection.commit()
    
try:  
#connecting to MySQL database
    connection = pymysql.connect(host='localhost',user='root',password='12345')
# Creating a cursor object using the cursor() method
    cursor = connection.cursor() 
# Creating a database if it does not exist
    db_name = 'Phonepe_Pulse'
    create_database(cursor, connection, db_name)  
# Using the created database
    use_database(cursor, db_name) # Function calling
# Creating a table in the database
    table_name = 'Map_location'
    table_name1 = 'Map_Insurance'
    table_type_declaration = "(State VARCHAR(50), Year INT, Quarter INT, latitude FLOAT,longitude FLOAT,metric FLOAT,district VARCHAR(100))"
    table_type_declaration = "(State VARCHAR(50), Year INT, Quarter INT,District VARCHAR(50), Insurance_count INT, Insurance_amount BIGINT)"

    creation_of_table(cursor, connection, table_name1,table_type_declaration) # Function calling
    # Insert data into the table
    
    table_insert_declaration = "(State,Year,Quarter,latitude,longitude,metric,district) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    table_insert_declaration = "(State,Year,Quarter,District,Insurance_count,Insurance_amount) VALUES (%s,%s,%s,%s,%s,%s)"
    value_to_be_inserted = (df_map['State'], df_map['Year'], df_map['Quarter'], df_map['latitude'], df_map['longitude'], df_map['metric'], df_map['district'])  # Convert DataFrame columns to list of tuples
    value_to_be_inserted = (df_map_insurance['State'], df_map_insurance['Year'], df_map_insurance['Quarter'],df_map_insurance['District'], df_map_insurance['Insurance_count'], df_map_insurance['Insurance_amount'])  # Convert DataFrame columns to list of tuples
    value_to_be_inserted = list(zip(*value_to_be_inserted))  # Transpose the list of tuples
    response=insertion_table(cursor, connection, table_name1, table_insert_declaration, value_to_be_inserted) #Function calling  
    print("response = ", response)
except Exception as e:
        print(f"Error: {e}")


