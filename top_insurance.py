import os
import pandas as pd
import json
import pymysql
import streamlit as st
# This script processes aggregated transaction data from the PhonePe Pulse dataset.
path="C:/Users/Indhu/phonepe/pulse/data/top/insurance/country/india/state"
Agg_state_list=os.listdir(path)

#Creating a dataframe for the aggregated transaction data
try:
    clm={'State':[], 'Year':[],'Quarter':[],'districts':[], 'dist_insurance_count':[],'dist_insurance_amount':[],'pincodes':[],'pincode_insurance_count':[],'pincode_insurance_amount':[]}
    for state in Agg_state_list:
        Agg_yr=os.listdir(os.path.join(path, state)) #To get a list of all files and directories within that joined constructed path.
        for j in Agg_yr:
           Agg_quarter_list=os.listdir(os.path.join(path, state, j))
            for k in Agg_quarter_list:
                with open(os.path.join(path, state, j, k), 'r') as Data:
                    D=json.load(Data) # Load the JSON data from the file
           
                # Extracting transaction data from the loaded JSON
                    for z in D['data']['districts']:
                            districts = z['entityName']
                            dist_count = z['metric']['count']
                            dist_amount = z['metric']['amount']
                            for x in D['data']['pincodes']:
                                pincodes = x['entityName']
                                pincode_count = x['metric']['count']
                                pincode_amount = x['metric']['amount']
                                clm['districts'].append(districts)
                                clm['dist_insurance_count'].append(dist_count)
                                clm['dist_insurance_amount'].append(dist_amount)                
                                clm['pincodes'].append(pincodes)
                                clm['pincode_insurance_count'].append(pincode_count)
                                clm['pincode_insurance_amount'].append(pincode_amount)
                                clm['State'].append(state)
                                clm['State'] = [state.replace('-', ' ').title() for state in clm['State']]
                                clm['Year'].append(j)
                                clm['Quarter'].append(int(k.strip('.json'))) 
except Exception as e:
    print('error=',e)
                  

# Succesfully created a dataframe
df_top_insurance=pd.DataFrame(clm)


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
    table_name = 'Top_Insurance'
    table_type_declaration = "(State VARCHAR(50), Year INT, Quarter INT, districts VARCHAR(100), dist_insurance_count BIGINT, dist_insurance_amount BIGINT,pincodes INT,pincode_insurance_count BIGINT,pincode_insurance_amount BIGINT )"
    creation_of_table(cursor, connection, table_name, table_type_declaration) # Function calling
# Insert data into the table
    table_insert_declaration = "(State,Year,Quarter,districts,dist_insurance_count,dist_insurance_amount,pincodes,pincode_insurance_count,pincode_insurance_amount) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    value_to_be_inserted = (df_top_insurance['State'], df_top_insurance['Year'], df_top_insurance['Quarter'], df_top_insurance['districts'], df_top_insurance['dist_insurance_count'], df_top_insurance['dist_insurance_amount'], df_top_insurance['pincodes'],df_top_insurance['pincode_insurance_count'],df_top_insurance['pincode_insurance_amount'])  # Convert DataFrame columns to list of tuples
    value_to_be_inserted = list(zip(*value_to_be_inserted))  # Transpose the list of tuples
    response=insertion_table(cursor, connection, table_name, table_insert_declaration, value_to_be_inserted) #Function calling  
    print("response = ", response)
except Exception as e:
        print(f"Error: {e}")


