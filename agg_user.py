import os
import pandas as pd
import json
import pymysql
import streamlit as st
# This script processes aggregated transaction data from the PhonePe Pulse dataset.
path="C:/Users/Indhu/phonepe/pulse/data/aggregated/user/country/india/state"
Agg_state_list=os.listdir(path)
#print('Agg_state_list = ', Agg_state_list) # The list Agg_state_list contains the names of states in India for which transaction data is available.

#Creating a dataframe for the aggregated transaction data

clm={'State':[], 'Year':[],'Quarter':[],'reg_user':[], 'brand':[],'count':[],'percentage':[]}

for state in Agg_state_list:
    #print ('Processing state:', state)
#    p_i=path+i+"/"
    Agg_yr=os.listdir(os.path.join(path, state)) #To get a list of all files and directories within that joined constructed path.
    #print('Agg_yr = ', Agg_yr)  # The list Agg_yr contains the years for which transaction data is available for the state.
    for j in Agg_yr:
        #print('Processing year:', j)
#         p_j=p_i+j+"/"
        Agg_quarter_list=os.listdir(os.path.join(path, state, j))
        #print('Agg_quarter_list = ', Agg_quarter_list)
        for k in Agg_quarter_list:
#             p_k=p_j+k
            with open(os.path.join(path, state, j, k), 'r') as Data:
        
                D=json.load(Data) # Load the JSON data from the file
                #print('Processing quarter:', k)
                #print('Data loaded for state:', State, 'year:', j, 'quarter:', k)
                # Extracting transaction data from the loaded JSON
                try:
                    if D['data']['usersByDevice']!= None:
                        for z in D['data']['usersByDevice']:
                            reg_user = D['data']['aggregated']['registeredUsers']
                            brand = z['brand']
                            count = z['count']
                            #print(count)
                            percentage= z['percentage']
                            clm['reg_user'].append(reg_user)
                            clm['brand'].append(brand)
                            clm['count'].append(count)
                            clm['percentage'].append(percentage)
                            clm['State'].append(state)
                            clm['Year'].append(j)
                            clm['Quarter'].append(int(k.strip('.json')))
                    elif D['data']['usersByDevice'] == None:
                            reg_user =D['data']['aggregated']['registeredUsers']
                            brand = 'Data NA'
                            count = '0'
                            percentage = '0'
                            clm['reg_user'].append(reg_user)
                            clm['brand'].append(brand)
                            clm['count'].append(count)
                            clm['percentage'].append(percentage)
                            clm['State'].append(state)
                            clm['State'] = [state.replace('-', ' ').title() for state in clm['State']]
                            
                            clm['Year'].append(j)
                            clm['Quarter'].append(int(k.strip('.json')))
                    
                except Exception as e:
                    print('error=',e)
                    

# Succesfully created a dataframe
df_agg_user=pd.DataFrame(clm)
# print(df_agg_user.columns)

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
    #create_database(cursor, connection, db_name)  
# Using the created database
    # use_database(cursor, db_name) # Function calling
# Creating a table in the database
    table_name = 'Aggregated_User'
    table_type_declaration = "(State VARCHAR(50), Year INT, Quarter INT, reg_user INT, brand VARCHAR(50), count BIGINT, percentage DECIMAL(4,2) )"
    # creation_of_table(cursor, connection, table_name, table_type_declaration) # Function calling
# Insert data into the table
    table_insert_declaration = "(State,Year,Quarter,reg_user,brand,count,percentage) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    # value_to_be_inserted = (df_agg_user['State'], df_agg_user['Year'], df_agg_user['Quarter'], df_agg_user['reg_user'], df_agg_user['brand'], df_agg_user['count'], df_agg_user['percentage'])  # Convert DataFrame columns to list of tuples
    # value_to_be_inserted = list(zip(*value_to_be_inserted))  # Transpose the list of tuples
    # # print("values_to_be_inserted = ", value_to_be_inserted) 
    # response=insertion_table(cursor, connection, table_name, table_insert_declaration, value_to_be_inserted) #Function calling  
    # print("response = ", response)
except Exception as e:
        print(f"Error: {e}")

# groupby operations on the DataFrame
#df.groupby(['Year']).first()  # Grouping the DataFrame by 'State' and displaying the first entry for
#print(df.groupby(['State','Year','Quarter','Transaction_type']).agg({'Transaction_count':['sum'],'Transaction_amount':['sum']}))# Display the first few rows of the DataFrame

        