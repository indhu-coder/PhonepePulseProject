
import streamlit as st
from agg_insurance import *
from agg_transaction import *
from agg_user import *
from top_insurance import *
from top_transaction import *
from top_user import *
from map_insurance import *
from map_transaction import *
from map_user import *
import pymysql

def using_db(cursor, database_name):
    use_database_query = f"USE {database_name};"
    print("use_database_query = ", use_database_query)
    cursor.execute(use_database_query)
    print("Using Database Successfully")
    connection.commit()
try:
    # Connection Parameters
    connection = pymysql.connect(
         host='localhost', user='root',
         password='12345')        
    print("connection = ", connection)
    cursor = connection.cursor()
    print("cursor = ", cursor)  


    #using database
    database_name = 'Phonepe_pulse'
    using_db(cursor, database_name) 
    
    st.title('PHONEPE PULSE DATA ANALYSIS')
    
    
    st.write("This application provides insights into PhonePe Pulse data, including aggregated statistics, maps, and top transactions.")
    st.header("About PhonePe Pulse")
    st.write("""
        PhonePe Pulse is a platform that provides insights into digital payments in India. 
        It offers data on transactions, insurance, and user behavior across various states and districts.
        """)
    tab1,tab2,tab3 = st.tabs(["Transactions Data", "Insurance Data", "User Data"])
    with tab1:
        st.subheader("Transactions Data")
        st.write("💳 The pulse of financial activity")
        st.write("Tracks amount, transaction type, transaction count and location")
        st.write("Reveals spending patterns, regional trends, and category growth")
        

    with tab2:
        st.subheader("Insurance Data")
        st.write("🛡️ Signals of security and risk")
        st.write("Covers policy purchases, premiums, and counts")
        st.write("Provides insights into market adoption, consumer confidence, and risk trends.")
       
    with tab3:
        st.subheader(" User Data")
        st.write("👥 The people behind the numbers")
        st.write("Demographics, app usage, and preferences")
        st.write("Enables customer profiling, segmentation, and engagement analysis")

except Exception as e:

    print('error = ', e)

