
import streamlit as st
import pymysql
import sys, os
# Get the parent folder (pulse/)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agg_insurance import *
from agg_transaction import *
from agg_user import *
import plotly.express as px
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
try:
     # Connection Parameters
    connection = pymysql.connect(
         host='localhost', user='root',
         password='12345')        
    print("connection = ", connection)
    cursor = connection.cursor()
    print("cursor = ", cursor)  

  # using the database
    db_name = 'Phonepe_Pulse'
    use_database(cursor, db_name) # Function calling
    with st.sidebar:
        add_radio = st.radio(
        "Choose a category",
        ("Transactions", "Insurance", "Users"),
        index=0)
    if add_radio == "Users":
        st.subheader('Registered user Behaviour Analysis-Year wise')
        fetch_query = f'''SELECT State,Year, max(reg_user)as Highest_reg_user 
        FROM aggregated_user 
        GROUP BY State,Year      
        ORDER BY Highest_reg_user DESC limit 10 '''
        df_agg_user1= pd.read_sql_query(fetch_query, connection)
        fig1 = px.bar(df_agg_user1, x='Year', y='Highest_reg_user', color='State',width=400, height=500)
        st.plotly_chart(fig1, use_container_width=True)
        st.subheader('Registered users Brand Analysis-State wise')
        state_options = st.selectbox("Select State", df_agg_user['State'], index=0, label_visibility="collapsed")
        selected_year = st.selectbox("Choose a year",[2018,2019,2020, 2021, 2022, 2023,2024],index=0,label_visibility="collapsed",) 
        
        if selected_year >= 2023:
            st.write("Data for the year is not available yet. Please select a different year.")

        else:
            
            fetch_query = f'''SELECT state,count,brand,percentage 
            FROM aggregated_user 
            WHERE state = '{state_options}' AND year = {selected_year}
            GROUP BY brand,state,count,percentage 
            ORDER BY count DESC LIMIT 10'''
            df_agg_user2 = pd.read_sql_query(fetch_query, connection)
            
            fig_agg_user = px.pie(df_agg_user2, values='count',names='brand',labels={'brand': 'Brand', 'count': 'Count'},title=f'Analysis of brand')
            
            fig_agg_user.update_traces(textposition='inside', textinfo='percent+label')
            fig_agg_user.update_layout(legend_title_text='Brand', title_x=0.5, title_font=dict(size=20))

            st.plotly_chart(fig_agg_user, use_container_width=True)
    elif add_radio == "Insurance":
            st.subheader('Insurance Behaviour Analysis-District wise')
            state_options = st.selectbox("Select State", Agg_state_list, index=0, label_visibility="collapsed")
            fetch_query = f'''SELECT State,Year,District, max(Insurance_amount) as High_insurance_amount 
            FROM map_insurance
            WHERE State = '{state_options}'
            GROUP BY State,Year,District
            ORDER BY High_insurance_amount DESC
            LIMIT 10'''
            df_agg_insurance1= pd.read_sql_query(fetch_query, connection)
            fig_agg_insurance = px.bar(df_agg_insurance1, x = 'Year', y = 'High_insurance_amount', color = 'District',labels = 'District', width=400, height=500)
            st.plotly_chart(fig_agg_insurance, use_container_width=True)

            st.subheader('Insurance Behaviour Analysis-year wise')

            selected_year = st.selectbox("Choose a year",[2018,2019,2020, 2021, 2022, 2023,2024],index=0,label_visibility="collapsed",) 
            if selected_year <= 2019:
                st.write("Data for the year is not available yet. Please select a different year.")
            else:
                fetch_query = f'''SELECT State,Year,District, max(Insurance_amount) as High_insurance_amount 
                FROM map_insurance
                WHERE year = {selected_year}
                GROUP BY State,Year,District
                ORDER BY High_insurance_amount DESC
                LIMIT 10'''
                df_agg_insurance2 = pd.read_sql_query(fetch_query, connection)
                fig_agg_insurance2 = px.pie(df_agg_insurance2, values='High_insurance_amount', names='District')
                st.plotly_chart(fig_agg_insurance2, use_container_width=True)
    elif add_radio == 'Transactions':
               
            st.subheader('Transaction Type Analysis-State wise')
            state_options = st.selectbox("Select State", Agg_state_list, index=0, label_visibility="collapsed")
            fetch_query = f'''SELECT State,Transaction_type,Transaction_count,max(Transaction_amount) as High_Transaction_amount  
            FROM aggregated_transaction 
            WHERE State = '{state_options}'
            GROUP BY State,Transaction_type,Transaction_count
            ORDER BY High_Transaction_amount DESC'''
            df_agg_transaction1= pd.read_sql_query(fetch_query, connection)
            # st.bar_chart(source, x="year", y="yield", color="site", stack=False)
            st.bar_chart(df_agg_transaction1, x='State', y='High_Transaction_amount',color = 'Transaction_type',use_container_width=True,stack = True)
              
            st.subheader('Transaction Type Analysis-In years')

            selected_year = st.selectbox("Choose a year",[2018,2019,2020, 2021, 2022, 2023,2024],index=0,label_visibility="collapsed",) 
            # selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4], index=0, label_visibility="collapsed")
            fetch_query = f'''SELECT State,Year,Transaction_type,max(Transaction_amount) as High_Transaction_amount  
            FROM aggregated_transaction 
            WHERE Year ={selected_year} 
            GROUP BY Year,Transaction_type,State
            ORDER BY High_Transaction_amount DESC LIMIT 15'''
            df_agg_transaction2= pd.read_sql_query(fetch_query, connection)
            st.bar_chart(df_agg_transaction2, x='State', y='High_Transaction_amount', color = 'Transaction_type',use_container_width=True,stack = False)
              
except Exception as e:

    print('error = ', e)


