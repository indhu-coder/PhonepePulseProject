
import streamlit as st
import plotly.express as px
import sys, os

# Get the parent folder (pulse/)
# os.path.abspath(__file__) → gives full path of 1_📊_Aggregated.py
# os.path.dirname() twice → moves up from pages/ to pulse/
# Adds pulse/ to sys.path
# Now Python can see agg_insurance.py directly.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from top_insurance import *
from top_transaction import *
from top_user import *
import pymysql
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)


try:
    connection = pymysql.connect(
    host='localhost', user='root',
    password='12345')
    print("connection = ", connection)
    cursor = connection.cursor()
    print("cursor = ", cursor) 
    #using database
    database_name = 'Phonepe_pulse'
    use_database(cursor, database_name)

    with st.sidebar:
        add_radio = st.radio(
        "Choose a category",
        ("Transactions", "Insurance", "Users"),
        index=0)
#    #SCENARIO 7
    if add_radio == "Users":
        st.subheader('User Registration Analysis-District wise')
        # state_options = st.selectbox("Select State", Agg_state_list, index=0, label_visibility="collapsed")
        selected_year = st.selectbox("Select Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024], index=0, label_visibility="collapsed")
        selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4], index=0, label_visibility="collapsed")
       
        fetch_query = f"""SELECT 
                State,
                districts,
                zipcode,
                Year,
                Quarter,
                SUM(dist_registered_users) AS total_reg_users,
                SUM(zipcode_reg_user) AS total_zipcode_reg_user
            FROM top_user
            WHERE Year = {selected_year} 
            AND Quarter = {selected_quarter} 
           
            GROUP BY State,districts,zipcode
            ORDER BY total_reg_users DESC
            LIMIT 10 """

        df_top_user= pd.read_sql_query(fetch_query, connection)
        st.bar_chart(df_top_user,x='districts',y='total_reg_users')

        st.write('Micro-level User Registration Analysis')
        fetch_query = f"""SELECT 
                State,
                districts,
                zipcode,
                Year,
                Quarter,
                SUM(dist_registered_users) AS total_reg_users,
                SUM(zipcode_reg_user) AS total_zipcode_reg_user
            FROM top_user
            WHERE Year = {selected_year} 
            AND Quarter = {selected_quarter} 
           
            GROUP BY State,districts,zipcode
            ORDER BY total_zipcode_reg_user DESC
            LIMIT 10 """
        df_top_user1= pd.read_sql_query(fetch_query, connection)
        st.bar_chart(df_top_user1,x='zipcode',y='total_zipcode_reg_user')


    

    # SCENARIO 9
    if add_radio == "Insurance" :
            selected_category = st.selectbox(
            "Select Category", ["Districts", "Pincodes"], index=0, label_visibility="collapsed")
            st.write("You selected:", selected_category)
            selected_year = st.selectbox(
            "Select Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024], index=0, label_visibility="collapsed")
            st.write("You selected:", selected_year)
            selected_quarter = st.selectbox(
            "Select Quarter", [1, 2, 3, 4], index=0, label_visibility="collapsed")
            st.write("You selected:", selected_quarter)

            if selected_category == "Districts" and selected_year >= 2020 and selected_quarter >= 1:
                st.subheader('Top 10 Insurance Users - District wise')

                fetch_query = f"""
                    SELECT districts, Year, Quarter, 
                    MAX(dist_insurance_amount) AS High_insurance_amount
                    FROM top_insurance
                    WHERE Year = {selected_year} AND Quarter = {selected_quarter}
                    GROUP BY districts, Year, Quarter
                    ORDER BY High_insurance_amount DESC
                    LIMIT 10"""
                df_1 = pd.read_sql_query(fetch_query, connection)
                st.bar_chart(df_1,x='districts',y='High_insurance_amount')

            elif selected_category == "Pincodes" and selected_year >= 2020 and selected_quarter >= 1 : 
                st.subheader('Pincode wise Insurance users-Top 10')
                fetch_query = f'''SELECT State,pincodes,Year,Quarter,max(pincode_insurance_amount)as Max_insurance_amount 
                FROM top_insurance 
                WHERE Year = {selected_year} AND Quarter = {selected_quarter}
                GROUP BY State,pincodes,Year,Quarter
                
                ORDER BY Max_insurance_amount DESC 
                LIMIT 10'''
                df_2= pd.read_sql_query(fetch_query, connection)
                st.bar_chart(df_2,x='pincodes',y='Max_insurance_amount')
            elif selected_year <= 2020 and selected_quarter >= 1 and add_radio == "Insurance":
                st.write("Insurance data is not available for years before 2020.")
        
            
    # SCENARIO 7
    if add_radio == "Transactions": 
    
        selected_year = st.selectbox( "Select Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024], index=0, label_visibility="collapsed")
        selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4], index=0, label_visibility="collapsed")
        st.subheader('District level champions in Transactions')
        fetch_query = f'''SELECT State,districts,Year,sum(dist_txn_amount) as High_txn_amount ,sum(dist_txn_count) as High_txn_count
        FROM top_transaction
        where Year = {selected_year} AND Quarter = {selected_quarter}
        GROUP BY State,districts
        order by High_txn_amount desc
        limit 10'''
        df_top_txn= pd.read_sql_query(fetch_query, connection)

  
        # Melt dataframe so both Amount & Count can be plotted
        df_melted = df_top_txn.melt(
            id_vars=['State', 'districts'],
            value_vars=['High_txn_amount', 'High_txn_count'],
            var_name='Metric',
            value_name='Value'
        )

        # Create bar chart
        fig_top_txn = px.bar(
            df_melted,
            x="districts",
            y="Value",
            color="Metric",
            barmode="group",
            text_auto='.2s',
            hover_data=["State"]
        )

        # Add nice labels
        fig_top_txn.update_layout(
            
            xaxis_title="District",
            yaxis_title="Value",
            legend_title="Metric",
            xaxis={'categoryorder':'total descending'}
        )

        # Show in Streamlit
        st.plotly_chart(fig_top_txn, use_container_width=True)

        

        st.write("Micro level champions in Transactions")
        fetch_query = f'''SELECT State,districts,zipcodes,sum(zipcode_txn_amount) as High_txn_zipcode ,sum(zipcode_txn_count) as High_count_zipcode
        FROM top_transaction
        Where Year = {selected_year} AND Quarter = {selected_quarter}
        GROUP BY State,districts,zipcodes
        order by High_txn_zipcode desc
        limit 10'''
        df_top_txn1= pd.read_sql_query(fetch_query, connection)
        fig_top_txn2 = px.bar(
            df_top_txn1,
            x="zipcodes",
            y="High_txn_zipcode",
            color="High_txn_zipcode"
        )
        
        # Show in Streamlit
        st.plotly_chart(fig_top_txn2, use_container_width=True)


except Exception as e:

    print('error = ', e)
