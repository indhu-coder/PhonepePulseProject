import streamlit as st
import pymysql
import sys, os
from urllib.request import urlopen
import streamlit as st
# Get the parent folder (pulse/)
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from map_insurance import *
import plotly.express as px
import pandas as pd 
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

    with urlopen("https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson") as response:
        jsonfile = json.load(response)

    with st.sidebar:
        add_radio = st.radio(
        "Choose a category",
        ("Transactions", "Insurance", "Users"),
        index=0)
    if add_radio == "Insurance":
        st.title("India Map with Total Insurance amount across States")
        selected_year = st.selectbox("Select Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024], index=0, label_visibility="collapsed")
        selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4], index=0, label_visibility="collapsed")
        if selected_year>=2020 or selected_quarter>1:
            fetch_query = f'''SELECT State,
            SUM(Insurance_amount) AS Total_insurance_amount,
            SUM(Insurance_count) AS Total_insurance_count
            FROM
            aggregated_insurance
            WHERE Year = {selected_year} AND Quarter = {selected_quarter}
            GROUP BY
            State'''

            df = pd.read_sql_query(fetch_query, connection)
            df['State'] = [state.replace('-', ' ').title() for state in df['State']]
            df["State"] = df["State"].str.strip()
            df["State"] = df["State"].str.title()
            df["State"] = df["State"].replace({
                "Andaman & Nicobar Islands": "Andaman & Nicobar",
                "Nct Of Delhi": "Delhi",
                "Dadara & Nagar Havelli": "Dadra & Nagar Haveli",
                "Jammu And Kashmir": "Jammu & Kashmir",
                "Arunanchal Pradesh": "Arunachal Pradesh",
                "Dadra & Nagar Haveli & Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu"
            })

            # --- Get valid states from GeoJSON ---
            geo_states = [feature["properties"]["ST_NM"] for feature in jsonfile["features"]]

            # --- Find mismatched states ---
            missing_states = set(df["State"]) - set(geo_states)

            if missing_states:
                print("⚠️ These states from SQL do not match GeoJSON:")
                for ms in missing_states:
                    print("   -", ms)
            else:
                print("✅ All states matched GeoJSON!")
            
            fig_map = px.choropleth(df,
                        geojson=jsonfile,
                        locations='State',
                        color='Total_insurance_amount',
                        featureidkey="properties.ST_NM",
                        hover_data=["Total_insurance_count"],
                        scope="asia",
                        color_continuous_scale='YlOrRd')

            fig_map.update_geos(fitbounds="locations", visible=False)

        # Remove fixed width/height and let Streamlit handle it
            fig_map.update_layout(
            autosize=True,
            # minimal padding
            )

            # Display inside Streamlit app (auto responsive)
            st.plotly_chart(fig_map, use_container_width=True)
       

            st.title("India Map with Average Ticket Size(ATS) across States")
            fetch_query = f'''SELECT State,(SUM(Insurance_amount)/SUM(Insurance_count) ) as Average_transaction_value
            FROM
            aggregated_insurance
            WHERE Year = {selected_year} AND Quarter = {selected_quarter}
            GROUP BY
            State
            order by average_transaction_value'''
            df = pd.read_sql_query(fetch_query, connection)
            df['State'] = [state.replace('-', ' ').title() for state in df['State']]
            df["State"] = df["State"].str.strip()
            df["State"] = df["State"].str.title()
            df["State"] = df["State"].replace({
                "Andaman & Nicobar Islands": "Andaman & Nicobar",
                "Nct Of Delhi": "Delhi",
                "Dadara & Nagar Havelli": "Dadra & Nagar Haveli",
                "Jammu And Kashmir": "Jammu & Kashmir",
                "Arunanchal Pradesh": "Arunachal Pradesh",
                "Dadra & Nagar Haveli & Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu"
            })

            # --- Get valid states from GeoJSON ---
            geo_states = [feature["properties"]["ST_NM"] for feature in jsonfile["features"]]

            # --- Find mismatched states ---
            missing_states = set(df["State"]) - set(geo_states)

            if missing_states:
                print("⚠️ These states from SQL do not match GeoJSON:")
                for ms in missing_states:
                    print("   -", ms)
            else:
                print("✅ All states matched GeoJSON!")
                
            fig_map4 = px.choropleth(df,
                            geojson=jsonfile,
                            locations='State',
                            color='Average_transaction_value',
                            featureidkey="properties.ST_NM",
                            hover_data=["Average_transaction_value"],
                            scope="asia",
                            color_continuous_scale='YlOrRd')

            fig_map4.update_geos(fitbounds="locations", visible=False)

            # Remove fixed width/height and let Streamlit handle it
            fig_map4.update_layout(
                autosize=True,
                # minimal padding
                )

                # Display inside Streamlit app (auto responsive)
            st.plotly_chart(fig_map4, use_container_width=True)
        else:
            st.write("Insurance data is not available for years before 2020.")
        

    elif add_radio == "Users":
        st.title("India Map with Total Registered Users across States")
        fetch_query = f'''SELECT State,sum(reg_user) as Total_reg_user FROM map_user
        GROUP BY State'''

        df_map_1 = pd.read_sql_query(fetch_query, connection)

        df_map_1["State"] = df_map_1["State"].str.strip()
        df_map_1["State"] = df_map_1["State"].str.title()
        df_map_1["State"] = df_map_1["State"].replace({
            "Andaman & Nicobar Islands": "Andaman & Nicobar",
            "Nct Of Delhi": "Delhi",
            "Dadara & Nagar Havelli": "Dadra & Nagar Haveli",
            "Jammu And Kashmir": "Jammu & Kashmir",
            "Arunanchal Pradesh": "Arunachal Pradesh",
            "Dadra & Nagar Haveli & Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu"
        })

        # --- Get valid states from GeoJSON ---
        geo_states = [feature["properties"]["ST_NM"] for feature in jsonfile["features"]]

        # --- Find mismatched states ---
        missing_states = set(df_map_1["State"]) - set(geo_states)

        if missing_states:
            print("⚠️ These states from SQL do not match GeoJSON:")
            for ms in missing_states:
                print("   -", ms)
        else:
            print("✅ All states matched GeoJSON!")

        fig_map1 = px.choropleth(df_map_1,
                    geojson=jsonfile,
                    locations='State',
                    color='Total_reg_user',
                    featureidkey="properties.ST_NM",
                    hover_data=["Total_reg_user"],
                    scope="asia",
                    color_continuous_scale='gnbu')

        fig_map1.update_geos(fitbounds="locations", visible=False)

    # Remove fixed width/height and let Streamlit handle it
        fig_map1.update_layout(
        autosize=True,
        # minimal padding
        )

        # Display inside Streamlit app (auto responsive)
        st.plotly_chart(fig_map1, use_container_width=True)

        st.title("India Map with active user ratio across States")
        fetch_query = f'''SELECT State,(SUM(appopens)/SUM(reg_user))as active_users
        FROM map_user
        GROUP BY State'''

        df_map_1 = pd.read_sql_query(fetch_query, connection)
        fig_map2 = px.choropleth(df_map_1,
                    geojson=jsonfile,
                    locations='State',
                    color='active_users',
                    featureidkey="properties.ST_NM",
                    hover_data=["active_users"],
                    scope="asia",
                    color_continuous_scale='Viridis')

        fig_map2.update_geos(fitbounds="locations", visible=False)

    # Remove fixed width/height and let Streamlit handle it
        fig_map2.update_layout(
        autosize=True,
        # minimal padding
        )

        # Display inside Streamlit app (auto responsive)
        st.plotly_chart(fig_map2, use_container_width=True)

    elif add_radio == "Transactions":
        st.title("India Map with High Transaction amount across States")
        selected_year = st.selectbox("Select Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024], index=0, label_visibility="collapsed")
        selected_quarter = st.selectbox("Select Quarter", [1, 2, 3, 4], index=0, label_visibility="collapsed")
        fetch_query = f'''SELECT State,max(map_txn_amount) as High_Transaction_Amount
        FROM map_transaction
        WHERE year = {selected_year} AND quarter = {selected_quarter}
        GROUP BY State'''

        df_map_2 = pd.read_sql_query(fetch_query, connection)
        df_map_2['State'] = [state.replace('-', ' ').title() for state in df_map_2['State']]
        df_map_2["State"] = df_map_2["State"].str.strip()
        df_map_2["State"] = df_map_2["State"].str.title()
        df_map_2["State"] = df_map_2["State"].replace({
            "Andaman & Nicobar Islands": "Andaman & Nicobar",
            "Nct Of Delhi": "Delhi",
            "Dadara & Nagar Havelli": "Dadra & Nagar Haveli",
            "Jammu And Kashmir": "Jammu & Kashmir",
            "Arunanchal Pradesh": "Arunachal Pradesh",
            "Dadra & Nagar Haveli & Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu"
        })

        # --- Get valid states from GeoJSON ---
        geo_states = [feature["properties"]["ST_NM"] for feature in jsonfile["features"]]

        # --- Find mismatched states ---
        missing_states = set(df_map_2["State"]) - set(geo_states)

        if missing_states:
            print("⚠️ These states from SQL do not match GeoJSON:")
            for ms1 in missing_states:
                print("   -", ms1)
        else:
            print("✅ All states matched GeoJSON!")

        fig_map3 = px.choropleth(df_map_2,
                    geojson=jsonfile,
                    locations='State',
                    color='High_Transaction_Amount',
                    featureidkey="properties.ST_NM",
                    hover_data=["High_Transaction_Amount"],
                    scope="asia",
                    hover_name='State',
                    color_continuous_scale='gnbu')

        fig_map3.update_geos(fitbounds="locations", visible=False)

    # Remove fixed width/height and let Streamlit handle it
        fig_map3.update_layout(
        autosize=True,
        # minimal padding
        )

        # Display inside Streamlit app (auto responsive)
        st.plotly_chart(fig_map3, use_container_width=True)

        st.title("India Map with Average Transaction Value across States")
       

        
        fetch_query = f'''SELECT State,(sum(map_txn_amount)/sum(map_txn_count)) as Average_transaction_value
        FROM map_transaction
        WHERE year = {selected_year} AND quarter = {selected_quarter}
        GROUP BY State'''

        df_map_5 = pd.read_sql_query(fetch_query, connection)
        df_map_5['State'] = [state.replace('-', ' ').title() for state in df_map_5['State']]
        df_map_5["State"] = df_map_5["State"].str.strip()
        df_map_5["State"] = df_map_5["State"].str.title()
        df_map_5["State"] = df_map_5["State"].replace({
            "Andaman & Nicobar Islands": "Andaman & Nicobar",
            "Nct Of Delhi": "Delhi",
            "Dadara & Nagar Havelli": "Dadra & Nagar Haveli",
            "Jammu And Kashmir": "Jammu & Kashmir",
            "Arunanchal Pradesh": "Arunachal Pradesh",
            "Dadra & Nagar Haveli & Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu"
        })

        # --- Get valid states from GeoJSON ---
        geo_states = [feature["properties"]["ST_NM"] for feature in jsonfile["features"]]

        # --- Find mismatched states ---
        missing_states = set(df_map_5["State"]) - set(geo_states)

        if missing_states:
            print("⚠️ These states from SQL do not match GeoJSON:")
            for ms1 in missing_states:
                print("   -", ms1)
        else:
            print("✅ All states matched GeoJSON!")
        fig_map_6 = px.choropleth(df_map_5,
                    geojson=jsonfile,
                    locations='State',
                    color='Average_transaction_value',
                    featureidkey="properties.ST_NM",
                    hover_data=["Average_transaction_value"],
                    scope="asia",
                    hover_name='State',
                    color_continuous_scale='Turbo')

        fig_map_6.update_geos(fitbounds="locations", visible=False)

    # Remove fixed width/height and let Streamlit handle it
        fig_map_6.update_layout(
        autosize=True,
        # minimal padding
        )

        # Display inside Streamlit app (auto responsive)
        st.plotly_chart(fig_map_6, use_container_width=True)

except Exception as e:
    print('Error:', e)




