import streamlit as st
import pandas as pd
import boto3
import os
from io import BytesIO
import pyarrow.parquet as pq

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")  # Store in environment variables
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")  # Store in environment variables
S3_BUCKET_NAME = "databricks-workspace-liga-mt-bucket"  # Replace with your S3 bucket name

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Function to fetch Parquet data from S3
@st.cache_data(ttl=3600)  # Cache data for 1 hour to avoid repeated S3 calls
def fetch_parquet_from_s3(city):
    key = f"{S3_BUCKET_NAME}/unity-catalog/silver/{city.lower()}/*.parquet"  # Adjust path as per your S3 structure
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        parquet_file = BytesIO(response["Body"].read())
        return pq.read_table(parquet_file).to_pandas()  # Convert Parquet to Pandas DataFrame
    except Exception as e:
        st.error(f"Error fetching data for {city}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

# Streamlit App
def app():
    st.title("Liga MT - Sezon Zimowy")
    st.header("by Stephen Barrie")

    # Sidebar Filters
    cities = ["Kraków", "Gdańsk", "Poznań", "Wrocław", "Warszawa", "Śląsk"]
    selected_city = st.sidebar.selectbox("Wybierz miasto", sorted(cities))

    # Fetch data for the selected city
    df_city = fetch_parquet_from_s3(selected_city)

    if df_city.empty:
        st.warning("No data available for the selected city.")
        return

    # Age category filter
    selected_age = st.sidebar.selectbox("Wybierz kategorię wiekową", sorted(df_city["category"].unique()))

    # Filter Data
    filtered_df = df_city[df_city["category"] == selected_age]

    # Combine team names from home_team and away_team columns
    all_teams = pd.unique(filtered_df[["home_team", "away_team"]].values.ravel("K"))

    # Add "All Teams" option to the list of teams
    all_teams = ["Wszystkie Drużyny"] + sorted(all_teams)

    # Add team search to the sidebar
    selected_team = st.sidebar.selectbox("Wybierz nazwę zespołu", all_teams)

    # Filter data based on selected team
    if selected_team == "Wszystkie Drużyny":
        team_filtered_df = filtered_df  # Show all matches for the selected age category
    else:
        team_filtered_df = filtered_df[
            (filtered_df["home_team"] == selected_team) | 
            (filtered_df["away_team"] == selected_team)
        ]

    # Display League Table or Match Results
    view_option = st.radio("Wybierz Widok:", ["Tabela Ligowa", "Wyniki Meczu"])

    if view_option == "Tabela Ligowa":
        # Calculate home team standings
        home_standings = filtered_df.groupby("home_team").agg(
            Mecze=("home_team", "count"),
            Zwycięstwa=("home_goals", lambda x: (x > filtered_df.loc[x.index, "away_goals"]).sum()),
            Przegrane=("home_goals", lambda x: (x < filtered_df.loc[x.index, "away_goals"]).sum()),
            Remisy=("home_goals", lambda x: (x == filtered_df.loc[x.index, "away_goals"]).sum()),
            Strzelone_Bramki=("home_goals", "sum"),
            Stracone_Bramki=("away_goals", "sum")
        ).reset_index().rename(columns={"home_team": "Drużyna"})

        # Calculate away team standings
        away_standings = filtered_df.groupby("away_team").agg(
            Mecze=("away_team", "count"),
            Zwycięstwa=("away_goals", lambda x: (x > filtered_df.loc[x.index, "home_goals"]).sum()),
            Przegrane=("away_goals", lambda x: (x < filtered_df.loc[x.index, "home_goals"]).sum()),
            Remisy=("away_goals", lambda x: (x == filtered_df.loc[x.index, "home_goals"]).sum()),
            Strzelone_Bramki=("away_goals", "sum"),
            Stracone_Bramki=("home_goals", "sum")
        ).reset_index().rename(columns={"away_team": "Drużyna"})

        # Combine home and away standings
        league_table = pd.concat([home_standings, away_standings]).groupby("Drużyna").agg(
            Mecze=("Mecze", "sum"),
            Zwycięstwa=("Zwycięstwa", "sum"),
            Remisy=("Remisy", "sum"),
            Przegrane=("Przegrane", "sum"),
            Strzelone_Bramki=("Strzelone_Bramki", "sum"),
            Stracone_Bramki=("Stracone_Bramki", "sum")
        ).reset_index()

        league_table["Różnica_Bramek"] = league_table["Strzelone_Bramki"] - league_table["Stracone_Bramki"]
        league_table["Punkty"] = league_table["Zwycięstwa"] * 3 + league_table["Remisy"]

        # Sort the league table
        league_table_sorted = league_table.sort_values(by=["Punkty", "Różnica_Bramek"], ascending=[False, False])
        league_table_sorted.reset_index(drop=True, inplace=True)
        league_table_sorted.index += 1

        # Display the league table
        st.dataframe(league_table_sorted)
    else:
        # Display match results
        team_filtered_df["date"] = pd.to_datetime(team_filtered_df['date'], format="%d/%m/%Y").dt.date
        team_filtered_df_sorted = team_filtered_df[["date", "home_team", "home_goals", "away_team", "away_goals"]].sort_values(by="date", ascending=True)
        team_filtered_df_sorted.reset_index(drop=True, inplace=True)
        team_filtered_df_sorted.index += 1

        # Display the sorted match results
        st.dataframe(team_filtered_df_sorted)

# Run the Streamlit app
if __name__ == "__main__":
    app()
