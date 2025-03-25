import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
import pyarrow.parquet as pq
from datetime import datetime

# Fetch AWS credentials from Streamlit Secrets
AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = st.secrets["AWS_SECRET_KEY"]
S3_BUCKET_NAME = st.secrets["S3_BUCKET_NAME"]

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Function to fetch all Parquet files from a folder in S3
@st.cache_data(ttl=3600)  # Cache data for 1 hour to avoid repeated S3 calls
def fetch_all_parquet_from_s3(city):
    prefix = f"unity-catalog/silver/{city}/"
    try:
        # List objects in the folder
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        if "Contents" not in response:
            st.warning(f"No files found in {prefix}")
            return pd.DataFrame()

        # Fetch and concatenate all Parquet files
        dfs = []
        for obj in response["Contents"]:
            if obj["Key"].endswith(".parquet"):  # Ensure only Parquet files are processed
                file_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=obj["Key"])
                parquet_file = BytesIO(file_response["Body"].read())
                df = pq.read_table(parquet_file).to_pandas()
                dfs.append(df)
        
        if not dfs:
            st.warning(f"No Parquet files found in {prefix}")
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        st.error(f"Error fetching data for {city}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

def calculate_league_table(df):
    """Helper function to calculate league table from a dataframe"""
    home = df.groupby("home_team").agg(
        Mecze=("home_team", "count"),
        Zwycięstwa=("home_goals", lambda x: (x > df.loc[x.index, "away_goals"]).sum()),
        Przegrane=("home_goals", lambda x: (x < df.loc[x.index, "away_goals"]).sum()),
        Remisy=("home_goals", lambda x: (x == df.loc[x.index, "away_goals"]).sum()),
        Gole_Strzelone=("home_goals", "sum"),
        Gole_Stracone=("away_goals", "sum")
    ).reset_index().rename(columns={"home_team": "Drużyna"})
    
    away = df.groupby("away_team").agg(
        Mecze=("away_team", "count"),
        Zwycięstwa=("away_goals", lambda x: (x > df.loc[x.index, "home_goals"]).sum()),
        Przegrane=("away_goals", lambda x: (x < df.loc[x.index, "home_goals"]).sum()),
        Remisy=("away_goals", lambda x: (x == df.loc[x.index, "home_goals"]).sum()),
        Gole_Strzelone=("away_goals", "sum"),
        Gole_Stracone=("home_goals", "sum")
    ).reset_index().rename(columns={"away_team": "Drużyna"})

    table = pd.concat([home, away]).groupby("Drużyna").agg({
        "Mecze": "sum", 
        "Zwycięstwa": "sum", 
        "Remisy": "sum", 
        "Przegrane": "sum",
        "Gole_Strzelone": "sum", 
        "Gole_Stracone": "sum"
    }).reset_index()
    
    # Calculate goal difference and points
    table["Różnica Bramek"] = table["Gole_Strzelone"] - table["Gole_Stracone"]
    table["Punkty"] = table["Zwycięstwa"] * 3 + table["Remisy"]
    
    # Rename columns after all calculations
    table = table.rename(columns={
        "Gole_Strzelone": "Strzelone Bramki",
        "Gole_Stracone": "Stracone Bramki"
    }).sort_values(["Punkty", "Różnica Bramek"], ascending=[False, False])
    
    table.index = range(1, len(table)+1)
    return table

def main():
    st.set_page_config(layout="wide")    
    
    st.title("Liga MT - Sezon Zimowy")
    st.header("by Stephen Barrie")

    st.markdown(
    """
    <style>
    .stMarkdown table th { background-color: #00172B; color: white; }
    .stMarkdown table td { color: white; }
    </style>
    """,
    unsafe_allow_html=True,
    )

    # Sidebar Filters
    cities = {
        "Kraków": "krakow",
        "Gdańsk": "gdansk",
        "Poznań": "poznan",
        "Wrocław": "wroclaw",
        "Warszawa": "warsaw",
        "Śląsk": "slask"
    }

    # Create a sorted list of display names
    sorted_display_names = sorted(cities.keys())
    
    # Create the selectbox with display names
    selected_display_name = st.sidebar.selectbox("Wybierz miasto", sorted_display_names)  

    # Get the corresponding parameter value
    selected_city = cities[selected_display_name]

    # Fetch data for the selected city
    df_city = fetch_all_parquet_from_s3(selected_city)

    if df_city.empty:
        st.warning("No data available for the selected city.")
        return

    # Age category filter
    selected_age = st.sidebar.selectbox("Wybierz kategorię wiekową", sorted(df_city["category"].unique()))

    # Filter Data
    filtered_df = df_city[df_city["category"] == selected_age]
    filtered_df['date'] = pd.to_datetime(filtered_df['date'], format='%d/%m/%Y')

    # Get unique match dates for selected city and age group
    unique_dates = sorted(filtered_df['date'].dt.date.unique(), reverse=True)
    if len(unique_dates) == 0:
        st.error("Brak meczów dla wybranej kombinacji miasta i kategorii wiekowej")
        return

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
        st.markdown(
            """
            <style>
            /* Alternating every row */
            .stMarkdown table tr:nth-child(2n+1) {         
                background-color: #2E4E6F;
            }
            .stMarkdown table tr:nth-child(2n+2) {
                background-color: #1C2E4A;
            }
            .stMarkdown table th { background-color: #00172B; color: white; }
            .stMarkdown table td { color: white; }
            </style>
            """,
            unsafe_allow_html=True,
        )
        
        # Date selection using selectbox with actual match dates
        selected_date_str = st.selectbox(
            "Wybierz datę",
            options=[d.strftime("%d/%m/%Y") for d in unique_dates],
            index=0  # Default to most recent date
        )
        
        show_all_dates = st.checkbox("Pokaż wszystkie daty", value=False)
        
        if show_all_dates:
            # Calculate overall league table
            league_table = calculate_league_table(filtered_df)
            st.subheader(f"Tabela Ligowa")
            st.markdown(league_table.to_html(escape=False), unsafe_allow_html=True)
        else:
            # Calculate daily league tables by group
            selected_date = datetime.strptime(selected_date_str, "%d/%m/%Y").date()
            date_df = filtered_df[filtered_df['date'].dt.date == selected_date]
            groups = sorted(date_df['group'].unique())
            
            if not groups:
                st.warning(f"Brak meczów w dniu {selected_date_str}")
            else:
                st.header (f"Tabela Dnia - {selected_date_str}")
                for group in groups:
                    group_df = date_df[date_df['group'] == group]
                    league_table = calculate_league_table(group_df)                   
                    st.subheader(f"Grupa {group}")                 
                    st.markdown(league_table.to_html(escape=False), unsafe_allow_html=True)
                    st.write("")

    else:  # Wyniki Meczu
        # Date selection using selectbox with actual match dates
        selected_date_str = st.selectbox(
            "Wybierz datę",
            options=["Wszystkie daty"] + [d.strftime("%d/%m/%Y") for d in unique_dates],
            index=0  # Default to most recent date
        )
        
        # Filter by date
        if selected_date_str == "Wszystkie daty":
            results_df = team_filtered_df.copy()
        else:
            selected_date = datetime.strptime(selected_date_str, "%d/%m/%Y").date()
            results_df = team_filtered_df[team_filtered_df['date'].dt.date == selected_date]
        
        # Format and display results with index starting at 1
        results_df = results_df.sort_values(['date', 'group'])
        results_df["date"] = results_df['date'].dt.strftime('%d/%m/%Y')
        
        results_display = results_df[["date", "pitch", "group", "home_team", "home_goals", "away_team", "away_goals"]]
        results_display = results_display.rename(columns={
            "date": "Data", "pitch": "Boisko", "group": "Grupa",
            "home_team": "Drużyna Gospodarzy", "away_team": "Zespół Gości",
            "home_goals": "\u2003", "away_goals": "\u2800"
        })
        
        # Reset index to start at 1
        results_display.index = range(1, len(results_display)+1)
        
        if len(results_display) == 0:
            st.warning("Brak meczów dla wybranych kryteriów")
        else:
            if selected_team == "Wszystkie Drużyny":
                # Apply group-based coloring
                results_display["style"] = results_display["Grupa"].apply(
                    lambda g: "background-color: #2E4E6F;" if ord(g) % 2 else "background-color: #1C2E4A;"
                )
                html_table = (results_display.style
                             .apply(lambda x: [x["style"]] * len(x), axis=1)
                             .hide(axis="columns", subset=["style"])
                             .to_html(escape=False))
                st.markdown(html_table, unsafe_allow_html=True)
            else:
                st.markdown(
                    """
                    <style>
                    /* Alternating every three rows */
                    .stMarkdown table tr:nth-child(6n+1),
                    .stMarkdown table tr:nth-child(6n+2),
                    .stMarkdown table tr:nth-child(6n+3) {         
                        background-color: #2E4E6F;
                    }
                    .stMarkdown table tr:nth-child(6n+4),
                    .stMarkdown table tr:nth-child(6n+5),
                    .stMarkdown table tr:nth-child(6n+6) {
                        background-color: #1C2E4A;
                    }
                    .stMarkdown table th { background-color: #00172B; color: white; }
                    .stMarkdown table td { color: white; }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                st.markdown(results_display.to_html(escape=False), unsafe_allow_html=True)

if __name__ == "__main__":
    main()
