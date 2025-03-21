import streamlit as st
import pandas as pd
import boto3
from io import BytesIO
import pyarrow.parquet as pq

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

# Streamlit App
def app():

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
    cities = ["krakow", "gdansk", "poznan", "wroclaw", "warsaw", "slask"]
    selected_city = st.sidebar.selectbox("Wybierz miasto", sorted(cities))   

    # Fetch data for the selected city
    df_city = fetch_all_parquet_from_s3(selected_city)

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

        st.markdown(
            """
            <style>
            /* Alternating every row */
            .stMarkdown table tr:nth-child(2n+1) {         

                background-color: #2E4E6F; /* Darker blue for the first row */
            }
            .stMarkdown table tr:nth-child(2n+2) {
          
                background-color: #1C2E4A; /* Lighter blue for the next row */
            }
            .stMarkdown table th { background-color: #00172B; color: white; }
            .stMarkdown table td { color: white; }
            </style>
            """,
            unsafe_allow_html=True,
        )
        
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

        league_table_sorted = league_table_sorted.rename(columns={
        "Strzelone_Bramki": "Strzelone Bramki",
        "Stracone_Bramki": "Stracone Bramki",
        "Różnica_Bramek": "Różnica Bramek"       
        })
        
        league_table_sorted.reset_index(drop=True, inplace=True)
        league_table_sorted.index += 1

        st.markdown(league_table_sorted.to_html(escape=False), unsafe_allow_html=True)  
        #st.dataframe(league_table_sorted)
        
    else:
        # Display match results
        team_filtered_df["date"] = pd.to_datetime(team_filtered_df['date'], format="%d/%m/%Y").dt.date
        team_filtered_df_sorted = team_filtered_df[["date", "pitch", "group", "home_team", "home_goals", "away_team", "away_goals"]].sort_values(by=["date", "group"],ascending=[True,True])
        team_filtered_df_sorted.reset_index(drop=True, inplace=True)
        team_filtered_df_sorted.index += 1

        team_filtered_df_sorted = team_filtered_df_sorted.rename(columns={
        "date": "Data",
        "pitch": "Boisko",
        "group": "Grupa",
        "home_team": "Drużyna Gospodarzy",
        "away_team": "Zespół Gości",
        "home_goals": "\u2003",
        "away_goals": "\u2800"
        })

        if selected_team == "Wszystkie Drużyny" and view_option == "Wyniki Meczu":
            
            # Function to assign a background color based on "Grupa"
            def assign_background_color(group):
                if group == "A":
                    return "background-color: #2E4E6F;"  # Darker blue for Grupa A
                elif group == "B":
                    return "background-color: #1C2E4A;"  # Lighter blue for Grupa B
                elif group == "C":
                    return "background-color: #2E4E6F;"  # Darker blue for Grupa C
                elif group == "D":
                    return "background-color: #1C2E4A;"  # Lighter blue for Grupa D
                elif group == "E":
                    return "background-color: #2E4E6F;"  # Lighter blue for Grupa D
                elif group == "F":
                    return "background-color: #1C2E4A;"  # Lighter blue for Grupa D
                elif group == "G":
                    return "background-color: #2E4E6F;"  # Lighter blue for Grupa D
                elif group == "H":
                    return "background-color: #1C2E4A;"  # Lighter blue for Grupa D
                elif group == "I":
                    return "background-color: #2E4E6F;"  # Lighter blue for Grupa D
                elif group == "J":
                    return "background-color: #1C2E4A;"  # Lighter blue for Grupa D
                elif group == "K":
                    return "background-color: #2E4E6F;"  # Lighter blue for Grupa D
                elif group == "L":
                    return "background-color: #1C2E4A;"  # Lighter blue for Grupa D
                elif group == "M":
                    return "background-color: #2E4E6F;"  # Lighter blue for Grupa D
                    
                # Add more conditions if there are more groups
                else:
                    return ""  # Default (no background color)
    
            # Apply the background color to each row based on "Grupa"
            team_filtered_df_sorted["style"] = team_filtered_df_sorted["Grupa"].apply(assign_background_color)    
    
             # Generate the HTML table with inline styles, excluding the "style" column
            html_table = (
                team_filtered_df_sorted
                .style
                .apply(lambda x: [x["style"]] * len(x), axis=1)  # Apply styles
                .hide(axis="columns", subset=["style"])  # Hide the "style" column
                .to_html(escape=False, index=False)
            )
    
            # Display the styled match results
            st.markdown(html_table, unsafe_allow_html=True)        
        
        else:                                        
            # Custom CSS for alternating every three rows
            st.markdown(
                """
                <style>
                /* Alternating every three rows */
                .stMarkdown table tr:nth-child(6n+1),
                .stMarkdown table tr:nth-child(6n+2),
                .stMarkdown table tr:nth-child(6n+3) {         
    
                    background-color: #2E4E6F; /* Darker blue for the first six rows */
                }
                .stMarkdown table tr:nth-child(6n+4),
                .stMarkdown table tr:nth-child(6n+5),
                .stMarkdown table tr:nth-child(6n+6) {
              
                    background-color: #1C2E4A; /* Lighter blue for the next six rows */
                }
                .stMarkdown table th { background-color: #00172B; color: white; }
                .stMarkdown table td { color: white; }
                </style>
                """,
                unsafe_allow_html=True,
            )
                
            # Highlight the selected team in red
            #def highlight_team_match(val):
                #if val == selected_team:
                    #return f'<span style="color: #2E4E6F;">{val}</span>'
                #return val
    
            # Apply highlighting to the "Drużyna Gospodarzy" and "Zespół Gości" columns
            #team_filtered_df_sorted["Drużyna Gospodarzy"] = team_filtered_df_sorted["Drużyna Gospodarzy"].apply(highlight_team_match)
            #team_filtered_df_sorted["Zespół Gości"] = team_filtered_df_sorted["Zespół Gości"].apply(highlight_team_match)
    
           # Display the styled match results
            st.markdown(team_filtered_df_sorted.to_html(escape=False), unsafe_allow_html=True)
                  
            #st.dataframe(team_filtered_df_sorted_styled, height=600, width=500)  # Set fixed height to enable scrolling 
       

# Run the Streamlit app
if __name__ == "__main__":
    app()
