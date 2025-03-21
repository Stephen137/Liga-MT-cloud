# Liga Młodych talentów

⚽ ⚽ ⚽ [Liga Młodych Talentów (The Young Talent League)](https://ligamt.pl/) is a football competition which takes place across six locations in Poland.  Matches take place every fortnight over five rounds. The winter season came to an end last weekend, with just under 600 teams competing in almost 4,500 matches across 23 leagues. 
Results and league standings are currently maintained in Google Sheets, with a publicly available URL for each city. ⚽ ⚽ ⚽

## Project Scope
My aim was to bring all this information together in one place to provide users with real-time information. 

![LigaMT ETL Architecture](https://github.com/user-attachments/assets/6c98d918-b565-4a5a-b964-a44e1d3381ec)

From raw data to insights, here’s what I did:

- ✅ Pulled raw data from six Google Sheets URLs
- ✅ Cleaned & transformed it using Apache Spark in Databricks 🔥
- ✅ Followed the Medallion architecture (Bronze → Silver → Gold)
- ✅ Stored the processed data in AWS S3 (Parquet format) 🏗️
- ✅ Built a Streamlit app to serve insights in real-time! 🎨📊


## Databricks Workflow

![LIGA_MT](https://github.com/user-attachments/assets/d3b09962-e8d2-48b9-980f-b9f0d19ed989)

The Pipeline scriptsa re included in this repo under the Bronze, Silver and Gold folders.


## Write to parquet in AWS S3

![s3](https://github.com/user-attachments/assets/ffafe697-4ab3-4ad0-8713-6554aa124680)


## Serve as a Streamlit app hosted on Community Cloud
https://liga-mt-cloud-development-mode-stephen-barrie.streamlit.app/








