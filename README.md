# YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit
This repository contains a Python application for harvesting data from YouTube channels using the YouTube Data API, storing it in a SQL database, and providing interactive analysis and visualization features through a Streamlit web application.

**Features:**
1. Data Harvesting: Extract channel metadata, video details, and comments using the YouTube Data API.
2. Data Warehousing: Store the extracted data in a SQL database for easy access and analysis.
3. Interactive Analysis: Use Streamlit to execute SQL queries, visualize data, and generate insights.
4. Query Examples: Predefined SQL queries for common analytics tasks such as top videos, channel statistics, and comment analysis.

**Usage**
1. Data Extraction: Enter a YouTube channel ID to fetch data from the channel and store it in the SQL database.
2. Data Analysis: Use the Streamlit application to execute SQL queries, visualize data, and gain insights into the YouTube channel's performance.
3. Customization: Modify the SQL queries, data visualization options, and Streamlit layout to suit your specific requirements.

**Structure**
YTharvest.py: Python script for extracting data from YouTube using the YouTube Data API.
MySQL database file to store the extracted data.

**Technologies Used**
1. Python
2. Pandas
3. YouTube Data API
4. SQL (MySQL)
5. Streamlit

**Getting Started**
To get started with the application:
1. Clone the repository to your local machine.
2. Install the necessary Python libraries using pip install 'library name'.
3. Obtain a YouTube Data API key and set it in the code.
4. Set up a SQL database and update the connection details in the code.
5. Run the Streamlit application using streamlit run YTharvest.py.
6. Enter a YouTube channel ID and explore the data analysis features.
