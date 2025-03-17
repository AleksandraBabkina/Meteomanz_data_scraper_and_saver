# Meteomanz_data_scraper_and_saver
## Description
This script is designed to scrape large volumes of meteorological data from the Meteomanz website, which provides essential weather conditions and station data that significantly impact vehicle performance, accident rates, and overall road safety. By gathering data over multiple years, it allows for deeper analysis of the correlation between weather conditions and automotive statistics. The scraped data is processed, cleaned, and directly uploaded into an Oracle database for further statistical analysis and decision-making purposes. The efficient parsing and bulk downloading of weather data over several years is central to this approach, ensuring that high-quality and accurate datasets are readily available for predictive modeling and operational insights.

## Functional Description
The script performs the following key functions:
1. Scrapes meteorological data from the Meteomanz website for various continents, years, and months.
2. Parses the HTML content to extract weather station data, including temperature, precipitation, wind speed, and other weather-related metrics.
3. Cleans and formats the data to ensure consistency (e.g., converting strings, handling missing or erroneous data).
4. Loads the cleaned data into an Oracle database for long-term storage and analysis.
5. Supports the collection of data over multiple years, ensuring that datasets are comprehensive for analyzing trends over time.

## How It Works
1. The script connects to the Meteomanz website using HTTP requests and retrieves weather data for a range of years and months.
2. It uses BeautifulSoup to parse the HTML data, extracting key metrics such as temperature, wind speed, precipitation, pressure, and other relevant weather conditions.
3. The data is then cleaned by converting invalid or missing values, standardizing formats, and ensuring numerical columns are correctly parsed.
4. The processed data is directly uploaded to an Oracle database using SQLAlchemy, ensuring that the data is available for analysis without the need for manual intervention.
5. The script runs in a loop to process data for each month and year, fetching and uploading it continuously until all required datasets are collected.

## Input Structure
The script does not require specific user input for each run, but it needs to:
1. Connect to an Oracle database using credentials (username, password, DSN).
2. Be configured with the correct URLs for scraping data based on the desired continent, year, and month.

## Technical Requirements
To run the program, the following are required:
1. Python 3.x
2. Installed libraries: `requests`, `BeautifulSoup4`, `pandas`, `sqlalchemy`, `oracledb`, `re`, `math`, `IPython`
3. An Oracle database to store the collected data, with relevant permissions for inserting data into the database.

## Usage
1. Configure your Oracle database connection by modifying the `username`, `password`, and `dsn` values in the script.
2. Adjust the year, month, and continent settings to scrape the data you need.
3. Run the script. The weather data will be automatically scraped, cleaned, and uploaded to your Oracle database.

## Example Output
- The script outputs the collected and processed data directly into the Oracle database.
- Weather station data including:
  - **Temperature** (average, maximum, and minimum)
  - **Precipitation**
  - **Wind speed and direction**
  - **Pressure**
  - **Snow depth** (if available)
  - **Cloud cover**
  - **Insolation**

## Conclusion
This script provides an efficient way to collect and process large volumes of meteorological data from the Meteomanz website. The data is automatically cleaned and uploaded into an Oracle database, making it ready for use in analysis related to automotive performance, accident rates, and other critical factors influenced by weather conditions. By automating the scraping and data-loading processes, it reduces manual work and ensures high-quality data for predictive modeling and decision-making.
