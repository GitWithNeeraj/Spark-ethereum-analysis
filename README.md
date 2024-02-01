# Spark-ethereum-analysis
Ethereum Transactions and Smart Contracts Analysis
Introduction
This project aims to analyze Ethereum transactions and smart contracts to gain insights into their patterns and behavior. It utilizes Spark for big data processing to efficiently handle large volumes of data.

Time Analysis
Number of Transactions per Month
The first analysis involves creating a bar plot to visualize the number of transactions occurring every month between the start and end of the dataset. The Python script total.py uses Spark to read Ethereum transaction data from a CSV file stored in an S3 bucket. It then removes invalid transactions, computes the total number of transactions per month, and saves the result as a JSON file in another S3 bucket. The script also displays the result on the console.

To generate the bar plot, the Jupyter Notebook plota1.ipynb uses the matplotlib library to plot the number of transactions by month and year present in the total_transactions.txt file. The data is sorted by month and year, and the resulting bar plot is displayed.

Average Transaction Value per Month
The second analysis focuses on creating a bar plot to show the average value of transactions in each month between the start and end of the dataset. The Python script average.py loads transaction data from an S3 bucket, calculates the average transaction value per month, and saves the result to another S3 bucket.

To visualize the data, the Jupyter Notebook plota2.ipynb imports the data from total_transactions.txt, containing dates and transaction values, and uses matplotlib to plot the average transaction values per month and year.

Top Ten Most Popular Services
The third part of the analysis identifies the top ten most popular services in Ethereum transactions and smart contracts. The Python script topten.py reads data from two CSV files stored in an S3 bucket, filters and processes the data, and then saves the result in another S3 bucket.

The output is presented in the top10_smart_contracts2.txt file and is displayed in a table format, showing the rank, address, and total Ethereum value for the top ten most popular services.

Top Ten Most Active Miners
The fourth part of the analysis identifies the top ten most active miners of the Ethereum blockchain based on the size of their blocks. The Python script active.py performs data transformations using Spark to obtain the top ten miners.

The results are presented in the active_miners.txt file and displayed in a table format, showing the rank, miner address, and block size for the top ten most active miners.

Data Exploration - Scam Analysis
The final part of the analysis involves exploring scam-related data. Scam information is provided in the scams.json file, which is converted to a CSV file named scams.csv using the Python script jsontocsv.py.

Popular Scams
To identify the most lucrative scams, the Python script scams.py reads the scams.csv and transactions.csv files, and then performs data transformations to obtain the top 15 most lucrative scams. The results are saved in the most_lucrative_scams.txt file and are displayed in a table format, showing the rank, scam ID, scam type, and total ether profited.

Gas Guzzlers
The analysis also includes studying gas consumption and gas prices. The Python script gas_guz.py calculates the average gas price change over time and the average gas used over time using data from transactions.csv and contracts.csv. The results are saved in the av_gasused.txt and av_gasprice.txt files, respectively.

The data is visualized using matplotlib in the Jupyter Notebooks av_gasused.ipynb and av_gaspriced.ipynb.

Conclusion
This project provides valuable insights into Ethereum transactions and smart contracts. The analyses performed offer a deeper understanding of the data patterns and behavior, allowing for better decision-making and optimization strategies when dealing with large-scale Ethereum data.
![image](https://github.com/GitWithNeeraj/Spark-ethereum-analysis/assets/84373485/9781f6ef-e708-4d9b-b170-8bd0cda24f09)
![image](https://github.com/GitWithNeeraj/Spark-ethereum-analysis/assets/84373485/e2ed981a-a169-45a9-aa27-0e5d12972123)
