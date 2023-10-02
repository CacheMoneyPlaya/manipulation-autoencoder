# manipulation-lstm



# Manipulation detection model

Project I worked on over a weekend using an auto encoder approach to detect
anticipated surged in volatility on any Binance listed crypto asset. Uses 6 
layers which the input layer is fed a 25x12 dataset of readily available data
from the Binance API (Among other features that were calculated on the fly) and
will then output a 12x1 data structure which I run a Cosine Similarity test against
to determine if the model was able to detect these apparent volatility surges.

It was trained on about 5000 data sets of 5 min data upto 2 hours prior to historical
surges in an attempt to decode subtle patterns that occur before/during distribution
events. The code is also readily available to generate new training data sets.

## TODO:
- General cleanuo
- Make the lookbacks dynamic via .env file
- Make the ticker data retrieval able to run on all tickers at once rather than as specified

## Example Command To Use:

```
python3 concurrent_data_fetcher.py (This will run the scan every 5 mins on all availbe Binance cryptos on a pre-trained model)
python3 create_concurrent_files.py (This will create a new directory containing all listed Binance perps in their own CSV)
python3 generate_training_data.py (This will generate training data and normalize every column looking for a 5% surge ... can be configured in code)
python3 encoder_with_folder_data.py -d AGIX/AGIX_training_data/ (This will run the model against a specified folder containing normalized data and run for each data set, I use this for checking if the model has trained well against its own training data)
python3 ticker_data_retrieval (Will run a script to fetch any avaibale Binance tickers data from between a specified from and to date, will be asked for input on run)
```

- If you get some package errors about them not being available, just install using pip..
- Not all signals are good, look at the PA, it can detect dumps so ignore these, otherwise look for similar setups like below image, not much movement just MM twapping in


![CHR](https://i.imgur.com/eYUU8Fq.png)
![STORJ](https://i.imgur.com/6ovde4I.png)
