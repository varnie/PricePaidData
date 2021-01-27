### Setup virtual environment and install project dependencies:

    python -m venv venv
    source venv/bin/activate
    pip install -r ./requirements.txt

### Help

    source venv/bin/activate
    python ./src/app.py

### Initial download of CSV data (the 1st step):

    source venv/bin/activate
    python ./src/app.py --action=init

All downloaded CSV files are placed into the `DATA_DIR` folder.
Then all these files get preprocessed to the single `"single.csv"` file without duplicates. This `"single.csv"` file
is created in the `PROCESSED_DATA_DIR`.

For all constants values please refer to the `src/constants.py` module; it is self-explanatory.

***NOTE***:

This step performs cleanup for all the previously downloaded files, i.e. it 
removes all files from the `DATA_DIR` and `PROCESSED_DATA_DIR` folders upon starting.

### Query data from live website (the 2nd step):

    source venv/bin/activate
    python ./src/app.py --action=query
 
This step should be performed only when the 1st step is done and there's the `"single.csv"` file in the 
`PROCESSED_DATA_DIR`.
This step iterates through all the unique postcodes from the `"single.csv"` file, does requests and scrapes
the required relevant fields. 

It stores these fields in a separate CSV file named after a postcode (i.e. `SK13_5DB.csv` will be generated for the 
`"SK13 5DB"` postcode) in the `RESULTS_DIR` folder.
It skips postcodes if there's a corresponding CSV file in the `RESULTS_DIR`, therefore this step may be re-run many 
times without issues.
