"""Script which runs the full ETL pipeline."""
from datetime import datetime
from time import perf_counter

from dotenv import load_dotenv

from extract import load_sales_data, extract_data_from_json
from transform import clean_dataframe, convert_to_df
from load import get_db_connection, load_data

if __name__ == "__main__":
    start = perf_counter()
    # Extract
    sales_data = load_sales_data(datetime.now())
    print("Loaded!", start - perf_counter())
    extracted_data = extract_data_from_json(sales_data)
    print("Extracted!", start - perf_counter())

    # Transform
    extracted_data_df = convert_to_df(extracted_data)
    print("Converted!", start - perf_counter())
    clean_data_exploded, clean_data = clean_dataframe(extracted_data_df)
    print("Transformed!", start - perf_counter())

    # Load
    load_dotenv()
    con = get_db_connection()
    load_data(clean_data_exploded, clean_data, con)
    print("Loaded!", start - perf_counter())

    print(f"Time taken: {perf_counter() - start}")
