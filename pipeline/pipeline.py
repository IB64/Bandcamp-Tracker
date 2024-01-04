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
    extracted_data = extract_data_from_json(sales_data)
    print("Extracted!")

    # Transform
    extracted_data_df = convert_to_df(extracted_data)
    clean_data_exploded = clean_dataframe(extracted_data_df, False)
    clean_data = clean_dataframe(extracted_data_df, True)
    print("Transformed!")

    # Load
    load_dotenv()
    con = get_db_connection()
    load_data(clean_data_exploded, clean_data, con)
    print("Loaded!")

    print(f"Time taken: {perf_counter() - start}")
