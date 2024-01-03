"""Script which runs the full ETL pipeline."""
from datetime import datetime
from time import perf_counter

from extract import load_sales_data, extract_data_from_json
from transform import clean_dataframe, convert_to_df

if __name__ == "__main__":
    start = perf_counter()
    sales_data = load_sales_data(datetime.now())
    extracted_data = extract_data_from_json(sales_data)

    extracted_data_df = convert_to_df(extracted_data)
    clean_data = clean_dataframe(extracted_data_df)

    print(f"Time taken: {perf_counter() - start}")
