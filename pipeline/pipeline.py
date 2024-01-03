from datetime import datetime
from time import perf_counter

from extract import load_sales_data, extract_data_from_json
from transform import clean_tags

if __name__ == "__main__":
    start = perf_counter()
    sales_data = load_sales_data(datetime.now())
    extracted_data = extract_data_from_json(sales_data)

    extracted_data['tags'] = extracted_data['tags'].apply(clean_tags)
    extracted_data.to_csv("clean_data.csv", index=False)

    print(f"Time taken: {perf_counter() - start}")
