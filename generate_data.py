import random
import pandas as pd
from faker import Faker
from typing import Tuple
import os
from tqdm import tqdm

# Constant Configs
random.seed(42)
fake = Faker()

# Small Data (250 times smaller than large data)
NUM_PEOPLE = 4000
NUM_TABLES = NUM_PEOPLE // 5
NUM_CUSTOMERS = 250
NUM_PURCHASES = NUM_CUSTOMERS * 1000

# Large Data
# NUM_PEOPLE = 1000000 # May need to adjust to reach 50MB
# NUM_TABLES = NUM_PEOPLE // 5
# NUM_CUSTOMERS = 50000
# NUM_PURCHASES = NUM_CUSTOMERS * 1000

def gen_mega_event(num: int, num_tables: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = {
        "id": [],
        "name": [],
        "table_i": [],
        "test": [],
    }

    data_sick = {
        "id": [],
        "test": []
    }

    for i in tqdm(range(num), desc="Generating Mega Event Data", unit=" person"):
        data["id"].append(i)
        data["name"].append(fake.name())
        data["table_i"].append(random.randint(0, num_tables - 1))

        if random.randint(0, 15) < 1: # Made range larger to create even smaller subset
            data["test"].append("sick")
            data_sick["id"].append(i)
            data_sick["test"].append("sick")
        else:
            data["test"].append("not-sick")

    # Create DataFrames
    data_frame = pd.DataFrame(data)
    df_sick = pd.DataFrame(data_sick)
    data_frame_no_test = data_frame.drop(columns=["test"])
    
    # Set index
    data_frame_no_test.set_index("id")
    data_frame.set_index("id")
    df_sick.set_index("id")

    return data_frame_no_test, data_frame, df_sick

def generate_name_within_length(fake, min_length=10, max_length=20):
	while True:
		first = fake.first_name()
		last = fake.last_name()
		full_name = f"{first} {last}"
		if min_length <= len(full_name) <= max_length:
			return full_name
        
def gen_customers(num) -> pd.DataFrame:
	data = {
		"CustID": [],
		"Name": [],
		"Age": [],
		"Address": [],
		"Salary": []
	}

	for i in tqdm(range(num), desc="Generating Customer Data", unit=" customer"):
		data['CustID'].append(i)
		data["Name"].append(generate_name_within_length(fake))
		data["Age"].append(random.randint(18, 100))
		data["Address"].append(fake.address().replace("\n", " ").replace(",", ""))  # Avoid multi-line addresses
		data["Salary"].append(random.randint(1000, 10000))

	data_frame = pd.DataFrame(data)
	data_frame.set_index('CustID')
	return data_frame

def generate_word_within_length(fake: Faker, min_length=20, max_length=50):
    while True:
        sentence = fake.sentence(nb_words=3).replace(",", "")  # Ensure no commas
        if min_length <= len(sentence) <= max_length:
            return sentence

def gen_purchases(num_purchases, num_cust) -> pd.DataFrame:
	data = {
		"TransID": [],
		"CustID": [],
		"TransTotal": [],
		"TransNumItems": [],
		"TransDesc": [],
	}

	for i in tqdm(range(num_purchases), desc="Generating Purchase Data", unit=" purchase"):
		data["TransID"].append(i)
		data['CustID'].append(random.randint(0, num_cust - 1))
		data["TransTotal"].append(round(random.uniform(10, 2000), 2)) 
		data["TransNumItems"].append(random.randint(1, 15))
		data["TransDesc"].append(generate_word_within_length(fake))

	data_frame = pd.DataFrame(data)
	data_frame.set_index('TransID')
	return data_frame

if __name__ == "__main__":
    # Ensure output directory exists
	os.makedirs("output", exist_ok=True)

	# Generate event data
	mega_event_no_sick, mega_event, sick_report = gen_mega_event(NUM_PEOPLE, NUM_TABLES)
	mega_event.to_csv("output/Mega_Event.csv", index = False)
	mega_event_no_sick.to_csv("output/Mega_Event_No_Disclosure.csv", index = False)
	sick_report.to_csv("output/Reported_Illnesses.csv", index=False)

	# Generate customer and purchase data
	customers = gen_customers(NUM_CUSTOMERS)
	customers.to_csv("output/Customers.csv", index=False)

	purchases = gen_purchases(NUM_PURCHASES, NUM_CUSTOMERS)
	purchases.to_csv("output/Purchases.csv", index=False)
