import math
from datetime import datetime, date, timedelta, time
import random
import pandas as pd
from faker import Faker
import time
from typing import Tuple



# Config constants
NUM_PEOPLE = 100
NUM_FRIENDS = 40
NUM_ACCESSES = 50
NUM_POINTS = 3000

START = date(2004, 2, 4)
END = date.today()

START_TIME = datetime.combine(START, datetime.min.time())
END_TIME = datetime.combine(END, datetime.min.time())


def create_random_date(start_date, end_date):
	delta = end_date - start_date
	int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
	random_second = random.randint(0, int_delta)
	return start_date + timedelta(seconds=random_second)


def secsToMinSecs(secs) -> str:
	mins = math.floor(secs/60)
	spareSecs = round(secs % 60, 2)
	return "{}m {}s".format(mins, spareSecs)


def gen_mypage(num_people=NUM_PEOPLE) -> pd.DataFrame:
	ids = []
	names = []
	nationalities = []
	country_codes = []
	hobby = []

	# Generate fake info
	fake = Faker()
	hobbies = pd.read_csv("input/hobbies.csv")["hobby"]
	nat = pd.read_csv("input/nationalities.csv", encoding='latin1')

	start_time = time.time()

	for iterator in range(num_people):
		ids.append(iterator)
		names.append(fake.name())
		nationalities.append(nat.sample()['country'].values[0])
		country_codes.append(int(nat.sample()['code'].values[0]))
		hobby.append(hobbies.sample().values[0])
		timeTaken = time.time() - start_time
		timeRemaining = ((timeTaken * num_people) / (iterator + 1)) - timeTaken
		print("\rFinished {}/{} rows ({}%) in {}. About {} remaining"
			.format(iterator + 1, num_people, round(100 * ((iterator + 1) / num_people), 2), secsToMinSecs(timeTaken),
				secsToMinSecs(timeRemaining)), end='')

	print("")

	# Add data to dataframe
	data_frame = pd.DataFrame({
		"PersonID": ids,
		"Name": names,
		"Nationality": nationalities,
		"Country Code": country_codes,
		"Hobby": hobby
	})

	return data_frame


def gen_friends(people: pd.DataFrame, num_friends: int = NUM_FRIENDS) -> pd.DataFrame:

	num_people = people.shape[0]

	data = {
		"FriendRel": [],
		"PersonID": [],
		"MyFriend": [],
		"DateOfFriendship": [],
		"Desc": []
	}

	start_time = time.time()

	relationship_types = ["Friend", "Father", "Mother", "Son", "Daughter", "Uncle", "Aunt", "Cousin", "Brother", "Sister"]
	numForRange = num_people + 1
	list_of_IDS = range(0, numForRange, 1)

	# Add data to dataframe
	for i in range(num_friends):
		# options for relationship
		thisDesc = random.choice(relationship_types)

		# personId and Friend generation - should not repeat
		random_combination = random.sample(list_of_IDS, 2)
		creationOfFriend = create_random_date(START, END)

		data["FriendRel"].append(i)
		data["PersonID"].append(random_combination[0])
		data["MyFriend"].append(random_combination[1])
		data["DateOfFriendship"].append(creationOfFriend)
		data["Desc"].append(thisDesc)

		nDone = i+1
		timeTaken = time.time() - start_time
		timeRemaining = ((timeTaken * num_friends) / nDone) - timeTaken
		print("\rFinished {}/{} rows ({}%) in {}. About {} remaining"
		      .format(nDone, num_friends, round(100 * (nDone / num_friends), 2), secsToMinSecs(timeTaken),
		              secsToMinSecs(timeRemaining)), end='')

	print("")

	data_frame = pd.DataFrame(data)
	data_frame.set_index("FriendRel")

	return data_frame


def gen_accesslog(people: pd.DataFrame, num_accesses: int = NUM_ACCESSES) -> pd.DataFrame:

	num_people = people.shape[0]

	data = {
		"AccessID": [None]*num_accesses,
		"ByWho": [None]*num_accesses,
		"WhatPage": [None]*num_accesses,
		"TypeOfAccess": [None]*num_accesses,
		"AccessTime": [None]*num_accesses
	}

	access_types = ["Viewed Profile", "Shared Post", "Left a Comment", "Liked Post", "Followed Profile"]

	start_time = time.time()

	# Add data to dataframe
	for i in range(num_accesses):
		# Ensures no one accesses their own page
		ids = random.sample(range(num_people), 2)

		random_time = create_random_date(START_TIME, END_TIME).timestamp()

		data["AccessID"][i] = i
		data["ByWho"][i] = ids[0]
		data["WhatPage"][i] = ids[1]
		data["TypeOfAccess"][i] = random.choice(access_types)
		data["AccessTime"][i] = datetime.fromtimestamp(random_time)

		nDone = i+1
		timeTaken = time.time() - start_time
		timeRemaining = ((timeTaken * num_accesses) / nDone) - timeTaken
		print("\rFinished {}/{} rows ({}%) in {}. About {} remaining"
		      .format(nDone, num_accesses, round(100 * (nDone / num_accesses), 2), secsToMinSecs(timeTaken),
		              secsToMinSecs(timeRemaining)), end='')

	print("")

	data_frame = pd.DataFrame(data)
	data_frame.set_index("AccessID")

	return data_frame

def gen_points(num: int = NUM_POINTS) -> pd.DataFrame:
    x = []
    y = []
    
    for i in range(num):
        x.append(random.randint(0, 5000))
        y.append(random.randint(0, 5000))
        
    dataframe = pd.DataFrame({
		"x": x,
		"y": y
	}) 
    return dataframe

def gen_k(k_values, num: int = NUM_POINTS) -> pd.DataFrame:
    x = []
    y = []
    
    for i in range(k_values):
        x.append(random.randint(0, 10000))
        y.append(random.randint(0, 10000))
        
    dataframe = pd.DataFrame({
		"x": x,
		"y": y
	}) 
    return dataframe

def gen_mega_event(num, num_tables) -> pd.DataFrame:
	data = {
	"id": [],
	"name": [],
	"table-i": [],
	"test": [],
	}
	fake = Faker()

	for i in range(num):
		data['id'].append(i)
		data["name"].append(fake.name())
		data["table-i"].append(random.randint(0,num_tables))
		data["test"].append("sick" if random.randint(0, 1) == 0 else "not-sick")

	data_frame = pd.DataFrame(data)
	data_frame.set_index('id', inplace=True)
	return data_frame

def gen_mega_event(num, num_tables) -> pd.DataFrame:
	data = {
	"id": [],
	"name": [],
	"table-i": [],
	"test": [],
	}
	fake = Faker()

	for i in range(num):
		data['id'].append(i)
		data["name"].append(fake.name())
		data["table-i"].append(random.randint(0,num_tables))
		data["test"].append("sick" if random.randint(0, 1) == 0 else "not-sick")

	data_frame = pd.DataFrame(data)
	data_frame.set_index('id', inplace=True)
	return data_frame

def gen_mega_event_no_sick(num, num_tables) -> pd.DataFrame:
	data = {
	"id": [],
	"name": [],
	"table-i": [],
	}
	fake = Faker()

	for i in range(num):
		data['id'].append(i)
		data["name"].append(fake.name())
		data["table-i"].append(random.randint(0,num_tables))

	data_frame = pd.DataFrame(data)
	data_frame.set_index('id', inplace=True)
	return data_frame

def gen_mega_event(num, num_tables) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
	data = {
	"id": [],
	"name": [],
	"table-i": [],
	"test": [],
	}
 
	data_sick = {
		"id": [],
		"test": []
	}
	fake = Faker()

	for i in range(num):
		data['id'].append(i)
		data["name"].append(fake.name())
		data["table-i"].append(random.randint(0,num_tables))
		if random.randint(0, 1) == 0: 
			data["test"].append("sick")
			data_sick["id"].append(i)
			data_sick["test"].append
		else:
			data["test"].append("not-sick")

	data_frame_no_test = pd.DataFrame({k: v for k, v in data.items() if k != "test"})
	data_frame = pd.DataFrame(data)
	df_sick = pd.DataFrame(data_sick)
	data_frame_no_test.set_index('id', inplace=True)
	data_frame.set_index('id', inplace=True)
	df_sick.set_index('id', inplace=True)
	return data_frame_no_test, data_frame, df_sick

def generate_name_within_length(fake, min_length=10, max_length=20):
    while True:
        name = fake.name()
        if min_length <= len(name) <= max_length:
            return name
        
def gen_customers(num) -> pd.DataFrame:
	fake = Faker()

	data = {
		"CustID": [],
		"Name": [],
		"Age": [],
		"Address": [],
		"Salary": []
	}

	for i in range(num):
		data['CustID'].append(i)
		data["Name"].append(generate_name_within_length(fake))
		data["Age"].append(random.randint(18, 100))
		data["Address"].append(fake.address())
		data["Salary"].append(random.uniform(1000, 10000))

	data_frame = pd.DataFrame(data)
	data_frame.set_index('CustID', inplace=True)
	return data_frame

def generate_word_within_length(fake: Faker, min_length=20, max_length=50):
    while True:
        word = fake.word()
        if min_length <= len(word) <= max_length:
            return word

def gen_purchases(num_purchases, num_cust) -> pd.DataFrame:
	fake = Faker()

	data = {
		"TransID": [],
		"CustID": [],
		"TransTotal": [],
		"TransNumItems": [],
		"TransDesc": [],
	}

	for i in range(num_purchases):
		data["TransID"].append(i)
		data['CustID'].append(random.randint(0, num_cust))
		data["TransTotal"].append(random.uniform(10, 2000))
		data["TransNumItems"].append(random.randint(1, 15))
		data["TransDesc"].append(generate_word_within_length(fake))

	data_frame = pd.DataFrame(data)
	data_frame.set_index('TransID', inplace=True)
	return data_frame

if __name__ == "__main__":
	# print("Generating people...")
	# mypage = gen_mypage()
	# mypage.to_csv("output/pages.csv", index=False)

	# print("Generating friends...")
	# friends = gen_friends(mypage)
	# friends.to_csv("output/friends.csv", index=False)

	# print("Generating access...")
	# accessLog = gen_accesslog(mypage)
	# accessLog.to_csv("output/access_logs.csv", index=False)

	# points = gen_points()
	# points.to_csv("output/points.csv", index = False)

	# k_points = gen_k(100)
	# k_points.to_csv("output/k_seeds.csv", index = False)

	mega_event_no_sick, mega_event, sick_report = gen_mega_event(1000000, 100000)
	mega_event.to_csv("output/Mega_Event.csv", index = False)
	print("Done with mega event")

	mega_event_no_sick.to_csv("output/Mega_Event_No_Disclosure.csv", index = False)
	print("Done with mega event no sick")

	sick_report.to_csv("output/Reported_Illnesses.csv", index=False)
	print("Done with sick report")

	customers = gen_customers(50000)
	customers.to_csv("output/Customers.csv", index=False)
	print("Done with customers")

	purchases = gen_purchases(5000000, 50000)
	purchases.to_csv("output/Purchases.csv", index=False)
	print("Done with purchases")