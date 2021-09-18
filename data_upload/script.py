import pandas as pd     # to get csv data
import ast    # ast.literal_eval()
import pymongo    # mongoDB with python
from pymongo import MongoClient
client = pymongo.MongoClient(
    "enter you connection uri here, sorry u can't have mine :D")   # connecting to mongoDB
db = client["Movie"]     # selecting database from the cluster
collection = db["en_movie"]    # selecting collection from database
# importing csv file as pandas.DataFrame object
df = pd.read_csv("movie_english.csv")

# function to post data to collection


def upload(name, date, lang, dur, genre, direc, act, c_rate, u_rate, desc, poster):

    # handling some Nan values
    if pd.isna(lang):    # fun fact: Nan in pandas is of type float. sheeesh
        lang = "[]"
    if pd.isna(act):
        act = "[]"
    if pd.isna(genre):
        genre = ""

    # creating a document(row equivalent of NoSQL)
    post = {
        "name": name,
        "date": date,
        # converting string representation of list to actual list
        "lang": ast.literal_eval(lang),
        "dur": dur,
        # " action thriller comedy" -> ["action", "thriller", "comedy"]
        "genre": genre.strip().split(),
        "direc": direc,
        "act": ast.literal_eval(act),
        "c_rate": c_rate,
        "u_rate": u_rate,
        "desc": desc,
        "poster": poster
    }
    collection.insert_one(post)
    print("inserted:", post)


# upload(df.iloc[9][0], df.iloc[9][1], df.iloc[9][2], df.iloc[9][3], df.iloc[9][4], df.iloc[9][5],
#        df.iloc[9][6], df.iloc[9][7], df.iloc[9][8], df.iloc[8][9], df.iloc[9][10])
# collection.insert_one({"name": "some", "score": 2})

n_row = df.shape[0]    # number of rows in dataframe

# calling upload() for every row
for i in range(n_row):
    upload(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2], df.iloc[i][3], df.iloc[i][4], df.iloc[i][5],
           df.iloc[i][6], df.iloc[i][7], df.iloc[i][8], df.iloc[i][9], df.iloc[i][10])
