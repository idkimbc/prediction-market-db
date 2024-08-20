import os
from langchain_openai import OpenAI
from langchain.sql_database import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_community.llms.huggingface_hub import HuggingFaceHub
from apikey import apikey

dburi = "sqlite:///testDatabase.db" 
db = SQLDatabase.from_uri(dburi)
llm = OpenAI(temperature=0, api_key=apikey)

db_chain = SQLDatabaseChain(llm=llm, database = db, verbose = True)

db_chain.run("what is the percentage increase or decrease in the air quality index compared from the first half of the day to the second half of the day")