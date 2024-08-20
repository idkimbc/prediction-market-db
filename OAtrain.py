import os
from langchain_openai import OpenAI
from langchain.sql_database import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_community.llms.huggingface_hub import HuggingFaceHub
from apikey import apikey

dburi = "sqlite:///testDatabase.db" 
db = SQLDatabase.from_uri(dburi)
llm = OpenAI(temperature=0, api_key=apikey, model="gpt-4o-mini")

db_chain = SQLDatabaseChain(llm=llm, database = db, verbose = True)

db_chain.run("Do you think the average wind speed for the years 2024 - 2030 will be more than 12?")