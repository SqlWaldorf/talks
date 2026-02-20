# Code snippets used during the Translytical Task Flow talk at DataMinds Saturday, Feb 21, 2026, Mechelen

import datetime
import fabric.functions as fn
import logging
from faker import Faker

udf = fn.UserDataFunctions()

@udf.function()
def hello_random() -> str:
    fake = Faker()
    name = fake.name()
    return f"Welcome, {name}"


@udf.function()
def hello_fabric(name: str) -> str:
    logging.info('Python UDF trigger function processed a request.')

    return f"Welcome to Fabric Functions, {name}, at {datetime.datetime.now()}!"

@udf.function()
def somemore (nr1:int) -> int:
    return 42

@udf.connection(argName="sqlDB",alias="datamindsdemo")
@udf.function()
def update_schedule_duration(sqlDB: fn.FabricSqlConnection, sessionid: int, duration: str) -> str:
    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
 
    # update data into the table
    update_query = "UPDATE dbo.datamindsSaturdaySessions SET Duration = ? WHERE sessionid = ?;"
    data = (duration,sessionid)
    cursor.execute(update_query, data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()               
    return f"Session {sessionid} has updated duration: {duration}"


@udf.connection(argName="sqlDB",alias="datamindsdemo")
@udf.function()
def update_schedule_durations(sqlDB: fn.FabricSqlConnection, sessionids: str, duration: str) -> str:
    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()
 
    # update data into the table
    update_query = "UPDATE dbo.datamindsSaturdaySessions SET Duration = ? WHERE sessionid IN " + sessionids
    data = (duration)
    cursor.execute(update_query, data)

    # Commit the transaction
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()               
    return f"Sessions {sessionids} has updated duration: {duration}"

# Select 'Manage connections' and add a connection to a Fabric SQL Database 
# Replace the alias "<alias for sql database>" with your connection alias.
@udf.connection(argName="sqlDB",alias="datamindsdemo")
@udf.function()
def read_username(sqlDB: fn.FabricSqlConnection)-> str:
    query = "SELECT USER_NAME();"
    connection = sqlDB.connect()
    cursor = connection.cursor()
    cursor.execute(query)
    # Fetch all results
    results = []
    for row in cursor.fetchall():
        results.append(row)

    # Close the connection
    cursor.close()
    connection.close()
    return f"This ran as SQL Database user {results[0]}."


import ast

def generate_csv_random_data (params:str, nrofrows: int, locale:str) -> str:
    sep=";"
    fake = Faker(locale)
    config = ast.literal_eval(params)
    result = "id"
    # Header row
    for item in config:
        result+= sep + item[0]
    result+="\n"
    # Data rows
    for rn in range(1,nrofrows+1):
        result += str( rn)
        for item in config:
            method_name = item[1]
            args = item[2] if len(item) > 2 else []
            kwargs = item[3] if len(item) > 3 else {}
            
            # 2. Haal de methode dynamisch op en voer deze uit
            method = getattr(fake, method_name)
            result += sep + method(*args, **kwargs)
        result+="\n"
    
    return result


@udf.connection(argName="myLakehouse", alias="datamindslake")
@udf.function()
def write_csv_file_in_lakehouse(myLakehouse: fn.FabricLakehouseClient, nrofrows: int, filename: str, tableConfig: str, locale:str)-> str:

    csv_string = generate_csv_random_data(tableConfig,nrofrows,locale)
       
    # Upload the CSV file to the Lakehouse
    connection = myLakehouse.connectToFiles()
    csvFile = connection.get_file_client(filename)  
    csvFile.upload_data(csv_string, overwrite=True)
    csvFile.close()
    connection.close()
    return f"File {filename} with {nrofrows} rows of random data was written to the Lakehouse."
