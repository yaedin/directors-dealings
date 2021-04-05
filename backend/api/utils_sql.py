import psycopg2

# ----------------------------------------------
def create_server_connection(host_name, database_name, user_name, user_password):
    connection = None
    try:
        connection = psycopg2.connect(host=host_name, 
                                      database=database_name, 
                                      user=user_name, 
                                      password=user_password)
        print("MySQL Database connection successful")
    except Exception as error:
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))
        
    return connection

# ----------------------------------------------
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Exception as error:
        connection.rollback()
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))

# ----------------------------------------------
def execute_list_query(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Exception as error:
        connection.rollback()
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))

# ----------------------------------------------
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return result, columns
    except Exception as error:
        connection.rollback()
        print ("Oops! An exception has occured:", error)
        print ("Exception TYPE:", type(error))