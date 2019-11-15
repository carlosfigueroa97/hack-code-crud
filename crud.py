"""
Nombre: CRUD.python
Objetivo: implementar las cuatro operaciones  con mysql
Autor:
Fecha: 13 Noviembre de 2019

"""
import pymysql
import credentials as credentials

def dbConnection():
    try:
        db_connection = pymysql.connect(credentials.server,credentials.name,credentials.password,credentials.name)
        print("successfully connection!")
        return db_connection    
    except SystemError as err:
        print(f'Error: {err}')
        return None

def createTables(db):
    try:
        cursor = db.cursor()
        tablesDatabase = getTables(db)
        if(len(tablesDatabase) == 0):
            query_workers = ("""
                CREATE TABLE IF NOT EXISTS workers
                (id varchar(255) NOT NULL, 
                name varchar(255) NOT NULL,
                salary varchar(255) NOT NULL)
                ENGINE = InnoDB
            """)
            cursor.execute(query_workers)
            print("Tables created successfully!") 
    except SystemError as err:
        db.rollback()
        print(f'Error {err}')  

def getTables(db):
    cursor = db.cursor()
    tablesDatabase = []
    cursor.execute("SHOW TABLES;")
       
    if (cursor.rowcount != 0):
        tablesDatabase = [table[0] for table in cursor]

    return tablesDatabase

def insertData(db):
    try:
        cursor = db.cursor()
        tablesDatabase = getTables(db)
        if(len(tablesDatabase) != 0):                
            n = int(input("¿How much data do you want to insert? "))
            for _ in range(n):
                key = input("Enter the worker's key: ")
                name = input("Enter the worker's name: ")
                salary = input("Enter the worker's salary: ")
                query_workers = ("""INSERT INTO workers(id,name,salary)
                    VALUES ('{0}','{1}','{2}')
                """.format(key,name,salary))
                cursor.execute(query_workers)
                db.commit()
            print("Inserted data!")
        else:
            print("Please, create the tables into database!")
    except SystemError as err:
        db.rollback()
        print(f'Error: {err}')

def showData(db):
    try:
        cursor = db.cursor()
        tablesDatabase = getTables(db)
        if(len(tablesDatabase) != 0):            
            cursor.execute("SELECT * FROM workers")
            workers_result = cursor.fetchall()
            for row in workers_result:
                id = row[0]
                name = row[1]
                salary = row[2]
                print("ID: {0} \t NAME: {1} \t SALARY: {2}".format(id,name,salary))            
        else:
            print("Please, create the tables into database!")
    except SystemError as err:
        print(f'Error: {err}')

def deleteData(db):
    try:
        cursor = db.cursor()
        tablesDatabase = getTables(db)
        if(len(tablesDatabase) != 0):
            print(".:Delete all data:.")
            confirmation = int(input('Warning, all data will be deleted!\nContinue? (0 - No\t 1 - Yes)\nOption: '))
            if(confirmation == 1):
                query_workers = ("TRUNCATE TABLE workers;")
                cursor.execute(query_workers)
                print("Deleted data!")
            else:
                print("Ok, aborting!")
        else:
            print("Please, create the tables into database!")
    except SystemError as err:
        db.rollback()
        print(f'Error: {err}')

def searchData(db):
    try:
        value = ""
        field = ""
        cursor = db.cursor()
        tablesDatabase = getTables(db)
        if(len(tablesDatabase) != 0):
            option = input("To look for: 1.- Key  2.- Name  3.- Salary ")
            if(option == "1"):
                value = input("Enter the key: ")
                field = "id"
            elif(option == "2"):
                value = input("Enter the name: ")
                field = "name"
            elif(option == "3"):
                value = input("Enter the salary: ")
                field = "salary"
            else:
                print("There is no such option.")
            if(option != None or option >= 1 or option <= 3):
                if(value == None or value == ""):
                    print("Null values are not accepted")
                else:
                    query = ("SELECT * FROM `workers` WHERE " + field + "=%s")
                    response = validateId(cursor,value,field)
                    if(response == True):
                        cursor.execute(query,(value))
                        response = cursor.fetchall()
                        print(response)
                    else:
                        print("That data was not found!")
        else:
            print("Please, create the tables into database!")
    except SyntaxError as err:
        print(f'Error: {err}')

def validateId(cursor,key,field):
    query = ("SELECT * FROM `workers` WHERE " + field + "=%s")
    cursor.execute(query,(key))
    if(cursor.rowcount == 0):
        return False
    else:
        return True

def updateData(db):
    try:
        value = ""
        cursor = db.cursor()
        tablesDatabase = getTables(db)
        if(len(tablesDatabase) != 0):
            key = input("Enter the worker's key: ")
            if(key != None or key != ""):
                boolean = validateId(cursor,key,"id")
                if(boolean == True):
                    option = input("To look for: 1.- Key  2.- Name  3.- Salary ")
                    if(option == "1"):
                        value = input("Enter the key: ")                        
                        query = ("UPDATE workers SET id=%s WHERE id=%s")
                    elif(option == "2"):
                        value = input("Enter the name: ")                        
                        query = ("UPDATE workers SET name=%s WHERE id=%s")
                    elif(option == "3"):
                        value = input("Enter the salary: ")                        
                        query = ("UPDATE workers SET salary=%s WHERE id=%s")
                    else:
                        print("There is no such option.")
                    if(value != None or value != ""):
                        cursor.execute(query,(value,key))
                        db.commit()
                        print("Updated data!")
                    else:
                        print("Null values are not accepted")
                else:
                    print("That data was not found!")
        else:
            print("Please, create the tables into database!")
    except SyntaxError as err:
        print(f'Error: {err}')

def dashboard(db):
    if(db == None):
        print("Error trying to establish connection")
    else:
        loop = "S"
        while loop == "S" or loop == "s":
            print(" --- CRUD with MYSQL ---")
            print("1. Insert data ")
            print("2. Search data ")
            print("3. Modify data ")
            print("4. Delete data ")
            print("5. Show data ")
            print("6. Create tables ")
            print("7. Exit")
            print("\n")
            option = int(input("Choose option between 1 and 7: "))

            if option == 1:
                insertData(db)
            elif option == 2:
                searchData(db)
            elif option == 3:
                updateData(db)
            elif option == 4:
                deleteData(db)
            elif option == 5:
                showData(db)
            elif option == 6:
                createTables(db)
            elif option == 7:
                loop = 'n'
        else:
            print("Please enter an integer between 1 and 7")

def main():
    db = dbConnection()
    dashboard(db)


if __name__ == "__main__":
    main()