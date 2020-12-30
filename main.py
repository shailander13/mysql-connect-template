import mysql.connector
import pandas as pd

table_schema="""(id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                state VARCHAR(255))"""

class Database :
    def __init__(self):
        self.mydb = mysql.connector.connect(
            host="shailander",
            user="nineleaps",
        )
        self.mycursor = self.mydb.cursor()
        print("Connection Created")

    def __del__(self):
        self.mydb.close()
        print("Connection Ended")

    def create_database(self,db_name):
        self.mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        self.mycursor.execute(f"USE {db_name}")
        print("Database Connected")

    def create_table(self):
        self.mycursor.execute(f"CREATE TABLE IF NOT EXISTS employee {table_schema}")

    def load_data_into_table(self):

        data = pd.read_csv("data.csv")
        df = pd.DataFrame(data,columns=['name','state'])
        val = []

        for item in df.values.tolist() :
          val.append(tuple(item))
        sql = "INSERT INTO employee (name, state) VALUES (%s, %s)"

        self.mycursor.executemany(sql, val)
        self.mydb.commit()

        print(self.mycursor.rowcount, " rows were inserted")

    def show_data(self):
        self.mycursor.execute("SELECT * FROM employee")
        for row in self.mycursor:
            print(row)

if __name__ == '__main__' :
    db = Database()
    db_name = "mydatabase"
    db.create_database(db_name)
    db.create_table()
    db.load_data_into_table()
    db.show_data()

