import pandas as pd
import time
import sqlite3
from sqlite3 import Error




class Prepare(object):
    
    def __init__(self, file_path):
        self.file_path = file_path


    def Identify(self):
        self.file_type = self.file_path.split(".")[-1]
        return self.file_type


    def DataFrame(self):
        Data = None
        file_type = self.Identify()

        if file_type == 'csv':
            Data = pd.read_csv(self.file_path, low_memory = False)

        elif file_type == 'json':
            Data = pd.read_json()

        else:
            raise "Entery Valid File Type"

        return Data


    def Clean(self):
        Data = self.DataFrame().fillna(method = "ffill")

        return Data

    
    def PrepareResult(self):
        return self.Clean()


class Process(Prepare):

    def __init__(self, file_path):
        super().__init__(file_path)
        self.db_path = file_path.split(".")[0] + ".db"


    def ConnectExecute(self, Qurrey):
        try:
            Connection = sqlite3.connect(self.db_path)
            Cursor = Connection.cursor()
            Cursor.execute(str(Qurrey))
            Connection.commit()
            # print("Done Connect to DB")
        except Error as error:
            print(error)

        finally:
            Connection.close()

        return


    def CreateDB(self):
        Table = self.db_path.split(".")[0]
        Columns = self.PrepareResult().columns
        Data = self.PrepareResult()
        # print(Columns)

        ColumnsCreate = "("

        for Column in Columns:
            DataType = None
            if Data[Column].dtype == "object":
                DataType = "TEXT"

            elif Data[Column].dtype == "int64":
                DataType = "INT"
    
            elif Data[Column].dtype == "float64":
                DataType = "FLOAT"
            else :
                DataType = "TEXT"

            ColumnsCreate += "_" + str(Column.split(" ")[0]) + " " + str(DataType) + ","

        ColumnsCreate += "_AI_ DATETIME DEFAULT CURRENT_TIMESTAMP)"
        Qurrey = "CREATE TABLE IF NOT EXISTS " + str(Table) + str(ColumnsCreate)
        # print(Qurrey)
        self.ConnectExecute(Qurrey)
        return


    def InsertDB(self):
        
        Table = self.db_path.split(".")[0]
        Columns = self.PrepareResult().columns
        Data = self.PrepareResult()
        # print("InsertSection")

        ColumnsInsert = "("

        for Column in Columns:
            ColumnsInsert += "_" + str(Column.split(" ")[0]) + ","

        ColumnsInsert += "_AI_)"
        Qurrey = "INSERT INTO " + str(Table) + str(ColumnsInsert)
        # print(Data)

        for i in range(Data.shape[0]):
            ValuesList = []
            for j in range(Data.shape[1]):
                Value = Data.iloc[i, j]
                ValuesList.append(Value)
            # print(ValuesList)
            ValuesList.append(time.time())
            self.ConnectExecute(str(Qurrey) + " VALUES " + str(tuple(ValuesList)))
        return

    
    def ProcessResult(self):
        self.CreateDB()
        self.InsertDB()
        return


class Product(Process):
    def __init__(self, file_path):
        super().__init__(file_path)

    def ProductResult(self):
        self.ProcessResult()
        return

    def __str__(self):
        return "Convert File to DataBase Successfully"


if __name__ == "__main__":
    FileProduct = Product("MyntraFasionClothing.csv")
    FileProduct.ProductResult()
    print(FileProduct)