# file for running data manipulation languages

import pickle
from berkeleydb import db
from errors import *
from messages import *

myDB = db.DB()
myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
myDB.close()

# insert query
def insert(table_name, instance):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    # check table name already exist
    tables = myDB.get(b'table_name')
    table_list = []
    if tables is not None:
        table_list = pickle.loads(tables)
    if table_name not in table_list:
        myDB.sync()
        myDB.close()
        raise NoSuchTable()
    
    # get instance data list from database
    datas = myDB.get((f"{table_name}_ins").encode())
    data_list = []
    if datas is None:
        myDB.put((f"{table_name}_ins").encode(), pickle.dumps(data_list))
    else:
        data_list = pickle.loads(datas)
    
    # insert instance to table schema
    table_schema = myDB.get((f"{table_name}_ad").encode())
    table_schema = pickle.loads(table_schema)
    data = []
    for idx in range(0, len(instance)):
        col = list(table_schema.keys())[idx]
        if table_schema[col][0] == 'char':
            length = int(table_schema[col][1])
            if len(instance[idx][1]) > length+2:
                data.append(instance[idx][1][1:length+1])
            else:
                data.append(instance[idx][1][1:-1])
        else:
            data.append(instance[idx][1])
    data_list.append(data)
    myDB.delete((f"{table_name}_ins").encode())
    myDB.put((f"{table_name}_ins").encode(), pickle.dumps(data_list))
    
    myDB.sync()
    myDB.close()
    return insertResult()

# select query
def select(select_list, from_table_list):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    for table_name, new_table_name in from_table_list:
        
        # check table name already exist
        tables = myDB.get(b'table_name')
        table_list = []
        if tables is not None:
            table_list = pickle.loads(tables)
        if table_name not in table_list:
            myDB.sync()
            myDB.close()
            raise SelectTableExistenceError(table_name)
        
        # get table schema from database
        table_schema = myDB.get((f"{table_name}_ad").encode())
        table_schema = pickle.loads(table_schema)
        length = []
        for att in table_schema:
            print('+', end='')
            print('-'*(len(att)+20), end='')
            length.append(len(att)+18)
        print('+')
        for att in table_schema:
            print(f"|          {att}          ", end='')
        print('|')
        for att in table_schema:
            print('+', end='')
            print('-'*(len(att)+20), end='')
        print('+')
        
        # get instance data list from database
        datas = myDB.get((f"{table_name}_ins").encode())
        data_list = []
        if datas is None:
            myDB.put((f"{table_name}_ins").encode(), pickle.dumps(data_list))
        else:
            data_list = pickle.loads(datas)
        if select_list == []:
            for data in data_list:
                for idx in range(0, len(data)):
                    print(f"| {data[idx]} ", end='')
                    if len(data[idx]) < length[idx]:
                        print(' '*(length[idx]-len(data[idx])), end='')
                print('|')
        else:
            pass
        
        for att in table_schema:
            print('+', end='')
            print('-'*(len(att)+20), end='')
        print('+')
        
    myDB.sync()
    myDB.close()
    return
