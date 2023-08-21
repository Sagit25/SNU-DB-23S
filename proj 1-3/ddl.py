# file for running data defintion languages (+ show, etc.)

import pickle
from berkeleydb import db
from errors import *
from messages import *

# create table query
def create_table(table_name, table_schema):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    # check table name already exist, if not create
    tables = myDB.get(b'table_name')
    table_list = []
    if tables is None:
        myDB.put(b'table_name', pickle.dumps(table_list))
    else:
        table_list = pickle.loads(tables)
    if table_name in table_list:
        myDB.sync()
        myDB.close()
        raise TableExistenceError()
    
    # get column information and intergity constraint from schema
    column = {}
    pk = []
    fks = []
    for item in table_schema:
        if item[0] != ['keys']:
            name, data_type, nullable = item
            if name in column:
                myDB.sync()
                myDB.close()
                raise DuplicateColumnDefError()
            if nullable == 'not null':
                nullable = False
            else:
                nullable = True
            column[name] = [data_type[0], data_type[1], nullable]
        else:
            if item[1][0] == 'pk':
                if pk != []:
                    myDB.sync()
                    myDB.close()
                    raise DuplicatePrimaryKeyDefError()
                for pk_col in item[1][1]:
                    if pk_col not in column:
                        myDB.sync()
                        myDB.close()
                        raise NonExistingColumnDefError(pk_col)
                    pk.append(pk_col)
                    for col in column:
                        if pk_col == col:
                            column[col][2] = False
            elif item[1][0] == 'fk':
                fks.append([item[1][1], item[1][2], item[1][3]])
    
    # check several errors related with foreign keys
    for fk in fks:
        atts, ref_table, ref_atts = fk
        try:
            if ref_table not in table_list:
                myDB.sync()
                myDB.close()
                raise ReferenceTableExistenceError()
            ref_col = myDB.get((f"{ref_table}_ad").encode())
            ref_col = pickle.loads(ref_col)
            ref_pk = myDB.get((f"{ref_table}_pk").encode())
            ref_pk = pickle.loads(ref_pk)
            for att in atts:
                if att not in column:
                    myDB.sync()
                    myDB.close()
                    raise NonExistingColumnDefError(att)
            if ref_col is None:
                myDB.sync()
                myDB.close()
                raise ReferenceTableExistenceError()
            for ref_att in ref_atts:
                if ref_att in ref_col:
                    continue
                else:
                    myDB.sync()
                    myDB.close()
                    raise ReferenceColumnExistenceError()
            if len(ref_atts) == len(atts):
                if ref_atts != ref_pk:
                    myDB.sync()
                    myDB.close()
                    raise ReferenceNonPrimaryKeyError()
                for att, ref_att in zip(atts, ref_atts):
                    col1 = column[att]
                    col2 = ref_col[ref_att]
                    if col1[0] == col2[0] and col1[1] == col2[1]:
                        continue
                    else:
                        myDB.sync()
                        myDB.close()
                        raise ReferenceTypeError()
            else:
                myDB.sync()
                myDB.close()
                raise ReferenceTypeError()
        except db.DBNotFoundError:
            myDB.sync()
            myDB.close()
            raise ReferenceTableExistenceError()
        
    # put current table name to table list
    myDB.delete(b'table_name')
    table_list.append(table_name)
    myDB.put(b'table_name', pickle.dumps(table_list))
        
    # create schema and update reference information in database file
    myDB.put((f"{table_name}_ad").encode(), pickle.dumps(column))
    myDB.put((f"{table_name}_pk").encode(), pickle.dumps(pk))
    myDB.put((f"{table_name}_fk").encode(), pickle.dumps(fks))
    myDB.put((f"{table_name}_ref").encode(), pickle.dumps([0]))
    myDB.put((f"{table_name}_ins").encode(), pickle.dumps([]))
    for fk in fks:
        atts, ref_table, ref_atts = fk
        tmp = myDB.get((f"{ref_table}_ref").encode())
        tmp = pickle.loads(tmp)[0]
        myDB.delete((f"{ref_table}_ref").encode())
        tmp += 1
        myDB.put((f"{ref_table}_ref").encode(), pickle.dumps([tmp]))
    
    # print message
    myDB.sync()
    myDB.close()
    return createTableSuccess(table_name)

# drop table query
def drop_table(table_name):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    # find reference condition and check table existance
    try:
        reference_number = myDB.get((f"{table_name}_ref").encode())
        if reference_number is None:
            myDB.sync()
            myDB.close()
            raise NoSuchTable()
        reference_number = pickle.loads(reference_number)[0]
        if reference_number > 0:
            myDB.sync()
            myDB.close()
            raise DropReferencedTableError(table_name)
    except db.DBNotFoundError:
        myDB.sync()
        myDB.close()
        raise NoSuchTable()
    
    # delete all information about integrity condition
    tmp_fk = myDB.get((f"{table_name}_fk").encode())
    if pickle.loads(tmp_fk) is not None:
        for keys, ref_table_name, num in pickle.loads(tmp_fk):
            tmp_ref = myDB.get((f"{ref_table_name}_ref").encode())
            tmp_ref = pickle.loads(tmp_ref)[0]
            myDB.delete((f"{ref_table_name}_ref").encode())
            tmp_ref -= 1
            myDB.put((f"{ref_table_name}_ref").encode(), pickle.dumps([tmp_ref]))
    for file_type in {"_ad", "_pk", "_fk", "_ref", "_ins"}:
        myDB.delete((table_name+file_type).encode())
    
    # remove table from table list in database file
    tables = pickle.loads(myDB.get(b'table_name'))
    myDB.delete(b'table_name')
    tables.remove(table_name)
    myDB.put(b'table_name', pickle.dumps(tables))
    
    myDB.sync()
    myDB.close()
    return dropSuccess(table_name)

# explain, describe, desc query
def explain_describe_desc(table_name):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    # check table existance
    try:
        table_schema = myDB.get((f"{table_name}_ad").encode())
        if table_schema is None:
            myDB.sync()
            myDB.close()
            raise NoSuchTable()
        table_schema = pickle.loads(table_schema)
    except db.DBNotFoundError:
        myDB.sync()
        myDB.close()
        raise NoSuchTable()
    
    # find primary and foreign key data
    pk = myDB.get((f"{table_name}_pk").encode())
    pk = pickle.loads(pk)
    fk = []
    tmp_fk = myDB.get((f"{table_name}_fk").encode())
    if pickle.loads(tmp_fk) is not None:
        for keys, ref_table_name, num in pickle.loads(tmp_fk):
            for key in keys:
                fk.append(key)  
    
    # get data from table schema and print table's information
    print("-----------------------------------------------------------------")
    print(f"table_name [{table_name}]")
    print("{:<25} {:<12} {:<7} {:<10}".format("column_name", "type", "null", "key"))
    for col_name in table_schema:
        col_schema = table_schema[col_name]
        type_info = ""
        null_info = ""
        key_info = ""
        if col_schema[0] == "char":
            type_info = f"char({col_schema[1]})"
        else:
            type_info = col_schema[0]
        if col_schema[2]:
            null_info = 'Y'
        else:
            null_info = 'N'
        if col_name in pk and col_name in fk:
            key_info = "PRI/FOR"
        elif col_name in pk:
            key_info = "PRI"
        elif col_name in fk:
            key_info = "FOR"
        print("{:<25} {:<12} {:<7} {:<10}".format(col_name, type_info, null_info, key_info))
    print("-----------------------------------------------------------------")
    
    myDB.sync()
    myDB.close()
    return    

# show tables query
def show_tables():
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    # print table name by search all tables in database file
    print("------------------------")
    tables = myDB.get(b'table_name')
    if tables is not None:
        for table_name in pickle.loads(tables):
            print(table_name)
    print("------------------------")
    
    myDB.sync()
    myDB.close()
    return
