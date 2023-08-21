# file for running data manipulation languages

import lark
import pickle
from berkeleydb import db
from errors import *
from messages import *

myDB = db.DB()
myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
myDB.close()

# insert query
def insert(table_name, column, instance):
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
    
    # specified column case
    if column is not None:
        if len(column) != len(instance):
            myDB.sync()
            myDB.close()
            raise InsertTypeMismatchError()
        table_schema = myDB.get((f"{table_name}_ad").encode())
        table_schema = pickle.loads(table_schema)
        data = []
        for col in column:
            if col not in table_schema:
                myDB.sync()
                myDB.close()
                raise InsertColumnExistenceError(col)
        for col in table_schema:    
            if col not in column and table_schema[col][2] == False:
                myDB.sync()
                myDB.close()
                raise InsertColumnNonNullableError(col)
            elif col not in column:
                data.append('null')
            for idx in range(0, len(column)):
                if col == column[idx]:
                    if len(instance[idx]) == 1 and instance[idx].lower() == 'null':
                        if table_schema[col][2] == False:
                            myDB.sync()
                            myDB.close()
                            raise InsertColumnNonNullableError(col)
                        data.append('null')
                    elif instance[idx][0] == 'str' or instance[idx][0] == 'char':
                        if table_schema[col][0] != 'char':
                            myDB.sync()
                            myDB.close()
                            raise InsertTypeMismatchError()
                        length = int(table_schema[col][1])
                        if len(instance[idx][1]) > length+2:
                            data.append(instance[idx][1][1:length+1])
                        else:
                            data.append(instance[idx][1][1:-1])
                    elif instance[idx][0] == 'int' or instance[idx][0] == 'date':
                        if table_schema[col][0] != instance[idx][0]:
                            myDB.sync()
                            myDB.close()
                            raise InsertTypeMismatchError()
                        data.append(instance[idx][1])
        data_list.append(data)
        myDB.delete((f"{table_name}_ins").encode())
        myDB.put((f"{table_name}_ins").encode(), pickle.dumps(data_list))
        
    # only specified table case
    else:    
        # insert instance to table schema
        table_schema = myDB.get((f"{table_name}_ad").encode())
        table_schema = pickle.loads(table_schema)
        data = []
        if (len(instance) != len(table_schema)):
            myDB.sync()
            myDB.close()
            raise InsertTypeMismatchError()
        for idx in range(0, len(instance)):
            col = list(table_schema.keys())[idx]
            if (not isinstance(instance[idx], list)) and instance[idx].lower() == 'null':
                if table_schema[col][2] == False:
                    myDB.sync()
                    myDB.close()
                    raise InsertColumnNonNullableError(col)
                data.append('null')
            elif table_schema[col][0] == 'char':
                if instance[idx][0] != 'str' and instance[idx][0] != 'char':
                    myDB.sync()
                    myDB.close()
                    raise InsertTypeMismatchError()
                length = int(table_schema[col][1])
                if len(instance[idx][1]) > length+2:
                    data.append(instance[idx][1][1:length+1])
                else:
                    data.append(instance[idx][1][1:-1])
            else:
                if table_schema[col][0] != instance[idx][0]:
                    myDB.sync()
                    myDB.close()
                    raise InsertTypeMismatchError()
                data.append(instance[idx][1])
        data_list.append(data)
        myDB.delete((f"{table_name}_ins").encode())
        myDB.put((f"{table_name}_ins").encode(), pickle.dumps(data_list))
      
    # print messages  
    myDB.sync()
    myDB.close()
    return insertResult()

# delete query
def delete(table_name, delete_condition):
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
        
    # when where clause is not exists
    if delete_condition == None:
        myDB.delete((f"{table_name}_ins").encode())
        myDB.put((f"{table_name}_ins").encode(), pickle.dumps([]))
        del_count = len(data_list)
        myDB.sync()
        myDB.close()
        return deleteResult(del_count)
    
    # where clause exist case
    # get table schema data from database
    tables = myDB.get((f"{table_name}_ad").encode())
    table_schema = []
    if tables is None:
        myDB.put((f"{table_name}_ad").encode(), pickle.dumps(table_schema))
    else:
        table_schema = pickle.loads(tables)
        
    # find data to delete
    remain_data_list = []
    del_count = 0
    for data in data_list:
        if where([table_name], table_schema, data, delete_condition) == 'True':
            del_count += 1
        else:
            remain_data_list.append(data)
    
    # put remain data into database and return
    myDB.delete((f"{table_name}_ins").encode())
    myDB.put((f"{table_name}_ins").encode(), pickle.dumps(remain_data_list))
    myDB.sync()
    myDB.close()
    return deleteResult(del_count)       
    
# select query
def select(select_list, from_table_list, select_condition):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    # check table name already exist
    tables = myDB.get(b'table_name')
    table_list = []
    if tables is not None:
        table_list = pickle.loads(tables)
    table_name_list = []
    for table_name in from_table_list:
        table_name = table_name[0]
        if table_name not in table_list:
            myDB.sync()
            myDB.close()
            raise SelectTableExistenceError(table_name)
        table_name_list.append(table_name)
        
    # create total table instance
    table_count = len(table_name_list)
    total_table_schema = {}
    each_table_data = [[], [], []]
    for idx in range(0, len(table_name_list)):
        table_name = table_name_list[idx]
        table_schema = myDB.get((f"{table_name}_ad").encode())
        table_schema = pickle.loads(table_schema)
        for key, value in table_schema.items():
            key = table_name+"."+key
            total_table_schema[key] = value
        table_data = myDB.get((f"{table_name}_ins").encode())
        table_data = pickle.loads(table_data)
        each_table_data[idx] = table_data
    total_table_data = []
    if table_count == 1:
        total_table_data = each_table_data[0]
    elif table_count == 2:
        for col1 in each_table_data[0]:
            for col2 in each_table_data[1]:
                total_table_data.append(col1+col2)
    elif table_count == 3:
        for col1 in each_table_data[0]:
            for col2 in each_table_data[1]:
                for col3 in each_table_data[2]:
                    total_table_data.append(col1+col2+col3)
    
    # if where clause exist, check instaces which allow where clause   
    if select_condition != None: 
        
        # check instance allow where clause  
        new_total_table_data = []
        for data in total_table_data:
            if where(table_name_list, total_table_schema, data, select_condition) == 'True':
                new_total_table_data.append(data)
                
        # update total data
        total_table_data = new_total_table_data
        
    # select some columns which appear in select list
    if select_list != []:
        selected_columns = []
        
        # check column name and raise error
        for column in select_list:
            table_name = ""
            table_name_new = ""
            column_name = ""
            if '.' in column:
                table_name = column[0:column.find('.')]
                column_name = column[column.find('.')+1:]
            else:
                column_name = column
            flag = 0
            for idx in range(0, len(table_name_list)):
                table_name_tmp = table_name_list[idx]
                table_schema = myDB.get((f"{table_name_tmp}_ad").encode())
                table_schema = pickle.loads(table_schema)
                if column_name in table_schema:
                    flag += 1
                    table_name_new = table_name_tmp
            if flag == 0 or (flag > 1 and table_name == ""):
                myDB.sync()
                myDB.close()
                raise SelectColumnResolveError(column_name)
            if table_name == "":
                table_name = table_name_new
            selected_columns.append(table_name+"."+column_name)

        # selecting needed data and schema
        new_total_table_schema = []
        new_total_table_data = [[] for i in range(0, len(total_table_data))]
        idx_col = 0
        for column in total_table_schema:
            if column not in selected_columns:
                idx_col += 1
                continue
            new_total_table_schema.append(column)
            for idx in range(0, len(total_table_data)):
                new_total_table_data[idx].append(total_table_data[idx][idx_col])
            idx_col += 1
                
        # update data and schema to selected column list
        total_table_data = new_total_table_data
        total_table_schema = new_total_table_schema

    # print table information and instance
    length = []
    for att in total_table_schema:
        print('+', end='')
        print('-'*(len(att)+20), end='')
        length.append(len(att)+18)
    print('+')
    for att in total_table_schema:
        print(f"|          {att}          ", end='')
    print('|')
    for att in total_table_schema:
        print('+', end='')
        print('-'*(len(att)+20), end='')
    print('+')
            
    # print all instance data
    for data in total_table_data:
        for idx in range(0, len(data)):
            print(f"| {data[idx]} ", end='')
            if len(data[idx]) < length[idx]:
                print(' '*(length[idx]-len(data[idx])), end='')
        print('|')       
    for att in total_table_schema:
        print('+', end='')
        print('-'*(len(att)+20), end='')
    print('+')
    
    myDB.sync()
    myDB.close()
    return

# evaluate single expression
def where_evaluate(table_name_list, table_schema, data, condition):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    
    # is [not] null case
    if len(condition) == 2:
        idx = get_data_idx(table_name_list, table_schema, condition[0])
        comp_type = "not null"
        if condition[1][1] is None:
            comp_type = "null"
        if data[idx] == 'null' and comp_type == "not null":
            myDB.sync()
            myDB.close()
            return 'False'
        elif data[idx] != 'null' and comp_type == "null":
            myDB.sync()
            myDB.close()
            return 'False'
        else:
            myDB.sync()
            myDB.close()
            return 'True'
    
    # compare using operator
    operand1 = condition[0]
    operator = condition[1]
    operand2 = condition[2]
    
    # both side are constants
    if len(operand1) == 1 and len(operand2) == 1:
        if operand1[0][0] != operand2[0][0]:
            myDB.sync()
            myDB.close()
            raise WhereIncomparableError()
        new_operand1 = operand1[0][1]
        new_operand2 = operand2[0][1]
        if operand1[0][0] == 'str':
            new_operand1 = new_operand1[1:-1]
            new_operand2 = new_operand2[1:-1]
            myDB.sync()
            myDB.close()
            return comp(new_operand1, operator, new_operand2)
        elif operand1[0][0] == 'int':
            myDB.sync()
            myDB.close()
            return comp(int(new_operand1), operator, int(new_operand2))
        else:
            myDB.sync()
            myDB.close()
            new_operand1 = new_operand1[0:4]+new_operand1[5:7]+new_operand1[8:10]
            new_operand2 = new_operand2[0:4]+new_operand2[5:7]+new_operand2[8:10]
            return comp(int(new_operand1), operator, int(new_operand2))
    
    # one side is constant
    elif len(operand1) == 2 and len(operand2) == 1:
        idx = get_data_idx(table_name_list, table_schema, operand1)
        column = list(table_schema.keys())[idx]
        column_type = table_schema[column][0]  
        if data[idx] == 'null':
            myDB.sync()
            myDB.close()
            return 'Unknown'
        if column_type == 'char':
            column_type = 'str'
        if column_type != operand2[0][0]:
            myDB.sync()
            myDB.close()
            raise WhereIncomparableError()
        new_operand2 = operand2[0][1]
        if operand2[0][0] == 'str':
            new_operand2 = new_operand2[1:-1]
            myDB.sync()
            myDB.close()
            return comp(data[idx], operator, new_operand2)
        elif column_type == 'int':
            myDB.sync()
            myDB.close()
            return comp(int(data[idx]), operator, int(new_operand2))
        else:
            myDB.sync()
            myDB.close()
            new_operand1 = data[idx][0:4]+data[idx][5:7]+data[idx][8:10]
            new_operand2 = new_operand2[0:4]+new_operand2[5:7]+new_operand2[8:10]
            return comp(int(new_operand1), operator, int(new_operand2))
        
    elif len(operand1) == 1 and len(operand2) == 2:
        idx = get_data_idx(table_name_list, table_schema,operand2)
        column = list(table_schema.keys())[idx]
        column_type = table_schema[column][0]       
        if data[idx] == 'null':
            myDB.sync()
            myDB.close()
            return 'Unknown'
        if column_type == 'char':
            column_type = 'str'
        if column_type != operand1[0][0]:
            myDB.sync()
            myDB.close()
            raise WhereIncomparableError()
        new_operand1 = operand1[0][1]
        if operand1[0][0] == 'str':
            new_operand1 = new_operand1[1:-1]
            myDB.sync()
            myDB.close()
            return comp(new_operand1, operator, data[idx])
        elif operand1[0][0] == 'int':
            myDB.sync()
            myDB.close()
            return comp(int(new_operand1), operator, data[idx])
        else:
            myDB.sync()
            myDB.close()
            new_operand2 = data[idx][0:4]+data[idx][5:7]+data[idx][8:10]
            new_operand1 = new_operand1[0:4]+new_operand1[5:7]+new_operand1[8:10]
            return comp(int(new_operand1), operator, int(new_operand2))
        
    # both sides are column
    elif len(operand1) == 2 and len(operand2) == 2:
        idx1 = get_data_idx(table_name_list, table_schema, operand1)
        column1 = list(table_schema.keys())[idx1]
        column_type1 = table_schema[column1][0]  
        idx2 = get_data_idx(table_name_list, table_schema, operand2)
        column2 = list(table_schema.keys())[idx2]
        column_type2 = table_schema[column2][0]  
        if data[idx1] == 'null' or data[idx2] == 'null':
            myDB.sync()
            myDB.close()
            return 'Unknown'
        if column_type1 != column_type2:
            myDB.sync()
            myDB.close()
            raise WhereIncomparableError()
        if column_type1 == 'str':
            myDB.sync()
            myDB.close()
            return comp(data[idx1][1:-1], operator, data[idx2][1:-1])
        elif column_type1 == 'int':
            myDB.sync()
            myDB.close()
            return comp(int(data[idx1]), operator, int(data[idx2]))
        else:
            myDB.sync()
            myDB.close()
            new_operand1 = data[idx1][0:4]+data[idx1][5:7]+data[idx1][8:10]
            new_operand2 = data[idx2][0:4]+data[idx2][5:7]+data[idx2][8:10]
            return comp(int(new_operand1), operator, int(new_operand2))
        
# get data from data instance
def get_data_idx(table_name_list, table_schema, condition):
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    col_table = ""
    if condition[0] is not None:
        col_table = condition[0].value.lower()
    col_name = condition[1].value.lower()
    flag_table = 0
    flag_col = 0
    table_name = "" 
    for table_name_new in table_name_list:
        table_schema_new = myDB.get((f"{table_name_new}_ad").encode())
        table_schema_new = pickle.loads(table_schema_new)
        if col_name in table_schema_new:
            flag_col += 1
            table_name = table_name_new
        if col_table == table_name_new:
            flag_table += 1

    # check errors
    if flag_table == 0 and (col_table != ""):
        myDB.sync()
        myDB.close()
        raise WhereTableNotSpecified()
    if flag_col == 0:
        myDB.sync() 
        myDB.close()
        raise WhereColumnNotExist()
    if flag_table >= 2 or (flag_col >= 2 and col_table == ""):
        myDB.sync()
        myDB.close()
        raise WhereAmbiguousReference()
        
    # calculate result
    if col_table == "":   
        col_table = table_name
    idx = 0
    for cc in table_schema:
        if cc == col_name or cc == col_table+"."+col_name:
            break
        idx += 1
    return idx

# loop for multiple expressions
def where(table_name, table_schema, data, condition):
    while len(condition) == 1:
        condition = condition[0]
    if condition[0] == 'or':
        flag = 'False'
        for idx in range(1, len(condition)):
            if where(table_name, table_schema, data, condition[idx]) == 'True':
                flag = 'True'
                break
            if where(table_name, table_schema, data, condition[idx]) == 'Unknown':
                flag = 'Unknown'
        return flag
    elif condition[0] == 'and':
        flag = 'True'
        for idx in range(1, len(condition)):
            if where(table_name, table_schema, data, condition[idx]) == 'False':
                flag = 'False'
                break
            if where(table_name, table_schema, data, condition[idx]) == 'Unknown':
                flag = 'Unknown'
        return flag
    elif condition[0] == 'not':
        flag = 'False'
        if where(table_name, table_schema, data, condition[1]) == 'True':
            flag = 'False'
        elif where(table_name, table_schema, data, condition[1]) == 'False':
            flag = 'True'
        else:
            flag = 'Unknown'
        return flag
    else:
        flag = where_evaluate(table_name, table_schema, data, condition)
        return flag
    
# compare function
def comp(operand1, operator, operand2):
    if operator == '>':
        if operand1 > operand2: 
            return 'True'
        else:
            return 'False'
    elif operator == '<':
        if operand1 < operand2: 
            return 'True'
        else:
            return 'False'
    elif operator == '>=':
        if operand1 >= operand2: 
            return 'True'
        else:
            return 'False'
    elif operator == '<=':
        if operand1 <= operand2: 
            return 'True'
        else:
            return 'False'
    elif operator == '=':
        if operand1 == operand2: 
            return 'True'
        else:
            return 'False'
    elif operator == '!=':
        if operand1 != operand2: 
            return 'True'
        else:
            return 'False'
    
