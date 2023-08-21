import lark
import pickle
from berkeleydb import db
from errors import *
from messages import *
from ddl import *
from dml import *

# implement transformer class using lark
class MyTransformer(lark.Transformer):
    
    # return query_list
    def query_list(self, items):
        return items[0]
    
    # actions for each query (running query)
    def query(self, items):
        return items[0]  
      
    # return command or exit
    def command(self, items):
        if not isinstance(items[0], list): 
            exit()
        else:
            return items[0]

# transformer for each query
class MyQueryTransformer(MyTransformer):
    def create_table_query(self, items):
        return ['CREATE', items[2].value.lower(), items[3]]
    
    def drop_table_query(self, items):
        return ['DROP', items[2].value.lower()]
    
    def explain_query(self, items):
        return ['DESC', items[1].value.lower()]
    
    def describe_query(self, items):
        return ['DESC', items[1].value.lower()]
    
    def desc_query(self, items):
        return ['DESC', items[1].value.lower()]
    
    def insert_query(self, items):
        return ['INSERT', items[2].value.lower(), items[3], items[4]]
    
    def delete_query(self, items):
        return ['DELETE', items[2].value.lower(), items[3]]
    
    def select_query(self, items):
        return ['SELECT', items[1], items[2][0], items[2][1]]
        
    def show_tables_query(self, items):
        return ['SHOW', ]
    
    def update_query(self, items):
        return 'UPDATE'
    
    def exit(self, items):
        return 'EXIT'
        
# add some new parser for easy computing
class MyNewTransformer(MyQueryTransformer):
    def table_name(self, items):
        return items[0]
    
    def table_element(self, items):
        return items[0]
    
    def column_name(self, items):
        return items[0]
    
    def table_element_list(self, items):
        length = len(items)
        return items[1:length-1]
    
    # check not null and types for columns
    def column_definition(self, items):
        if items[2] is not None and items[3] is not None:
            return [items[0].value.lower(), items[1], 'not null']
        else:
            return [items[0].value.lower(), items[1], 'null']
    
    # check data types
    def data_type(self, items):
        if items[0].value.lower() == 'char':
            if int(items[2].value) < 1:
                raise CharLengthError()
            return [items[0].value.lower(), int(items[2].value)]
        else:
            return [items[0].value.lower(), -1]
        
    def column_name_list(self, items):
        length = len(items)
        new_items = []
        for idx in range(1, length-1):
            new_items.append(items[idx].value.lower())
        return new_items
        
    # check primary key
    def primary_key_constraint(self, items):
        return ['pk', items[2]]
    
    # check foreign key
    def referential_constraint(self, items):
        return ['fk', items[2], items[4].value.lower(), items[5]]
    
    def table_constraint_definition(self, items):
        return [['keys'], items[0]]
    
    def value(self, items):
        return items[0]
    
    def comparable_value(self, items):
        return [items[0].type.lower(), items[0].value]
    
    def value_list(self, items):
        length = len(items)
        return items[2:length-1]
    
    # find (from table as table) condition
    def referred_table(self, items):
        ref_table = ""
        if items[2] is not None:
            ref_table = items[2].value.lower()
        return [items[0].value.lower(), ref_table]
    
    def table_reference_list(self, items):
        return items
    
    def from_clause(self, items):
        return items[1]
    
    def select_list(self, items):
        return items
    
    # for where clause
    def comp_operand(self, items):
        return items
    
    def predicate(self, items):
        return items[0]
    
    def where_clause(self, items):
        return items[1]
    
    def comp_op(self, items):
        return items[0].value
    
    def boolean_factor(self, items):
        if items[0] != None and items[0].value.lower() == 'not':
            items[0] = 'not'
        else:
            items = items[1:]
        return items
    
    def comparison_predicate(self, items):
        return items
    
    def boolean_test(self, items):
        return items[0]
    
    def boolean_expr(self, items):
        if len(items) == 1:
            return items
        new_items = ['or']
        for idx in range(0, len(items), 2):
            new_items.append(items[idx])
        return new_items
    
    def boolean_term(self, items):
        if len(items) == 1:
            return items
        new_items = ['and']
        for idx in range(0, len(items), 2):
            new_items.append(items[idx])
        return new_items
    
    def parenthesized_boolean_expr(self, items):
        return items[1]
    
    # for selext query
    def selected_column(self, items):
        table_col = ""
        if items[0] is not None:
            table_col += items[0].value.lower()
            table_col += "."
        table_col += items[1].value.lower()
        return table_col
    
    def table_expression(self, items):
        return items
    
    # for null expression
    def null_predicate(self, items):
        return [[items[0], items[1]], items[2]]
    
    def null_operation(self, items):
        return items

# method for running query
def run(result):
    if result[0] == 'CREATE':
        return create_table(result[1], result[2])
    elif result[0] == 'DROP':
        return drop_table(result[1])
    elif result[0] == 'DESC':
        explain_describe_desc(result[1])
        return None
    elif result[0] == 'SHOW':
        show_tables()
        return None
    elif result[0] == 'INSERT':
        return insert(result[1], result[2], result[3])
    elif result[0] == 'DELETE':
        return delete(result[1], result[2])
    elif result[0] == 'SELECT':
        select(result[1], result[2], result[3])
        return None
    else:
        return None

# main function
if __name__ == "__main__":
    
    # check database files for create or open
    myDB = db.DB()
    myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
    myDB.sync()
    myDB.close()
    
    my_prompt = "DB_2021-15738> "
    
    # open grammar.lark for make sql_parser
    with open('grammar.lark') as file:
        sql_parser = lark.Lark(file.read(), start="command", lexer="basic")

    # transformer instance
    mytransformer = MyNewTransformer()
    
    # parsing query
    while True:
        
        # get input string and translate to query list
        query_list = []
        input_str = input(my_prompt)
        if input_str.strip():
            input_str = input_str.strip()
            while input_str[len(input_str)-1] != ';':
                input_str += ' ' + input();
            for query in input_str.split(';')[0:-1]:
                query_list.append(query+';')
            
        # parsing query using parse_tree
        for query in query_list:
            try:
                parse_tree = sql_parser.parse(query)
                result = mytransformer.transform(parse_tree)
                msg = run(result)
                if msg is not None:
                    print(my_prompt + msg)
            except lark.exceptions.UnexpectedInput:
                print(my_prompt + "Syntax error")
                break
            except lark.exceptions.VisitError as e:
                print(my_prompt + str(e.__context__))
                break
            except DBError as e:
                print(my_prompt + str(e))
                break
