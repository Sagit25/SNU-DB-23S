import lark

# implement transformer class using lark
class MyTransformer(lark.Transformer):
    
    # return query_list
    def query_list(self, items):
        return items
    
    # actions for each query (return command)
    def query(self, items):
        if items[0].data == 'create_table_query':
            return 'CREATE TABLE'
        elif items[0].data == 'drop_table_query':
            return 'DROP TABLE'
        elif items[0].data == 'explain_query':
            return 'EXPLAIN'
        elif items[0].data == 'describe_query':
            return 'DESCRIBE'
        elif items[0].data == 'desc_query':
            return 'DESC'
        elif items[0].data == 'insert_query':
            return 'INSERT'
        elif items[0].data == 'delete_query':
            return 'DELETE'
        elif items[0].data == 'select_query':
            return 'SELECT'
        elif items[0].data == 'show_tables_query':
            return 'SHOW TABLES'
        elif items[0].data == 'update_query':
            return 'UPDATE'
        elif items[0].data == 'exit':
            return 'EXIT'
        
    # return command or exit
    def command(self, items):
        if not isinstance(items[0], list): 
            exit()
        else:
            return items[0][0]

# main function
if __name__ == "__main__":
    my_prompt = "DB_2021-15738> "
    
    # open grammar.lark for make sql_parser
    with open('grammar.lark') as file:
        sql_parser = lark.Lark(file.read(), start="command", lexer="basic")

    # transformer instance
    mytransformer = MyTransformer()
    
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
                print(my_prompt + f"'{mytransformer.transform(parse_tree)}' requested")
            except lark.exceptions.UnexpectedInput:
                print(my_prompt + "Syntax error")
                break
