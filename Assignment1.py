import sqlparse, copy
import re, sys, os

path = './test_dataset/'
column_to_table_mapping = {}

def duplicate(List, n, flag):
    if(flag):
        temp_list = copy.deepcopy(List)
        return temp_list * n
    return [ele for ele in List for _ in range(n)]

def verify_keywords(keywords):
    keyword_string = ' '.join(keywords)

    keywords_to_check = ['SELECT', 'FROM']
    for key in keywords_to_check:
        if not re.search(key, keyword_string, re.IGNORECASE):
            print('{} is not present'.format(key))
            sys.exit()
    return
    
def check_validity(column_to_table_mapping, columns, tables = None, database = None):
    all_cols = []
    for table in tables:
        all_cols.extend(database[table]['schema'])
    all_cols_string = ' '.join(all_cols)
    
    for col in columns:
        if(col == 'and' or col == 'or'):
            continue
        if col not in all_cols_string:
            print("The column {} is not present in any of the tables".format(col))
            sys.exit()
    return

def print_table(table, column_to_table_mapping, column_list = None):
    if column_list is None:
        column_list = table['schema']
    
    print(",".join([column_to_table_mapping[col] + '.' + col for col in column_list]))
    
    m = len(table[ table['schema'][0] ])
    for row_index in range(m):
        print(",".join([str(table[col][row_index]) for col in column_list]))
    
    return

def print_database(database):
    pass

def build_database(tables):
    database = {}
    with open( path + 'metadata.txt', 'r') as table_file:
        new_table_started = False
        for line in table_file:
            if '<begin_table>' in line:
                new_table_started = True
            
            elif '<end_table>' in line:
                new_table_started = False
            
            else:
                if(new_table_started):
                    table = line.replace('\n', '')
                    database[table] = {}
                    database[table]['schema'] = []
                    new_table_started = False
                else:
                    column = line.replace('\n', '')
                    column = column.lower()
                    database[table][column] = []
                    database[table]['schema'].append(column)
    
    # Check if the tables present in query are correct
    all_files_string = ' '.join(os.listdir(path))
    for table in tables:
        match_object = re.search(table, all_files_string, re.IGNORECASE)
        if match_object:
            matched_name = match_object[0]
        else:
            print("The table {} doesnt exist".format(str(table)))
            sys.exit()
        
        table_path = path + matched_name + '.csv'
        
        with open(table_path) as table_file:
            for line in table_file:
                line = line.replace('\n', '')
                row = line.split(',')
                col_index = 0
                for column in database[matched_name]['schema']:
                    database[matched_name][column].append( int(row[col_index]) )
                    col_index += 1
            
            if(matched_name != table):
                database[table.lower()] = database.pop(matched_name)
    
    #Make a Column to table mapping
    for table in database.keys():
        for col in database[table]['schema']:
            column_to_table_mapping[col] = table
    
    return database, tables

def sql_parser(query):
    tables = []
    columns = []
    keywords = []
    conditions = None
    position = 0

    if ';' not in query:
        print("Semicolon is missing from query")
        sys.exit()

    # remove semicolon
    query = query.replace(';','')
    #Remove extra white spaces
    query = ' '.join(query.split())

    #parse the query
    statement_object = sqlparse.parse(query)[0]
    all_tokens = statement_object.tokens
    for token in all_tokens:
        #Ignoring Whitespaces
        if(str(token.ttype) == 'Token.Text.Whitespace'):
            continue

        #getting keywords
        elif(str(token.ttype) == 'Token.Keyword' or str(token.ttype) == 'Token.Keyword.DML'):
            keywords.append(str(token))
            #To avoid group by and order by columns in conditions
            if(position == 2):
                position += 1

        #getting columns
        elif(position == 0 and (str(token.ttype) == 'None' or str(token.ttype) == 'Token.Wildcard') ):
            position += 1
            columns = str(token.value).split(',')
            columns = [col.strip() for col in columns]
        
        #getting tables
        elif(position == 1 and str(token.ttype) == 'None'):
            position += 1
            tables = str(token.value).split(',')
            tables = [table.strip() for table in tables]

        #get conditions
        elif(position == 2 and str(token.ttype) == 'None'):
            position += 1
            conditions = str(token).replace('WHERE' , '')
            conditions = str(token).replace('where' , '')
            conditions = conditions.strip()
    
    #convert the cols to lower case
    columns = [col.lower() for col in columns]
    tables = [table.lower() for table in tables]
    all_tokens = [str(key).lower() if str(key) in keywords else str(key) for key in all_tokens]
    
    ## Verify Keywords
    verify_keywords(keywords)

    group_by_column = None
    order_by_column = None
    order_by_nature = None
    if 'group by' in all_tokens:
        Index = all_tokens.index('group by')
        if(Index + 2 >= len(all_tokens)):
            print("You have not provided a group by column")
            sys.exit()
        
        group_by_column = all_tokens[Index+2]
        group_by_column = group_by_column.lower()
    
    if 'order by' in all_tokens:
        Index = all_tokens.index('order by')
        if(Index + 2 >= len(all_tokens)):
            print("You have not provided a order by column")
            sys.exit()

        order_by_column = all_tokens[Index+2].split()[0]
        order_by_column = order_by_column.lower()

        if(len(all_tokens[Index+2].split(' ')) < 2 ):
            print("Provide ASC or DESC with order by")
            sys.exit()
        order_by_nature =  all_tokens[Index+2].split()[1]

    if(len(tables) == 0):
        print("No tables have been provided")
        sys.exit()
    
    if(len(keywords) == 0):
        print("Query is syntactically incorrect")
    
    return tables, columns, conditions, keywords, group_by_column, order_by_column, order_by_nature

def preprocess_columns(columns, group_by_column, order_by_column, tables, database, column_to_table_mapping):
    #Replace * with all the columns of the table
    if '*' in columns:
        Index = columns.index('*')
        columns.remove('*')
        columns.extend(database[tables[Index]['schema']])

    aggregation_operators = ['max', 'min', 'sum', 'average', 'count']
    #Replace upper case operators to lower case
    for index in range(len(columns)):
        col = columns[index]
        for operator in aggregation_operators:
            to_replace = operator.upper()
            col = col.replace(to_replace, operator)
        columns[index] = col
    
    #separate the columns in which aggregation is being applied, also preprocess and prepare a list of new columns i.e.
    #processed_columns which contains both normal and aggregation columns
    processed_columns = []
    aggregation_columns = []
    column_to_aggOperation_mapping = {}
    for column in columns:
        for operator in aggregation_operators:
            if operator in column:
                column = column.replace(operator + '(', '').replace(')', '')
                column_to_aggOperation_mapping[column] = operator
                aggregation_columns.append(column)
                break
        processed_columns.append(column)
    
    processed_columns = [col.lower() for col in processed_columns]
    aggregation_columns = [col.lower() for col in aggregation_columns]
    
    if(group_by_column):
        for column in processed_columns:
            if(column == group_by_column):
                continue
            else:
                if column not in aggregation_columns:
                    print("Aggregation operation has to be applied on column {}".format(column))
                    sys.exit()
        
        if len(aggregation_columns) == 0:
            print("Give some aggregation columns when applying groupby")
            sys.exit()
    
    #check validity of processed columns
    check_validity(column_to_table_mapping, processed_columns, tables, database)

    return processed_columns, aggregation_columns, column_to_aggOperation_mapping
    
def join_tables(tables, database):
    #initialize the result with first table
    result = database[tables[0]]

    #join the result with rest of the tables one by one
    for i in range(1, len(tables)):
        table = database[tables[i]]
        m = len(result[ result['schema'][0] ])
        n = len(table[ table['schema'][0] ])

        for column in result['schema']:
            if( len(result[column]) ):
                result[column] = duplicate(result[column], n, 0)
        
        for column in table['schema']:
            result[column] = []
            if( len(table[column]) ):
                result[column] = duplicate(table[column], m, 1)
                result['schema'].append(column)
    
    return result

def apply_where_condition(table, tables, database, conditions):
    if(conditions == None):
        return table 
    
    #Check validity of conditions
    conditions = conditions.replace('AND', 'and').replace('OR', 'or')
    conditions = conditions.replace('=', '==')
    only_cols =  re.compile('[A-Za-z]+').findall(conditions)
    check_validity(column_to_table_mapping, only_cols, tables, database)

    #dict where final result will be stored
    result = {}
    result['schema'] = table['schema']
    for col in result['schema']:
        result[col] = []

    m = len(table[ table['schema'][0] ])

    for row_index in range(m):
        local_dict = {}
        for col in table['schema']:
            if col in conditions:
                local_dict[col] = table[col][row_index]
        
        include_this_row = eval(conditions, local_dict)

        if(include_this_row):
            for col in table['schema']:
                result[col].append(table[col][row_index])
    
    return result

def apply_aggregation(table, column, operation):
    if operation == 'average':
        return eval("sum" + '(' + column + ')', {column: table[column]}) / len(table[column])
    elif operation == 'count':
        return len(table[column])
    
    return eval(operation + '(' + column + ')', {column: table[column]})

def aggregation_and_groupby(table, columns, aggregation_columns, column_to_aggOperation_mapping, group_by_column = None):
   
   #if group by is present, make a new table temporarily to do processing and aggregation
    if(group_by_column):
        #Error handling is being done in preprocess_columns function
        
        m = len(table[ table['schema'][0] ])
        temp_dict = {}
        for row_index in range(m):
            key = table[group_by_column][row_index]
            if key not in temp_dict:
                temp_dict[key] = {}
            for col in aggregation_columns:
                if col not in temp_dict[key]:
                    temp_dict[key][col] = []
                temp_dict[key][col].append(table[col][row_index])
        
        for key in list(temp_dict.keys()):
            for col_key in list(temp_dict[key].keys()):
                temp_dict[key][col_key] = apply_aggregation(temp_dict[key], col_key, column_to_aggOperation_mapping[col_key])

        #write everything back into a new table
        result = {}
        result['schema'] = columns
        for col in result['schema']:
            result[col] = []
        for key in list(temp_dict.keys()):
            result[group_by_column].append(key)
            for col_key in list(temp_dict[key].keys()):
                result[col_key].append(temp_dict[key][col_key])
        
        return result, columns

    # else just apply aggregation
    elif(len(aggregation_columns)):
        result = {}
        result['schema'] = aggregation_columns
        for col in result['schema']:
            result[col] = []
        for col in aggregation_columns:
            result[col].append(apply_aggregation(table, col, column_to_aggOperation_mapping[col]))
        
        return result, aggregation_columns

    return table, columns

def apply_orderby(table, columns, order_by_column = None, order_by_nature = None):
    if order_by_column == None:
        return table
    
    order_by_nature = order_by_nature.upper()
    order_by_operators = ['ASC', 'DESC', 'asc', 'desc']

    #Error handling
    if order_by_nature not in order_by_operators:
        print("The spelling of {} is wrong".format(order_by_nature))
        sys.exit()
    
    m = len(table[ table['schema'][0] ])
    rank_list = [i for i in range(m)]
    rank_list = [x for _, x in sorted(zip(table[order_by_column],rank_list), key=lambda pair: pair[0], reverse = (order_by_nature == 'DESC') )]
    table[order_by_column] = sorted(table[order_by_column], reverse = (order_by_nature == 'DESC'))

    temp_list = [0 for i in range(m)]
    for col in columns:
        if(col == order_by_column):
            continue
        for position,element in enumerate(table[col]):
            temp_list[rank_list[position]] = element
        table[col] = copy.deepcopy(temp_list)

    return table

def apply_distinct(result, columns, keywords):
    if 'distinct' in keywords or 'DISTINCT' in keywords:
        m = len(result[ result['schema'][0] ])
        lists = []
        for row_index in range(m):
            temp = []
            for col in columns:
                temp.append(result[col][row_index])
            lists.append(list(temp))
        
        unique_data = [list(x) for x in set(tuple(x) for x in lists)]
        table = {}
        table['schema'] = columns
        for col in columns:
            table[col] = []
        
        for row in unique_data:
            for col_index,col in enumerate(table['schema']):
                table[col].append(row[col_index])
        
        return table
    
    return result

def execute_query(query):
    #parse the given query
    tables, columns, conditions, keywords, group_by_column, order_by_column, order_by_nature = sql_parser(query)

    #build the database from only the tables present in the query and also build a column to table dictionary
    database, tables = build_database(tables)

    #Check validity of columns, process the columns
    columns, aggregation_columns, column_to_aggOperation_mapping = preprocess_columns(columns, group_by_column, order_by_column, tables, database, column_to_table_mapping)

    #Join the above mentioned tables
    joined_table = join_tables(tables, database)

    #apply where condition
    result = apply_where_condition(joined_table, tables, database, conditions)

    #apply aggregation and group by
    result, columns = aggregation_and_groupby(result, columns, aggregation_columns,column_to_aggOperation_mapping, group_by_column)
    
    ## apply distinct
    result = apply_distinct(result, columns, keywords)

    #apply order by
    result = apply_orderby(result, columns, order_by_column, order_by_nature)
    
    #print the table on the screen
    print_table(result, column_to_table_mapping, columns)

    return result

if __name__ == "__main__":
    if(len(sys.argv) < 2 or sys.argv[1] == ''):
        print("You have not provided query as an argument")
        sys.exit()
        
    query = sys.argv[1]
    result = execute_query(query)

