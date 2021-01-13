import sqlparse. copy
import re, sys, os

path = './files/'
column_to_table_mapping = {}
group_by_column = None
order_by_column = None

def duplicate(List, n):
    return [ele for ele in List for _ in range(n)]

def check_validity(column_to_table_mapping, columns):
    for col in columns:
        if(col == 'and' or col == 'or'):
            continue
        if col not in column_to_table_mapping:
            print("The column {} is not present".format(col))
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
    column_to_table_mapping = {}
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
                    database[table][column] = []
                    database[table]['schema'].append(column)
                    column_to_table_mapping[column] = table
    

    for table in tables:
        table_path = path + str(table) + '.csv'
        if(os.exists == False):
            print("The table {} doesnt exis".format(str(table)))
            sys.exit()

        with open(table_path) as table_file:
            for line in table_file:
                line = line.replace('\n', '')
                row = line.split(',')
                col_index = 0
                for column in database[table]['schema']:
                    database[table][column].append( int(row[col_index]) )
                    col_index += 1
    
    return database

def sql_parser(query):
    tables = []
    columns = []
    keywords = []
    conditions = None
    position = 0
    # remove semicolon
    statement_object = sqlparse.parse(query)[0]
    for token in statement_object.tokens:
        #Ignoring Whitespaces
        if(str(token.ttype) == 'Token.Text.Whitespace'):
            continue

        #getting keywords
        elif(str(token.ttype) == 'Token.Keyword' or str(token.ttype) == 'Token.Keyword.DML'):
            keywords.append(str(token))

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
        
        elif(position > 3 and str(token.ttype) == 'None'):
            if(group_by_column == None and ('group' in keywords or 'GROUP' in keywords):
                group_by_column = str(token).strip()
            else:
                order_by_column = str(token).strip()

        if(len(tables) == 0):
        print("No tables have been provided")
        sys.exit()
    
    if(len(keywords) == 0):
        print("Query is syntactically incorrect")
    
    return tables, columns, conditions, keywords 

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
                result[column] = duplicate(result[column], n)
        
        for column in table['schema']:
            result[column] = []
            if( len(table[column]) ):
                result[column] = duplicate(table[column], m)
                result['schema'].append(column)
    
    return result

def apply_where_condition(table, conditions):
    if(len(conditions) == 0):
        return table 
    
    #Check validity of conditions
    conditions = conditions.replace('AND', 'and').replace('OR', 'or')
    only_cols =  re.compile('[A-Za-z]+').findall(text)
    check_validity(column_to_table_mapping, only_cols)

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

def aggregation_and_groupby(table, columns, group_by_column = None):
    aggregation_operators = ['max', 'min', 'sum', 'average', 'count']
    
    #Replace upper case operators to lower case
    for index in range(len(columns)):
        col = columns[index]
        for operator in aggregation_operators:
            to_replace = operator.upper()
            col = col.replace(to_replace, operator, col)
        columns[index] = col
    
    #separate the columns in which aggregation is being applied, also preprocessa and prepare a list of new columns
    processed_columns = []
    aggregation_columns = []
    column_to_operation_mapping = {}
    for column in columns:
        for operator in aggregation_operators:
            if operator in column:
                column = column.replace(operator + '(', '', column).replace(')', '', column)
                column_to_operation_mapping[column] = operator
                aggregation_columns.append(column)
                break
        processed_columns.append(column)

    #check validity of processed columns
    check_validity(column_to_table_mapping, processed_columns)

    #if group by is present, make a new table temporarily to do processing and aggregation
    if(group_by_column):
        #Error handling
        if group_by_column not in processed_columns:
            print("Use group by column in the select statement also")
            sys.exit()
        
        if len(aggregation_columns) == 0:
            print("Give some aggregation columns when applying groupby")
            sys.exit()

        m = len(table[ table['schema'] ][0])
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
                temp_dict[key][col_key] = apply_aggregation(temp_dict[key], col_key, column_to_operation_mapping[col_key])

        #write everything back into a new table
        result = {}
        result['schema'] = processed_columns
        for key in list(temp_dict.keys()):
            result[group_by_column].append(key)
            for col_key in list(temp_dict[key].keys()):
                result[col_key].append(temp_dict[key][col_key])

    # else just apply aggregation
    elif(len(aggregation_columns)):
        for col in aggregation_columns:
            table[col] = apply_aggregation(table, col, column_to_operation_mapping[col_key])
        
        return table, columns

    return result, processed_columns

def apply_orderby(table, columns, order_by_column = None):
    if order_by_column == None:
        return table
    
    m = len(table[ table['schema'][0] ])
    rank_list = [i for i in range(m)]
    rank_list = [x for _, x in sorted(zip(table[order_by_column],rank_list), key=lambda pair: pair[0])]
    table[order_by_column] = sort(table[order_by_column])

    for col in columns:
        temp_list = [0 for i in range(m)]
        for position,element in enumerate(table[col]):
            temp_list[rank_list[position]] = element
        table[col] = cop.deep_copy(temp_list)

    return table

def apply_distinct:
    pass

def execute_query(query):
    #parse the given query
    tables, columns, conditions, keywords = sql_parser(query)

    #build the database from only the tables present in the query
    database = build_database(tables)

    #Join the above mentioned tables
    joined_table = join_tables(tables, database)

    #apply where condition
    result = apply_where_condition(joined_table, conditions)

    #apply aggregation and group by
    result, columns = aggregation_and_groupby(result, columns, group_by_column)

    ## apply distinct

    #apply order by
    result = apply_orderby(table, columns, order_by_column)
   
    #print the table on the screen
    print_table(result, column_to_table_mapping, columns)

    return result

if __name__ == "__main__":
    query = "SELECT max(A) FROM table1 where A>B"
    result = execute_query(query)

