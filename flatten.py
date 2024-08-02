# to import pandas library
import pandas as pd

# to import json module
import json

# helps in flattening a json value
from pandas import json_normalize

# path from where required file can be fetched
file_path = r'C:\Users\VidhikaSingh\Downloads\sample_data.csv'

# to read a csv file in pandas dataframe
df = pd.read_csv(file_path)

# dataframe consisting of columns with unflatten jsons(needs to be flatten) and columns with int values
# df = df[['id', 'created_at', 'updated_at', 'status', 'knockout', 'score_card', 'loan_offers_list', 'stpl_knockout', 'stpl_score']]

# list containing columns with nested unflatten json values
lst = ['knockout', 'score_card', 'loan_offers_list', 'stpl_knockout', 'stpl_score']

# used to fill a "null" where there are empty cells so that the empty cells will not throw any error
df.fillna("NULL", inplace=True)
# print(df)


# flatten_json function is defined to flatten single nested unflattened json
def flatten_json(y):
    # empty dictionary with no values
    out = {}

    # function flatten defined to flatten json where x represent objects to flatten.
    def flatten(x, name=''):
        # if X is a dictionary
        if type(x) is dict:
            # initiates a for loop that iterates over 'a'(represent every element) in an object x
            for a in x:
                # recursively call to flatten function for  all the elements "a" of an object x then 'a' is appended.
                flatten(x[a], name + a + '_')
        #  if X is a list
        elif type(x) is list:
            # initializing i
            i = 0
            # recursively calling for loop to flatten each element
            for a in x:
                # recursively call flatten to flatten 'a' & string is appended
                flatten(a, name + str(i) + '_')

                # increments the value of i by 1
                i += 1
        else:
            out[name[:-1]] = x

    # call function to flatten y
    flatten(y)

    # ends the function call
    return out


# creating an empty dataframe in which  all flattened json will be stored  merged with another columns
complete_df = pd.DataFrame()

# iteration will take place for each row in that dataframe
for index, row in df.iterrows():

    # to create an empty dataframe namely row_df for each row
    row_df = pd.DataFrame()

    # row_data contains columns with no json values in which id is taken as the primary key(unique key) for indexing
    row_data = pd.DataFrame({
        "id": [row['id']],
        "created_at": [row['created_at']],
        "updated_at": [row['updated_at']],
        "status": [row['status']]
    }, index=[row['id']])

    # concatenating the row_df and row_data
    row_df = pd.concat([row_df, row_data])

    #  loop to flatten single value of each column for each row
    for i in lst:

        # the values for json_data will be fetched from  a row
        json_data = row[i]
        try:
            # here json.loads is used to convert the json_data(string of json in json format) to json object
            json_object = json.loads(json_data)

            # flatten_json_value will contain the string of json in json format that needs to be flatten
            flatten_json_value = flatten_json(json_object)

            # flatten_json_df is a dataframe containing flatten_json_value. json normalize will flatten each value of json.
            flatten_json_df = json_normalize(flatten_json_value)

            # id will be taken corresponding to each column in df.
            flatten_json_df['id'] = row['id']

            # row_df will merge with every row of flatten_json_df using id as unique primary key for indexing
            row_df = row_df.merge(flatten_json_df, on='id')

        # this is used to catch errors
        except json.JSONDecodeError as e:
            pass

    # complete_df  in which all the flatten values of every column is concatenated with each row_df, ignore_index=
    complete_df = pd.concat([complete_df, row_df], ignore_index=True)
# to print complete_df and json is flattened
print(complete_df)
# to view a complete dataframe in csv format after flattening by providing the file path.`
complete_df.to_csv(r'C:\Users\VidhikaSingh\Downloads\complete_data.csv', index=False)
