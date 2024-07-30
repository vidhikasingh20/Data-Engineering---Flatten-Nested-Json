import pandas as pd
import json
from pandas import json_normalize

# read sample unflattened data
file_path = r'C:\Users\VidhikaSingh\Downloads\sample_data.csv'
df = pd.read_csv(file_path)
df = df[['id', 'created_at', 'updated_at', 'status', 'knockout', 'score_card', 'loan_offers_list', 'stpl_knockout', 'stpl_score']]
lst = ['knockout', 'score_card', 'loan_offers_list', 'stpl_knockout', 'stpl_score']
df.fillna("NULL", inplace=True)
# print(df)
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    # print("flatten json", out)
    return out

complete_df = pd.DataFrame()
for index, row in df.iterrows():
    row_df = pd.DataFrame()
    # row_data = pd.DataFrame({"id": row['id']},{"created_at": row['created_at']},{"updated_at": row['updated_at']},{"status": row['status']}, index=id)
    row_data = pd.DataFrame({
        "id": [row['id']],
        "created_at": [row['created_at']],
        "updated_at": [row['updated_at']],
        "status": [row['status']]
    }, index=[row['id']] )
    row_df = pd.concat([row_df, row_data])

    for i in lst:
        json_data = row[i]
        try:
            json_object = json.loads(json_data)
            banks_data = flatten_json(json_object)
            banks_df = json_normalize(banks_data)
            banks_df['id'] = row['id']

            # print(banks_df['id'])
            row_df = row_df.merge(banks_df, on='id')

            # print(row_df)
        except json.JSONDecodeError as e:
            pass

    complete_df = pd.concat([complete_df, row_df], ignore_index=True)
print(complete_df)
complete_df.to_csv(r'C:\Users\VidhikaSingh\Downloads\complete_data.csv', index=False)

