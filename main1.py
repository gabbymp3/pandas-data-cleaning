import pandas as pd
import os

df = pd.read_csv("jamie_codes_doronized.csv", encoding = 'utf-8')
df['Folder'].ffill(inplace=True)
df['Video_Name'].ffill(inplace=True)



pid_columns = [f'Person_Number_{i}' for i in range(1,13)]

gender_columns = [f'Gender_Presentation_{i}' for i in range(1,13)]


# small df
#folder_name = 'folder_20201225_142054'
#video_name = 'video_0_158_3_Final.mp4'
#file_name = f'{folder_name}/{video_name}.csv'
#df = pd.read_csv(file_name)



'''-----------------------------------------------------------------------------------------
functions
_________________________________________________________________________________________'''


def exclude_column(df, column_to_exclude):
    cols = [col for col in pid_columns if col != column_to_exclude]
    #print(f'excluding {cols}')
    return cols




def corresponding_gender_presentation(df, pid_column):
    pid_number = int(pid_column.split('_')[-1])
    column = f'Gender_Presentation_{pid_number}'
    return column




def forward_fill_column(df, column_to_ffill, columns_to_check):

    previous_value = None
    value_to_fill = None
    skip_first_row = True

    for index, row in df.iterrows():
        #print(f'{index}')

        if pd.notnull(row[column_to_ffill]):                            #find first non-empty value to ffill
            value_to_fill = row[column_to_ffill]
            #gender_column = corresponding_gender_presentation(df, column_to_ffill)
            #pid_gender = row[gender_column]
            stop_filling_pid = False
            #stop_filling_gender = False

            #print(f"Processing row {index}, filling with {value_to_fill}")

            for next_index, next_row in df.iloc[index + 1:].iterrows():
                for col in columns_to_check:

                    #print(f'Checking if value in column {col}')


                    if pd.notnull(next_row[col]) and next_row[col] == value_to_fill:     # stop filling if matching value found in another column
                        stop_filling_pid = True
                        #print(f"Found matching value in column {col}, stopping filling")


                if stop_filling_pid:
                    #print(f"Stopped filling for this row {next_row}")
                    break

                else:
                    df.at[next_index, column_to_ffill] = value_to_fill        # if no matching value found in another column, forward fill next cell
                    #df.at[next_index, gender_column] = pid_gender
                    previous_value = value_to_fill
                    #print(f"Filled empty row {index} with {previous_value}")


'''
def forward_fill_gender(df, column_to_ffill, pid_column):
    value_to_fill = None
    for index, row in df.iterrows():
        if pd.notnull(row[column_to_ffill]):
            value_to_fill = row[column_to_ffill]
    for next_index, next_row in df.iloc[index + 1:].iterrows():
        if pd.isnull(next_row[pid_column]):
            break
        else:
            df.at[next_index, column_to_ffill] = value_to_fill

            #df.at[index:, column_to_ffill] = df.at[index:, column_to_ffill].ffill().fillna(value_to_fill)

'''

def forward_fill_gender(df, column_to_ffill, pid_column):
    value_to_fill = df[column_to_ffill].first_valid_index()  #find index of first gender
    if value_to_fill is None:
        return
    for index,row in df.iterrows():                             #if pid is not null, fill in gender
        if pd.notnull(row[pid_column]):
            df.at[index, column_to_ffill] = df.at[value_to_fill, column_to_ffill]



def forward_fill_pids_gender(df):
    for pid in range(1,13):
        #print(f'Forward filling pid {pid}')
        forward_fill_column(df, f'Person_Number_{pid}', columns_to_check = exclude_column(df, f'Person_Number_{pid}'))
        forward_fill_gender(df, f'Gender_Presentation_{pid}', pid_column = f'Person_Number_{pid}')
    #df.to_csv('four_filled.csv', index=False, na_rep='NA')


def find_duplicate_pids(df):
    lines_with_dupes = []
    for index, row in df.iterrows():
        valid_pids = [pid for pid in row['combined_pids'].split(' - ') if '.' in pid]
        if len(valid_pids) != len(set(valid_pids)):
            lines_with_dupes.append(row['Unnamed: 0'])
            print(f'Duplicate found at line {row["Unnamed: 0"]}')
    return lines_with_dupes


#__________________________________________________________________________________________________________



# group into individual videos

grouped_data = df.groupby(['Folder', 'Video_Name'])
smaller_dfs = []

for group_key, group_df in grouped_data:
    folder, video_name = group_key

    # create directory
    folder_path = f'new_folder_{folder}'
    os.makedirs(folder_path, exist_ok=True)

    # save individual files
    file_name = f'{folder_path}/video_{video_name}.csv'
    group_df.to_csv(file_name, index=False)

    group_df = pd.read_csv(file_name, encoding = 'utf-8')
    forward_fill_pids_gender(group_df)
    group_df['combined_pids'] = group_df[pid_columns].astype(str).agg(' - '.join, axis=1)
    #forward_fill_gender(group_df)

    smaller_dfs.append(group_df)




# concat smaller dfs and sort by first column to preserve original order
final_df = pd.concat(smaller_dfs)
final_df_sorted = final_df.sort_values(by='Unnamed: 0', ascending=True)
find_duplicate_pids(final_df_sorted)
final_df_sorted.to_csv('feb12.csv', index=False, na_rep='NA')