


import pandas as pd
import time
from elasticsearch import Elasticsearch

# Record the start time
start_time = time.time()

# Read CSV files into DataFrames

df = pd.read_csv("people-500000.csv")

df['group'] = df.index % 2  # 0 for even, 1 for odd

# Separate even and odd rows into two DataFrames
even_rows = df[df['group'] == 0].reset_index(drop=True)
odd_rows = df[df['group'] == 1].reset_index(drop=True)


merged_df1 = pd.merge(even_rows, odd_rows, on="Phone", how='inner')

print("Part 1")
print(merged_df1)


# Convert 'Date of birth' column to datetime format
df['Date of birth'] = pd.to_datetime(df['Date of birth'])

# Count occurrences for each unique birthday
birthday_counts = df['Date of birth'].value_counts()

# Filter for birthdays that occur exactly once
unique_birthdays_once = birthday_counts[birthday_counts == 1]

# Display the unique birthdays that occur exactly once

print("Part 2")
print(unique_birthdays_once.count())


# Convert 'Date of birth' column to datetime format

# Count occurrences for each unique birthday
birthday_counts = df['Job Title'].value_counts()

# Filter for birthdays that occur exactly once
unique_birthdays_once = birthday_counts[birthday_counts == 1]

# Display the unique birthdays that occur exactly once

print("Part 3")
print(unique_birthdays_once.count())


df['Date of birth'] = pd.to_datetime(df['Date of birth'])

# Group by 'Date of birth' and count the occurrences
birthday_counts = df.groupby('Date of birth').size()

# Filter for birthdays that have more than one person
shared_birthdays = birthday_counts[birthday_counts > 1]

# Display the shared birthdays and their counts

# Display the unique birthdays that occur exactly once

print("Part 4")
print(shared_birthdays)





df['First Name'] = df['First Name']

# Group by 'Date of birth' and count the occurrences
birthday_counts = df.groupby('First Name').size()

# Filter for birthdays that have more than one person
shared_birthdays = birthday_counts[birthday_counts > 1]

# Display the shared birthdays and their counts

# Display the unique birthdays that occur exactly once

print("Part 5")
print(shared_birthdays)




# Record the end time
end_time = time.time()

# Calculate and print the time taken
elapsed_time = end_time - start_time
print(f"Time taken: {elapsed_time:.2f} seconds")
