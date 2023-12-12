import pandas as pd

string_store = []

for i in range(1,60000):
    string = "https://thinkhazard.org/en/report/"
    string += str(i)
    string_store.append(string)

df = pd.DataFrame(string_store, columns=['url'])
#print(df)

df.to_csv('new_url_sequence.csv', index = False)

