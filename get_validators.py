import requests
import psycopg2


update_query = '''
INSERT INTO shielded_expedition_new.validators_info (address, voting_power, block_height, rank)
VALUES (%s, %s, %s, %s)
ON CONFLICT (address)
DO UPDATE SET
 voting_power = EXCLUDED.voting_power,
 block_height = EXCLUDED.block_height,
 rank = EXCLUDED.rank
WHERE shielded_expedition_new.validators_info.voting_power IS DISTINCT FROM EXCLUDED.voting_power OR
      shielded_expedition_new.validators_info.block_height IS DISTINCT FROM EXCLUDED.block_height OR
      shielded_expedition_new.validators_info.rank IS DISTINCT FROM EXCLUDED.rank;
'''

url = "http://localhost:26657/validators"
pages = ["1", "2", "3", "4"]
headers = {"accept": "application/json"}
insert_values = []
rank = 1

for page in pages:
    params = {"per_page": "100", "page": page}

    try:
        response = requests.get(url, params=params, headers=headers)
    except:
        continue

    if response.status_code == 200:
        response = response.json()
    else:
        print('Error:', response.status_code)
        print(response)
        continue

    val_info = response["result"]["validators"]
    block_height = response["result"]["block_height"]

    for info in val_info:
        addr = info["address"]
        vp = round(int(info["voting_power"])/ 1_000_00)
        insert_values.append((addr, vp, block_height, rank))
        print((addr, vp, block_height, rank))
        rank += 1

try:
    con = psycopg2.connect(
    database="<YOUR_DATABASE>",
    user="postgres",
    password="<YOUR_PASSWORD>",
    host="localhost",
    port= '5432'
    )

    cursor_obj = con.cursor()
    cursor_obj.execute('''
                        CREATE TABLE IF NOT EXISTS shielded_expedition_new.validators_info (
                            address VARCHAR(255) PRIMARY KEY,
                            voting_power BIGINT,
                            block_height BIGINT,
                            rank BIGINT
                        );
                    ''')
    
    cursor_obj.executemany(update_query, insert_values)
    con.commit()
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)


if con:
    cursor_obj.close()
    con.close()
    print("PostgreSQL connection is closed")
