import requests
import psycopg2


update_query = '''
INSERT INTO shielded_expedition_new.validators_info (address, voting_power, block_height, rank, moniker, tendermint_address)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (address)
DO UPDATE SET
 voting_power = EXCLUDED.voting_power,
 block_height = EXCLUDED.block_height,
 rank = EXCLUDED.rank,
 moniker = EXCLUDED.moniker,
 tendermint_address = EXCLUDED.tendermint_address
WHERE shielded_expedition_new.validators_info.voting_power IS DISTINCT FROM EXCLUDED.voting_power OR
      shielded_expedition_new.validators_info.block_height IS DISTINCT FROM EXCLUDED.block_height OR
      shielded_expedition_new.validators_info.moniker IS DISTINCT FROM EXCLUDED.moniker OR
      shielded_expedition_new.validators_info.tendermint_address IS DISTINCT FROM EXCLUDED.tendermint_address OR
      shielded_expedition_new.validators_info.rank IS DISTINCT FROM EXCLUDED.rank;
'''

url = "http://localhost:26657/validators"
pages = ["1", "2", "3", "4"]
headers = {"accept": "application/json"}
insert_values = []
rank = 1

response = {"jsonrpc":"2.0","id":-1,"result":{"block_height":"90044","validators":[{"address":"2C8300C6D4EE7F9641963DF7B7F53391CC172CB0","pub_key":{"type":"tendermint/PubKeyEd25519","value":"M5cVI72wX67qbBWmEVbOZqXmUFs8WelMMKb7dpEn7TM="},"voting_power":"2692877300000","proposer_priority":"47342885438620"},{"address":"3F2D9EC60C67480619DF90B08F013480EBCBE219","pub_key":{"type":"tendermint/PubKeyEd25519","value":"zjs8kDXM3TmFXGaAR6xjymzT5R/u8FO7gdmy8AHBEJI="},"voting_power":"2680920390000","proposer_priority":"-5533958664913"},{"address":"52894D2ABA1614EF24CC1DDAE127A7A2386DE3BB","pub_key":{"type":"tendermint/PubKeyEd25519","value":"R+SsQftv6RIApI0+NjbyKiMA0ywXupz0pswDG1jKWXo="},"voting_power":"2672418560000","proposer_priority":"78753334975493"},{"address":"0E1CFCF387B1CDBCD642323F3B94E67D6B2F931A","pub_key":{"type":"tendermint/PubKeyEd25519","value":"o8/pRRN+IOFMymQc+I/b7piOk9p89egdJKPmWEA1Wgo="},"voting_power":"2669206230000","proposer_priority":"-56377886196034"},{"address":"DCF55C0FC98CD4DBC08AE906883EA33ADAA35F11","pub_key":{"type":"tendermint/PubKeyEd25519","value":"f9NyXv70EJL7uD0sh15bXnD1gh5CHRWw0vooqwfdm7o="},"voting_power":"2661564778300","proposer_priority":"2717601874970"},{"address":"1586E894E66E6F7ED162DFA94318A7DAB23FBD2F","pub_key":{"type":"tendermint/PubKeyEd25519","value":"XxeAKwvUVUku56S+FO7xsQdxKcURzeMkcjM/BkExzTA="},"voting_power":"2661454015902","proposer_priority":"-16531309411569"},{"address":"35351448982F24250484F18C35E982AC5EACD145","pub_key":{"type":"tendermint/PubKeyEd25519","value":"t9ZcAGw7MM3R4CBHle4h/7RIOp0Z+sBHb8Xkej+CqlQ="},"voting_power":"2660559000000","proposer_priority":"16668103928931"},{"address":"1752596117BC8CDD6E18E9284FFE629D0C69B7B7","pub_key":{"type":"tendermint/PubKeyEd25519","value":"6QP9a7mISM0JdJbGNZtDquLxfwXxS8TDjf91FPDWdjU="},"voting_power":"2660381510000","proposer_priority":"-4194335419507"},{"address":"E19BA58E6D3F8BBCC20A510611464F34C3D3AE4C","pub_key":{"type":"tendermint/PubKeyEd25519","value":"Uv6lwxzdqp85ZwB9/yCWpRvJ7klGcu7iqpyqhwyMa54="},"voting_power":"2660033858300","proposer_priority":"-21654582784644"},{"address":"5D201FBAB54BE105C4784C4D752A552771CD1238","pub_key":{"type":"tendermint/PubKeyEd25519","value":"cXWj33bgZen2cpmmNoW9JYuqj2biJ4GsuWJs4tHd6us="},"voting_power":"2654145300000","proposer_priority":"86106460217391"},{"address":"ECCB99DE2BB6889DE0F6F57F4A1FB6F1656A4B8A","pub_key":{"type":"tendermint/PubKeyEd25519","value":"BgfAcxQ9kWwGFuq3E0ICtopS8f0hvKHiRhzFkSUsRz0="},"voting_power":"2653802007602","proposer_priority":"-64282506434344"},{"address":"EBFD9287C1D8DEAD3C177CB5AD6EF2FBE7DD14D8","pub_key":{"type":"tendermint/PubKeyEd25519","value":"QAmDyLcGGO+mp166Turxhgk4tHSWhpRFrwrFI/fyw/Q="},"voting_power":"2653366200000","proposer_priority":"90194970580268"},{"address":"5FA412EA2A32FB2BC7F86366366687B9C0B5066D","pub_key":{"type":"tendermint/PubKeyEd25519","value":"NDscCFVmto1/lDO1nPCWmKsNsARA0Dhqogm1KLO5WmE="},"voting_power":"2652506180000","proposer_priority":"-50179828308574"},{"address":"4364CFE57453CCC8466EF2C74823D85065FBF279","pub_key":{"type":"tendermint/PubKeyEd25519","value":"DgMrYWdkEH7i4OU1G0dHx1vIH2m+Ufi+5p1+lQ54eCU="},"voting_power":"2651210000000","proposer_priority":"69039582031450"},{"address":"10A31951CBF749C3F7FE8EF8E551ED798B1CB1ED","pub_key":{"type":"tendermint/PubKeyEd25519","value":"9ynerbZjmtWxUH6A2P4O/VQXFziPCocEaxBe6eqDmqU="},"voting_power":"2651206050000","proposer_priority":"-38696461027002"},{"address":"E083155AEBD6C3A2FF175F750D65691FA47EB912","pub_key":{"type":"tendermint/PubKeyEd25519","value":"0a9TTjy4PalxlaVBtQqHykjJyIpDJPPEH/97QQKU4dg="},"voting_power":"2650663500000","proposer_priority":"-29643478944269"},{"address":"9256ADF51D05281D787C3D4A52840A27135B8ABB","pub_key":{"type":"tendermint/PubKeyEd25519","value":"Z6HukmlrwePKyItoJVqW01C0CSPmShhDY50ZlQVIJv8="},"voting_power":"2650431630000","proposer_priority":"57237366394448"},{"address":"11BEA6434F204C2F7BE6CB7CF845C44C0FD3FBD2","pub_key":{"type":"tendermint/PubKeyEd25519","value":"Rq9aufm0sP6A0m7Le7/Jk+BcJI+Jxu7gnOKznaaSSEA="},"voting_power":"2650396500000","proposer_priority":"58098263542414"},{"address":"98FBA17933CDBED11FA4DE41B3CA08A7C5E29EF1","pub_key":{"type":"tendermint/PubKeyEd25519","value":"/ddlb+hjJiFkArKq1L6zoKRrVxhJwARYuZJsHCW/ip0="},"voting_power":"2650367680000","proposer_priority":"57163274023039"},{"address":"F2ADC4D51000C0AE6A48248B70683B27B27E7020","pub_key":{"type":"tendermint/PubKeyEd25519","value":"wI9b36PqPxsf2AIxB1MR79RXoEGqtZsTihoS29VgokU="},"voting_power":"2650190000000","proposer_priority":"-28464091960107"},{"address":"4777F1971572B5A22EE8FBD64777383E0647AC4D","pub_key":{"type":"tendermint/PubKeyEd25519","value":"Ly8bbP2tMESO/E2/aH6mDRI8qIdlLBRrUL3cLQ0qQ8o="},"voting_power":"2649319500000","proposer_priority":"47700018652387"},{"address":"7A27465351C2EAFD51B8853D38DE51D9FB6B3970","pub_key":{"type":"tendermint/PubKeyEd25519","value":"d0NFe9L9mXHEGaThGYV0QpKit7dHGcOXMx/RwWvuMEc="},"voting_power":"2649131000000","proposer_priority":"-51075778153860"},{"address":"93DC731C3F3B1B6561501FD8F0ECC10106E93C53","pub_key":{"type":"tendermint/PubKeyEd25519","value":"uyouUZDD8mR6X1xzspxFVqymVsv3CR5cJ8CRcLcE6jY="},"voting_power":"2648644500000","proposer_priority":"-57051971681625"},{"address":"E11AAA5F6D45F9D8A5A62996C67EC6B03DD415F9","pub_key":{"type":"tendermint/PubKeyEd25519","value":"L4jByBhvDWonU78PDIEkioIFUcGwLv1CdGkueC3pI3o="},"voting_power":"2648071127602","proposer_priority":"87905372437707"},{"address":"12553DD365FCD9E919C9143C6FCDF3C5AECFD7BF","pub_key":{"type":"tendermint/PubKeyEd25519","value":"cnC4gHXcbe/acMWB6KR+gI5+Yn8MMfkGX135rohwrRE="},"voting_power":"2647829680000","proposer_priority":"-59251414792363"},{"address":"61F0BEB16A4B4EFB93300C6E7CC061791F4939C9","pub_key":{"type":"tendermint/PubKeyEd25519","value":"PIAJW9De3mIM2s0B+ItjXSo8opUxGw6qFA/FiE7+DtI="},"voting_power":"2647775720000","proposer_priority":"81781259310327"},{"address":"7D8784F2DD6FDCFF7A78087B0D7CDFB2289E45BA","pub_key":{"type":"tendermint/PubKeyEd25519","value":"o9mmxKtCETGGwN4/Xs0SdD5rLeXHOqBv/DQTBoHM7ew="},"voting_power":"2647537100000","proposer_priority":"78883597496577"},{"address":"D4625C77BD7B4DF522FC9635BCFEDE4C7498F92B","pub_key":{"type":"tendermint/PubKeyEd25519","value":"u7c4JePi8pUqNWkaSasdEeq9tWwnYWBTndm74sR4Uyw="},"voting_power":"2647524950000","proposer_priority":"73521805890639"},{"address":"D1F4FAF06AA13FD2FEA350A0AE631F53AF914285","pub_key":{"type":"tendermint/PubKeyEd25519","value":"kpE970Uaj9gCvqXF4JUgc6LRsZKutE+YN/dM+uxJBBQ="},"voting_power":"2647471460000","proposer_priority":"74145907637827"},{"address":"44092E5AEA6F6583644071066B42396C44749C48","pub_key":{"type":"tendermint/PubKeyEd25519","value":"TkBD1EzRdJEkP6bd5/qHxFcsDq8CI1A25ByRkNL2TPg="},"voting_power":"2647343908300","proposer_priority":"30968630835917"},{"address":"270D091AE90B927E840F5A48CA9B828C82EEBBB9","pub_key":{"type":"tendermint/PubKeyEd25519","value":"c6abKZYxGB6fM89JqHll37mbVJwU0z+FrzIs8gZ4DA4="},"voting_power":"2647188190000","proposer_priority":"81844755799811"},{"address":"197E6BD5675B8B063DCEAA559A5DDFFEB5E500C3","pub_key":{"type":"tendermint/PubKeyEd25519","value":"arvEaIrWYDC89Vfv5KyLWtqbctz5tj0wR6yosOIWZsg="},"voting_power":"2646574000000","proposer_priority":"-61298305927718"},{"address":"0EDE9DD15BA3638E3775736415CB585FFE07EDAD","pub_key":{"type":"tendermint/PubKeyEd25519","value":"xwK3zPkmreMfYQ5pU+SEWepWQGgn9M/SRNMUab2TZr0="},"voting_power":"2646322000000","proposer_priority":"90690377795491"},{"address":"95516F3FF3CBF906968A1079FD32D7F6362B47D9","pub_key":{"type":"tendermint/PubKeyEd25519","value":"TZ28v6NicWCu5Cr7a8+tHjUf3K8ZKdg22jHOraXJo/4="},"voting_power":"2646088600000","proposer_priority":"53583487027827"},{"address":"CC0722E3A981B96DFA12A28C0B6033CC0B1DE46A","pub_key":{"type":"tendermint/PubKeyEd25519","value":"XAKD4fwxrRadMPzStSk7EBbtbxqGg79SGUGp3F9ghGk="},"voting_power":"2645942500000","proposer_priority":"19386802815800"},{"address":"A21CC5CC4C83590F54753D7A859DDBE4D2BDBE5E","pub_key":{"type":"tendermint/PubKeyEd25519","value":"Q6F9597JZzAldoL4Q4Gp60uiDGuHufQ2W7b+/AoDdas="},"voting_power":"2642761700000","proposer_priority":"40623420739611"},{"address":"640A18C5B678E929229B68702BBEA5E423A5B75A","pub_key":{"type":"tendermint/PubKeyEd25519","value":"EfPy5/Xj2RKUw3x1LRr8aWFxy8uqdvJjeD2rH16mYk0="},"voting_power":"2640063000000","proposer_priority":"-17495324029616"},{"address":"835DD26474008AF69330EC0D91F45BA268C209F9","pub_key":{"type":"tendermint/PubKeyEd25519","value":"MT+ne/IQZLcQTTU8AsItmoubq6HuLQ4Mkw/lJGZ62a8="},"voting_power":"2623771000000","proposer_priority":"56121889359624"},{"address":"09AB496450AA3A96D2DE8FDDE60B422DF3E37CD5","pub_key":{"type":"tendermint/PubKeyEd25519","value":"D5EbXLI//xDB3j+OOVois/ZGAtnjOI3BEI0F3Jb2t1w="},"voting_power":"2620106500000","proposer_priority":"-13169624242239"},{"address":"E77FEFF7D6B98EF8BEB27F7EF3FABD0031074C69","pub_key":{"type":"tendermint/PubKeyEd25519","value":"HNcHzksfvs9cK+TwhGW6Lcs5HCfRDzvgLUhqY4txGgw="},"voting_power":"2619016180000","proposer_priority":"1041483165691"}],"count":"40","total":"219"}}
val_details = requests.get('https://raw.githubusercontent.com/L0vd/namada_idexer_validators/main/genesis_tm_address_to_alias.json')
val_details = val_details.json()

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

        try:
            moniker = val_details[addr]["alias"]
            tendermint_address = val_details[addr]["nam_address"]
        except:
            moniker, tendermint_address = '', ''


        insert_values.append((addr, vp, block_height, rank, moniker, tendermint_address))
        print((addr, vp, block_height, rank, moniker, tendermint_address))
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
                            rank BIGINT,
                            moniker VARCHAR(255),
                            tendermint_address VARCHAR(255)
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
