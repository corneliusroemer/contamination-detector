#%%
import requests
import json
import pandas as pd
import aiohttp
import asyncio
import tqdm
import tqdm.asyncio
#%%
# Use list from latest release for better compatibility with what's on covSpectrum
lineage_url = 'https://raw.githubusercontent.com/cov-lineages/pango-designation/master/lineages.csv'
lineages = pd.Series(pd.read_csv(lineage_url).lineage.unique())
#%%
#Remove withdrawn lineages (start with *)
lineages[~lineages.str.contains('*', regex=False)]
lineages
#%%
baseurl = 'https://lapis.cov-spectrum.org/gisaid/v1/sample/nuc-mutations?host=Human&pangoLineage='
#%%
# Can theoretically query for all lineages at once, getting lineage names from designation repo
# lineages = ['B.1.1.7','P.1','AY.4','AY.4.2','AY.9','B.1.640','B.1.621','B.1.617.2','BA.1', 'BA.2', 'BA.3']
#%%
# query_result = []
# for lineage in lineages[:30]:
#     url = baseurl + lineage # + '*'
#     print(url)
#     r = requests.get(url)
#     query_result.append({'lineage': lineage, 'data': r.json()})


#%%
async def get_mutations(session, lineage):
    url = baseurl + lineage # + '*'
    try:
        async with session.get(url,timeout=3600) as resp:
            # r = await resp
            try:
                data = await resp.json()
                # print(f"{lineage} has {len(data)} mutations")
            except:
                print(f"Error with {lineage}, {r.text[:100]}")
                data = []
    except:
        print(f"Timeout with {lineage}")
        data = []

    lineage_dict = {'lineage': lineage, 'data': data}
    # print(counter,lineage)
    # print(lineage)
    return lineage_dict

async def execute_requests():
    connector = aiohttp.TCPConnector(limit=50)
    timeout = aiohttp.ClientTimeout(total=3600)

    async with aiohttp.ClientSession(connector=connector,timeout=timeout) as session:

        tasks = []
        for lineage in lineages:
            tasks.append(asyncio.ensure_future(get_mutations(session, lineage)))

        mutations_by_lineage = []
        all_lineages = [await f for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))]
        # all_lineages = await asyncio.gather(*tasks)
        for lineage in all_lineages:
            mutations_by_lineage.append(lineage)

        return mutations_by_lineage

#%%
# asyncio.run(execute_requests())
# In Jupyter environment need await directly
mutations_by_lineage = await execute_requests()

query_result = mutations_by_lineage

#%%
mutations_clade = []
for lineage in query_result:
    if lineage['data'] != []:
        for mutation in lineage['data']['data']:
            mutations_clade.append({
                'lineage': lineage['lineage'],
                'mutation': mutation['mutation']
            })
# mutations_clade
# %%
df = pd.DataFrame(mutations_clade)
df['position'] = df['mutation'].apply(lambda x: x[1:-1])
df
# %%
lineage_per_mutation = pd.DataFrame(df.groupby('mutation').lineage.apply(list))
lineage_per_mutation
#%%
lineage_per_mutation['position'] = lineage_per_mutation.index.map(lambda x: int(x[1:-1]))
lineage_per_mutation.sort_values(by='position',ascending=True,inplace=True)
lineage_per_mutation

# %%
lineage_per_mutation['lineage'].to_json('lineage_per_mutation.json',indent=4)
# %%
# %%
