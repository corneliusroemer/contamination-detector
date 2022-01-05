#%%
# 1. Cut down nextclade tsv to clade, mutation
# aws s3 cp s3://nextstrain-ncov-private/metadata.tsv.gz .
# gzcat metadata.tsv.gz  | tsv-select -H -f Nextstrain_clade,substitutions > clade_subs.tsv

# 2. Group by clade
# 3. Count mutations
# 4. Select mutations based on some metric

from collections import defaultdict
import pandas as pd
from tqdm import tqdm
import json

#%%
df = pd.read_csv('clade_subs.tsv',sep='\t').dropna()
df
# %%
def accumulate_mutations(acc: defaultdict(int), row) -> defaultdict(int):
    try:
        for mutation in str(row).split(','):
            acc[mutation] += 1
    except:
        print(row)
        raise
    return acc

def aggregate_mutations(series) -> defaultdict(int):
    mutations = defaultdict(int)
    for row in tqdm(series):
        mutations = accumulate_mutations(mutations, row)
    return mutations

#%%
# subsampled = df.sample(n=100000)
# clade_muts = subsampled.groupby('Nextstrain_clade').substitutions.apply(aggregate_mutations).dropna().astype(int)
#%%
clade_muts = df.groupby('Nextstrain_clade').substitutions.apply(aggregate_mutations).dropna().astype(int)
#%%
clade_muts.rename('mut_count', inplace=True)
clade_muts
#%%
clade_count = df.groupby('Nextstrain_clade').count()
clade_count.rename(columns={'substitutions':'clade_count'},inplace=True)
clade_count
# %%
clade_muts['21A (Delta)'].sort_values(ascending=False)
#%%
mutations = pd.DataFrame(clade_muts).reset_index()
mutations.rename(columns={'level_1':'mutation'},inplace=True)
# mutations['count'] = clade_count
mutations = mutations.join(clade_count,on='Nextstrain_clade')
mutations
#%%
mutations['proportion'] = mutations['mut_count']/mutations['clade_count']
mutations.sort_values(by=['Nextstrain_clade','proportion'],ascending=False,inplace=True)
mutations
#%%
# mutations[mutations['level_1']=='nan']
# %%
# Select relevant mutations, in more than 50% of sequences of clade or appearing more than 1000 times in clade
relevant = mutations[(mutations['proportion']>0.3) | (mutations['mut_count'] > 100000)]
# %%
mut_dict = {}
for mutation, row in relevant.groupby('mutation'):
    mut_dict[mutation] = row[['Nextstrain_clade','mut_count','proportion']].sort_values(by='mut_count',ascending=False).to_dict(orient='records')
mut_dict = dict(sorted(mut_dict.items(), key=lambda item: int(item[0][1:-1])))
#%%
json.dump(mut_dict,open('clade_muts.json','w'),indent=2)

# %%
