#%%
import pandas as pd
import json
from collections import defaultdict
from collections import OrderedDict
#%%
privates = json.load(open('private_muts.json'))
privates
#%%
clade_muts = json.load(open('clade_muts.json'))
clade_muts
#%%
#%%
for sequence in privates:
    foreign_muts = defaultdict(list)
    for mutation in sequence['private_muts']:
        for clade in clade_muts.get(mutation,[]):
            foreign_muts[mutation].append(clade['Nextstrain_clade'])
    sequence['foreign_mutations'] = foreign_muts

privates


# %%
json.dump(privates,open('contaminants.json','w'),indent=4)

# %%
