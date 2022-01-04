#%%
import pandas as pd
import json
from collections import defaultdict
from collections import OrderedDict
#%%
privates = json.load(open('private_muts.json'))
lineage_muts = json.load(open('lineage_per_mutation.json'))

#%%
for sequence in privates:
    candidates = defaultdict(int)
    for mutation in sequence['private_muts']:
        for lineage in lineage_muts.get(mutation,[]):
            candidates[lineage] += 1
    sequence['candidate_contaminants'] = OrderedDict(sorted(candidates.items(), key=lambda x: x[1], reverse=True))

privates


# %%
json.dump(privates,open('contaminants.json','w'),indent=4)

# %%
