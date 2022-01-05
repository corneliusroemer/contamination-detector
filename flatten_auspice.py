#%%
import json
#%%
# Opening JSON file
f = open('nextclade.auspice.json')
data = json.load(f)
data
#%%
nodes = []
def flatten_tree(data):
    """
    Recursively flatten the nodes of the tree
    """
    if 'children' in data:
        for child in data['children']:
            flatten_tree(child)
    elif data['node_attrs']['Node type']['value'] == 'New':
    # else:
        nodes.append(data)

flatten_tree(data['tree'])
nodes
# %%
# %%
private_muts = []
for node in nodes:
    muts = []
    if 'branch_attrs' in node:
        if 'mutations' in node['branch_attrs']:
            muts = node['branch_attrs']['mutations']['nuc']
    private_muts.append({
        'name': node['name'],
        'clade': node['node_attrs']['clade_membership']['value'],
        'private_muts': muts
    })
json.dump(private_muts,open('private_muts.json','w'),indent=4)

# %%

# %%
