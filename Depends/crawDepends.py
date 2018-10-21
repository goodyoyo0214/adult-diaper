import pandas as pd
import requests as re
import json

# %% reorganized data into correct formate

def organizeJson(jsonFile, Sepkey, rmkeys): # group list of dictionary by "type" key
    
    dt_dic = {}

    for ele in jsonFile[Sepkey]: # drill into first layer key: data, included
        
        eleN = ele.copy() # copy the element dictionary
        key = eleN['type'] # get the type of dictionary

        
        if key not in dt_dic.keys(): # create the dictionary key of type
            dt_dic[key] = []
            
        for k in rmkeys: # delete the type tag
            del eleN[k]
        
        # nonetype happened in data 
        if key == 'master-product': # perprocess master-product tab
            
            eleN['relationships']['attributes'] = "NA" # delete attribute
            if eleN['attributes']['image'] is None: # put NA into non type
                eleN['attributes']['image'] = "NA"
                
        if key == 'product-retailer':
            if eleN['attributes']['maximo'] is None:
                eleN['attributes']['maximo'] = "NA"
                
        if key == 'product':
            if eleN['attributes']['more-info'] is None:
                eleN['attributes']['more-info'] = "NA"
            if eleN['attributes']['ean'] is None:
                eleN['attributes']['ean'] = "NA"
        if key == 'embedded-code':
            eleN['attributes']['zip_detected'] = "NA"
        
        
        # only export brand and retailer when included key
        if Sepkey == 'included':
            if key == 'brand':
                dt_dic[key].append({key:eleN['attributes']['name']})
            if key == 'retailer':
                eleN['attributes']['forgot-password-url'] = "NA"
                eleN['attributes']['register-url'] = "NA"
                
                if eleN['attributes']["url"] is None:
                    eleN['attributes']["url"] = "NA"
                dt_dic[key].append(eleN)
            
        else:
            dt_dic[key].append(eleN)
    return(dt_dic)



def DClean(data): # merge list of dictionary into dataframe
    final_Dic = {}
    for k in data.keys():
        if len(data[k]) > 0:
            final_Dic[k] = pd.io.json.json_normalize(data[k], errors = 'ignore')
    return(final_Dic)



#%%  main

LinkDir = "D:/OPT/project/adult diaper/product_crawer/adult-diaper/Depends/source.txt"
product_ls = [json.loads(re.get(l).text) for l in pd.read_table(LinkDir, header = None )[0]]

data_ls = [DClean(organizeJson(data, 'data', ['type', 'links'])) for data in product_ls]
included_ls = [DClean(organizeJson(data, 'included', ['type'])) for data in product_ls]


#%%

data_organized = {}
for k in data_ls[0].keys():
    data_organized[k] = list(data_organized[k] for data_organized in data_ls)

include_organzed = {}
for k in included_ls[0].keys():
    include_organzed[k] = list(include_organzed[k] for include_organzed in included_ls)

#%%
data_dic = {}
    
for k in data_organized.keys():
    data_dic[k] = pd.concat(data_organized[k])
del data_organized

include_dic = {}
for k in include_organzed.keys():
    include_dic[k] = pd.concat(include_organzed[k])
del include_organzed







    







    
    



