import pandas as pd
import requests as re
import json

#%% try read complex json file
link = "https://api.smartcommerce.co/api/v1/widget/f03e3f7f-07b3-4a67-a833-1865b0579b62?sepmId=100372"
dt_json = json.loads(re.get(link).text) # get the web dictionary


# %% 

def organizeJson(jsonFile, Sepkey, rmkeys): # group list of dictionary by "type" key
    
    dt_dic = {}

    for ele in jsonFile[Sepkey]: # drill into first layer key: data, included
        
        eleN = ele.copy() # copy the element dictionary
        key = eleN['type']
        
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


#%% create dictionary
data_LS = DClean(organizeJson(dt_json, 'data', ['type', 'links']))
inlcude_LS = DClean(organizeJson(dt_json, 'included', ['type']))





    
    



