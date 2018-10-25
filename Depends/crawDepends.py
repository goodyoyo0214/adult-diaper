import pandas as pd
import requests as req
import json
import re


# %% function for cleaning json file

# categorized json dictionary based on type
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


# merge the same data into dataframe
def DClean(data): # merge list of dictionary into dataframe
    final_Dic = {}
    for k in data.keys():
        if len(data[k]) > 0:
            final_Dic[k] = pd.io.json.json_normalize(data[k], errors = 'ignore')
    return(final_Dic)

#%% extract the datagframe

# use intersect to find if feature exist
def intersect(a, b):
    """ return the intersection of two lists """
    return list(set(a) & set(b))

# update the last index of product name
def checkIdx(org,new):
    if new<org:
        return(new)
    else:
        return(org)

# serperate string into list
def findfeature(data_ls, featur_dic, nameIdx, key, result_dic, str_TFIdx):
    if type(featur_dic[key]) == list: # if the taget is list
        targetFeature = intersect(data_ls,featur_dic[key])
    if type(featur_dic[key]) == str:
        r = re.compile(featur_dic['Size'])
        targetFeature = list(filter(r.match, data_ls))
    if targetFeature: # if target feature exist
        result_dic[key] = targetFeature[0] # update result dic
        featureIdx = data_ls.index(targetFeature[0]) # get the feature dic
        str_TFIdx[featureIdx] = False # update idxTF for check remaining element
        nameIdx = checkIdx(nameIdx,featureIdx) # update the nameIdx
    return(nameIdx)

# return a dictioanry of different column
def seperateProductName(StrData, BrandIncluded = True,tagetFeature = ["Color","Absorb","Size"] ,  *args):
    result_dic = {"Brand":"", "Product_Name":"", "Gender":"", "Color":"", "Size":"","Absorb":"", "other":""}
    # dictionary for searching
    targetFeature_dic = { "Color": ['black','lavender','beige','grey','blue'], 
                         "Absorb":['maximum','moderate'], 
                         "Size":"^(l|m|xl|s)/?(l|m|xl|s)?$"}
    # split string into list
    str_ls = StrData.lower().split(" ")
    # idx list to check if everything is there
    str_TFIdx = [True] * len(str_ls)
    start_idx = 0
    ForExist = False
    
    # idx if there is no for
    nameIdx = len(str_ls) 
    # the brand name
    if BrandIncluded:
        result_dic["Brand"] = str_ls[0]
        str_TFIdx[0] = False
        start_idx = 1
    
    # find features and update result  dic
    for i in tagetFeature:
        nameIdx = findfeature(data_ls = str_ls, 
                              featur_dic= targetFeature_dic,
                              result_dic = result_dic,  
                              str_TFIdx = str_TFIdx,
                              nameIdx= nameIdx, 
                              key=i)
    # locate if for exist
    if "for" in str_ls:
        ForExist = True    
        forIndex = str_ls.index("for")
        result_dic["Product_Name"] = " ".join([i for i in str_ls[1:forIndex]])
        result_dic["Gender"] = str_ls[forIndex+1]
        str_TFIdx[start_idx:forIndex+2] = [False]*len(str_TFIdx[start_idx:forIndex+2])
    
    if not ForExist:
        result_dic["Product_Name"] = " ".join([i for i in str_ls[start_idx:nameIdx]])
        str_TFIdx[start_idx:nameIdx+1] = [False]*len(str_TFIdx[start_idx:nameIdx+1])
    
    if sum(str_TFIdx) > 0:
        result_dic['other'] = " ".join([i for (i, v) in zip(str_ls, str_TFIdx) if v])
    
    return(pd.DataFrame(result_dic, index = [0]))

#%%  main 

# file stores dowload link
LinkDir = "D:/OPT/project/adult diaper/product_crawer/adult-diaper/Depends/source.txt"
product_ls = [json.loads(req.get(l).text) for l in pd.read_table(LinkDir, header = None )[0]]

# the list of dictionary of data tag and included tag
data_ls = [DClean(organizeJson(data, 'data', ['type', 'links'])) for data in product_ls]
included_ls = [DClean(organizeJson(data, 'included', ['type'])) for data in product_ls]


#%% reformate the list

data_organized = {}
for k in data_ls[0].keys():
    data_organized[k] = list(data_organized[k] for data_organized in data_ls)

include_organzed = {}
for k in included_ls[0].keys():
    include_organzed[k] = list(include_organzed[k] for include_organzed in included_ls)

# create the final dictionary contain dataframe
data_dic = {}
    
for k in data_organized.keys():
    data_dic[k] = pd.concat(data_organized[k])


include_dic = {}
for k in include_organzed.keys():
    include_dic[k] = pd.concat(include_organzed[k])


#%% authentic product table

data_dic['authentic-product'].columns = ['Image', 'Product_Name_full', 'AuthProduct_ID',
       'Brand_ID','Brand_Name']
data_dic['authentic-product'] = data_dic['authentic-product'].drop(columns = ['Brand_Name'])

AuthenProd_df = data_dic['authentic-product'].drop_duplicates().reset_index()


AuthenProd_df = pd.concat([pd.concat([seperateProductName(x) for x in AuthenProd_df['Product_Name_full']]).reset_index(),
                           AuthenProd_df],axis=1).drop(columns = ['index'])

#%% master product table

data_dic['master-product'].columns = ['Count_pack', 'EAN', 'Image',
       'Max_price', 'Min_price', 'Product_Name_full',
       'Slug', 'UPC', 'MasterProduct_id', 'relationships.attributes',
       'AuthenticProd_id',
       'relationships.authentic-product.data.type',
       'Brand_ID', 'relationships.brand.data.type']


MasterProd_df = data_dic['master-product'].drop_duplicates().reset_index()

MasterProd_df = MasterProd_df.drop(columns = ['index', 'relationships.attributes', 'AuthenticProd_id',
       'relationships.authentic-product.data.type',
       'relationships.brand.data.type'])

#%%

testMasterName = pd.concat([seperateProductName(i) for i in MasterProd_df['Product_Name_full']])



    

#%%

test1 = ['a']



test1.merge


    
    



