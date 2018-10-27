import pandas as pd
import requests as req
import json
import re
import numpy as np


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
    return(list(set(a) & set(b)))

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
        result_dic[key] = targetFeature[0].upper() # update result dic
        featureIdx = data_ls.index(targetFeature[0]) # get the feature dic
        str_TFIdx[featureIdx] = False # update idxTF for check remaining element
        nameIdx = checkIdx(nameIdx,featureIdx) # update the nameIdx
    return(nameIdx)


    

# return a dictioanry of different column
def seperateProductName(StrData, BrandIncluded = True, tagetFeature = ["Color","Absorb","Size"] ):
    result_dic = {"Brand":"", "Product_Name":"", "Gender":"", "Color":"", "Size":"","Absorb":"", "other":""}
    # dictionary for searching
    targetFeature_dic = { "Color": ['black','lavender','beige','grey','blue'], 
                         "Absorb":['maximum','moderate'], 
                         "Size":"^(l|m|xl|s)/?(l|m|xl|s)?$"}
    # mapping size to size abbreviate
    size_dic = {'median':'m', 'large':'l', "medium":"m", "small":"s"}
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
    # update size if it is not abbreciate
    intersect_ls = intersect(str_ls,size_dic.keys())
    if intersect_ls:
        result_dic["Size"] = "/".join([size_dic[k] for k in intersect_ls]+[result_dic["Size"]]).upper()
        for i in intersect_ls:
            str_TFIdx[str_ls.index(i)] = False
        
    
    # locate if for exist
    if "for" in str_ls:
        ForExist = True    
        forIndex = str_ls.index("for")
        result_dic["Product_Name"] = " ".join([i for i in str_ls[1:forIndex]])
        result_dic["Gender"] = str_ls[forIndex+1]
        str_TFIdx[start_idx:forIndex+2] = [False]*len(str_TFIdx[start_idx:forIndex+2])
    
    # find number and update nameIdx
    intIdx = [i for i, item in enumerate(str_ls) if re.search(r"\d", item)]
    if intIdx:
        nameIdx = checkIdx(nameIdx, intIdx[0])

    # get product name if no for exist    
    if not ForExist:
        result_dic["Product_Name"] = " ".join([i for i in str_ls[start_idx:nameIdx]])
        str_TFIdx[start_idx:nameIdx+1] = [False]*len(str_TFIdx[start_idx:nameIdx+1])
    
    if sum(str_TFIdx) > 0:
        result_dic['other'] = " ".join([i for (i, v) in zip(str_ls, str_TFIdx) if v])
    
    return(pd.DataFrame(result_dic, index = [0]))

def getCount(StrData):
    # basic data
    Count_ls = StrData.lower().split(" ")
    resultDic = {"Count":np.nan,"Pack":np.nan}
    # the count feature
    c = re.compile(r"^(ct|count)$")
    countFeature = list(filter(c.match, Count_ls))
    if countFeature:
        resultDic["Count"] = Count_ls[Count_ls.index(countFeature[0])-1]
        resultDic["Pack"] = 1
    # the pack feature
    if "pack" in Count_ls:
        resultDic["Pack"] = Count_ls[Count_ls.index("pack")-1]
    return(pd.DataFrame(resultDic, index = [0]))
    
#%%
test_LS = ["16 ct 4 pack", "174 ct", "real fit incontinence briefs for men maximum absorbency small medium 20 count", "fit flex incontinence underwear for women maximum absorbency s tan packaging may vary"]  

getCount(test_LS[2])

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
# update index
AuthenProd_df.index = AuthenProd_df['AuthProduct_ID']

#%% master product table

data_dic['master-product'].columns = ['Count_pack', 'EAN', 'Image',
       'Max_price', 'Min_price', 'Product_Name_full',
       'Slug', 'UPC', 'MasterProduct_id', 'relationships.attributes',
       'AuthenticProd_id',
       'relationships.authentic-product.data.type',
       'Family_ID', 'relationships.brand.data.type']

# remove duplicate and reset index
MasterProd_df = data_dic['master-product'].drop_duplicates().reset_index()

# reset index
MasterProd_df = MasterProd_df.drop(columns = ['index', 'relationships.attributes', 
       'relationships.authentic-product.data.type',
       'relationships.brand.data.type'])

MasterProd_df = pd.concat([pd.concat([
                           # seperate name into meaning ful column
                           seperateProductName(x) for x in MasterProd_df['Product_Name_full']]).reset_index(),
                           # divied the count and pack value
                           pd.concat([getCount(x) for x in MasterProd_df['Count_pack']]).reset_index(),
                           # the original MasterProd_df
                           MasterProd_df],axis=1).reset_index().drop(columns = ['index','level_0'])

# index by master product id for join later
MasterProd_df.index = MasterProd_df['MasterProduct_id']

# filling the missing color value
MasterProd_df['Color'] = MasterProd_df['AuthenticProd_id'].map( MasterProd_df.groupby(['AuthenticProd_id'])['Color'].max())

# filling missing absorb value
# filling moderate product
MasterProd_df.loc[MasterProd_df['Product_Name'] == "shields",['Absorb']] = "MODERATE"
MasterProd_df['Absorb'].loc[(MasterProd_df['Absorb'] == "") & (MasterProd_df["Product_Name"] == "silhouette active fit briefs")] = "MODERATE"

# filling bed product
MasterProd_df['Absorb'].loc[(MasterProd_df['Absorb'] == "") & (MasterProd_df["Product_Name"] == "bed protectors")] = "OVERNIGHT"

# rest of product are max absorb
MasterProd_df['Absorb'].loc[(MasterProd_df['Absorb'] == "")] = "MAXIMUM"

# filling the Unisex gender
MasterProd_df.loc[MasterProd_df['Gender'] == "", 'Gender'] = "Unisex"

# filling the Size
MasterProd_df.loc[MasterProd_df['Size'] =="", "Size"] = "One size fits all"


#%% reference table for retail id to retail name

retailer_df = include_dic['retailer'].loc[:,['attributes.name','id']].drop_duplicates()
retailer_df.index = retailer_df['id']


# %% retailer product 

retailProd_df = data_dic['product-retailer'].drop_duplicates().reset_index()
retailProd_df = retailProd_df.drop(columns = ['index', 'attributes.maximo', 'attributes.minimo', 'attributes.price-string',
       'attributes.price-unit','relationships.master-product.data.type', 'relationships.retailer.data.type'])

retailProd_df.columns = ['Link', 'Product_Name_full', 'Price', 'RetailerInter_id', 'Product_id', 'MasterProduct_id', 'Retailer_id']
retailProd_df['Retailer_name'] = retailProd_df['Retailer_id'].map(retailer_df['attributes.name'])

# %% check for duplicate master_id and retailer_id
retailProdMaxPrice_df = retailProd_df.groupby(['Retailer_id','Retailer_name','MasterProduct_id'],as_index=False )['Price'].max()
retailProdMaxPrice_PivDf = retailProdMaxPrice_df.pivot(index = 'MasterProduct_id', columns = 'Retailer_name', values = 'Price' )

#%%

master_price = pd.merge(MasterProd_df, retailProdMaxPrice_PivDf, left_index = True, right_index = True,  how = "outer")
master_price.index = master_price['AuthenticProd_id']

#%% drop unasary column

Final_tbl = master_price.loc[:,['Product_Name_full','Product_Name','Family_ID','UPC','EAN','MasterProduct_id','AuthenticProd_id',
                          'Product_Name','Gender','Color','Size','Absorb','Count','Pack','Amazon','AmazonCanada',
                          'AmazonPrimePantry','BedBathAndBeyond','Bjs','Boxed','CVS','Costco','CostcoCanada',
                          'DollarGeneral','HEB','Instacart','Kroger','Rakuten',"Sam'sClub",'Target','Walgreens',
                          'Walmart','WalmartCanada','Image']].reindex()




