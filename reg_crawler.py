import requests
import json
from lxml import html

main_url =''
reg_url = ''

param_methodName="getDeedDetails"
param_deedsel=1
param_districtCode=8
param_sroCode=813
param_doctno=0
param_regyear=2019
param_submit='Submit'

def regsite_crawler(param_deedsel,param_districtCode,param_sroCode,param_doctno,param_regyear,param_submit):
    prop_desc = []
    dates = []
    prop_values= []
    parties = []
    prop_numbers  = []

    prop_record  = {
        "prop_desc" : prop_desc,
        "dates" : dates,
        "prop_values" : prop_values,
        "parties" : parties,
        "prop_numbers" : prop_numbers
        }

    session  = requests.Session()
    session.get(main_url)
    
    reg_url_temp = reg_url+'&deedsel='+str(param_deedsel)+'&districtCode='+str(param_districtCode)+'&sroCode='+str(param_sroCode)+'&doctno='+str(param_doctno)+'&regyear='+str(param_regyear)+'&submit='+param_submit
    print(reg_url_temp)
    r_reg = session.get(reg_url_temp)
    
    #Retrive the root element
    tree = html.fromstring(r_reg.content)
    
    #Retrive the record data
    prop_desc_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr[2]/td[2]/text()[normalize-space()]')
    for prop in prop_desc_path:
      prop_desc.append(str(prop).strip())
    
    dates_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr[2]/td[3]/text()[normalize-space()]')
    for prop in dates_path:
      dates.append(str(prop).strip())
    
    prop_values_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr[2]/td[4]/text()[normalize-space()]')
    for prop in prop_values_path:
      prop_values.append(str(prop).strip())
    
    parties_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr[2]/td[5]/text()[normalize-space()]')
    for prop in parties_path:
      parties.append(str(prop).strip())
      
    prop_numbers_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr[2]/td[6]/text()[normalize-space()]')
    for prop in prop_numbers_path:
      prop_numbers.append(str(prop).strip())  
    
    print(json.dumps(prop_record))
    return prop_record
    


reg_data = []

for x in range(1,6001) :
    print(param_doctno+x)
    reg_data.append(regsite_crawler(param_deedsel,param_districtCode,param_sroCode,param_doctno+x,param_regyear,param_submit))
 
print(json.dumps(reg_data))
with open('reg_data_2019_1_6000.json','w') as outfile:
    json.dump(reg_data,outfile)





    
    
    
    
