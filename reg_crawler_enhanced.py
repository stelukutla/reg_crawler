import requests
import sys
import threading
from lxml import html
import pymongo
import re
import json

main_url = ''
reg_url = ''

param_methodName="getDeedDetails"
param_deedsel=1
param_districtCode=8
param_sroCode=813
param_doctno=0

param_submit='Submit'


session  = requests.Session()
session.get(main_url)


def regsite_crawler(rec_seq,param_deedsel,param_districtCode,param_sroCode,param_doctno,param_regyear,param_submit):
    
    
    
    reg_url_temp = reg_url+'&deedsel='+str(param_deedsel)+'&districtCode='+str(param_districtCode)+'&sroCode='+str(param_sroCode)+'&doctno='+str(param_doctno)+'&regyear='+str(param_regyear)+'&submit='+param_submit
    print(reg_url_temp)
    r_reg = session.get(reg_url_temp)
    
    #Retrive the root element
    tree = html.fromstring(r_reg.content)
    
    incr = 2
    while True :
        dates = []
        prop_values= []
        parties = []
        prop_numbers  = []
        prop_descs = []
    
        prop_record  = {
        'rowid_object' :  rec_seq,  
        'year' : param_regyear,
        "prop_desc" : prop_descs,
        "dates" : dates,
        "prop_values" : prop_values,
        "parties" : parties,
        "prop_numbers" : prop_numbers
        }
        if len(tree.xpath('//table/tr//td/table/tr/td/table/tr['+str(incr)+']/td[2]/text()[normalize-space()]'))  < 1:
               break
        #Retrive the record data
        #property
        prop_desc_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr['+str(incr)+']/td[2]/text()[normalize-space()]')
        for prop in prop_desc_path:
            raw_prop_desc = str(prop).strip()
            village = re.search('VILL/COL:(.*) /', raw_prop_desc)
            colony = re.search('VILL/COL:(.*)W-B:', raw_prop_desc)
            survey_no = re.search('SURVEY:(.*)EXTENT:', raw_prop_desc)
            extent = re.search('EXTENT:(.*)BUILT:',raw_prop_desc)
            boundaries = re.search('Boundires:(.*)',raw_prop_desc)
            
            prop_desc = {
                "village" : village.group(1).strip(),
                "colony" : colony.group(1).strip(),
                "survey_no" :survey_no.group(1).strip(),
                "extent" : extent.group(1).strip(),
                "boundaries" : boundaries.group(1).strip()
                }
            prop_descs.append(prop_desc)
         
        #dates
        dates_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr['+str(incr)+']/td[3]/text()[normalize-space()]')
        
        dates_temp = {
                   "r_date":'',
                   "e_date" : '',
                   "p_date": ''
                   }
        
        for prop in dates_path:
            format_prop = prop.strip()
            if format_prop.find('R') == True :
                r = format_prop[4:]
            elif format_prop.find('E') == True :
                e = format_prop[4:]
            elif format_prop.find('P') == True :
                p = format_prop[4:]
                      
        dates_temp = {
                   "r_date": r,
                   "e_date" : e,
                   "p_date": p
                   }
        dates.append(dates_temp) 
            
        
        #property values
        prop_values_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr['+str(incr)+']/td[4]/text()[normalize-space()]')
        
        code = prop_values_path[0].strip()
        sale_desc = prop_values_path[1].strip()
        mkt_value = re.search('Mkt.Value:Rs. (.*)',prop_values_path[2].strip())
        cons_value = re.search('Cons.Value:Rs.(.*)',prop_values_path[3].strip())
        
        prop_values_temp = {
                "code" : code,
                "sale_desc" : sale_desc,
                "mkt_value": mkt_value.group(1).strip(),
                "cons_value": cons_value.group(1).strip()
                }
    #    for prop in prop_values_path:
    #        t = prop.strip()
        prop_values.append(prop_values_temp)
        
        #buyers and selllers
        parties_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr['+str(incr)+']/td[5]/text()[normalize-space()]')
        
        for prop in parties_path:
          parties.append(str(prop).strip())
          
        #records numbers  
        prop_numbers_path  = tree.xpath('//table/tr//td/table/tr/td/table/tr['+str(incr)+']/td[6]/text()[normalize-space()]')
        prop_nos = {
              "misc" : prop_numbers_path[0].strip(),
              "doc_no": prop_numbers_path[1].strip(),
              "SRO": prop_numbers_path[2].strip() 
              }
        prop_numbers.append(prop_nos)  
#        print(json.dumps(prop_record))
        incr += 1
    return prop_record
    

#mongo client
myclient = pymongo.MongoClient("mongodb://localhost:27017")  
mydb = myclient['reg_db_v5'] 
mycol = mydb['reg']

fail_over_size = 2



def run_batch(param_regyear, start_num,end_num) :
    fail_count = 0
    for x in range(start_num,end_num) :
        print(x)
        record = regsite_crawler(x, param_deedsel, param_districtCode,param_sroCode,param_doctno+x,param_regyear,param_submit)
        if len(record['prop_desc']) == 0 :
            fail_count += 1
            print('Empty Record found , total  : ' + str(fail_count))
            print ('Empty record identifed, failed count : ' + str(fail_count))
            return
            if fail_count > fail_over_size :
                print ('multiple failures found, system exit')
                break
        else :
            rec_id = mycol.insert_one(record)
            print(rec_id.inserted_id)
            
            
if __name__ == "__main__":
        
    for year in range (1983,2020):
        print('Batch runngin for year ->' +str(year) )
        x1 = threading.Thread(target=run_batch, args=(year,1,1001))
        x1.start()
        
        x2 = threading.Thread(target=run_batch, args=(year,1001,2001))
        x2.start()
        
        x3 = threading.Thread(target=run_batch, args=(year,2001,3001))
        x3.start()
        
        x4 = threading.Thread(target=run_batch, args=(year,3001,4001))
        x4.start()
        
        x5 = threading.Thread(target=run_batch, args=(year,4001,5001))
        x5.start()
    
        x6 = threading.Thread(target=run_batch, args=(year,5001,6001))
        x6.start() 
        x7 = threading.Thread(target=run_batch, args=(year,6001,7001))
        x7.start()
        x8 = threading.Thread(target=run_batch, args=(year,7001,8001))
        x8.start()
        x9 = threading.Thread(target=run_batch, args=(year,8001,9001))
        x9.start()
        x10 = threading.Thread(target=run_batch, args=(year,9001,10001))
        x10.start()
        
        x1.join()
        x2.join()
        x3.join()
        x4.join()
        x5.join()
        x6.join()
        x7.join()
        x8.join()
        x9.join()
        x10.join()
#    
    
        print('Job completed')
        session.close   
    
        
 
    
    
 
    
    




    
    
    
    
