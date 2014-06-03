'''
Created on Jun 3, 2014

@author: philipp
'''
from time import strftime
from datetime import datetime
from time import mktime
from collections import defaultdict
import csv
original  = file('trainHistory.csv', 'rU')
reader = csv.reader(original)
def cell(x, y):
    rownum = 0
    for row in reader:
        if rownum == x:
            return row[y]
        rownum = rownum + 1 
        

# print cell(1, 2)   


transactionsfile = file('transactions.csv', 'rU')
transactions = csv.reader(transactionsfile)
offersfile = file('offers.csv', 'rU')
offers = csv.reader(offersfile)
trainHistoryfile = file('trainHistory.csv', 'rU')
trainHistory = csv.reader(trainHistoryfile)


    




for row in offers:
    print row
   
offersfile.seek(0)
  
rownum = 0
for row in transactions:
    print row
    if rownum == 37:
        break
    rownum = rownum + 1
    
transactionsfile.seek(0)
  
company_of_offer = {}
category_of_offer = {}
brand_of_offer = {}
offer_value_of_offer = {}
offer_quantity_of_offer = {}
header_offers = offers.next()
for row in offers:
    company_of_offer[row[0]] = row[3]
    category_of_offer[row[0]] = row[1]
    brand_of_offer[row[0]] = row[5]
    offer_value_of_offer[row[0]] = (row[4])
    offer_quantity_of_offer[row[0]] = row[2]


offersfile.seek(0)

company_of_train = {}
category_of_train = {}
brand_of_train = {}
offer_value_of_train = {}
offer_quantity_of_train = {}
header_trainHistory = trainHistory.next()
for row in trainHistory:
    company_of_train[row[0]] = company_of_offer[row[2]]
    category_of_train[row[0]] = category_of_offer[row[2]]
    brand_of_train[row[0]] = brand_of_offer[row[2]]
    offer_value_of_train[row[0]] = float(offer_value_of_offer[row[2]])
    offer_quantity_of_train[row[0]] = float(offer_quantity_of_offer[row[2]])

trainHistoryfile.seek(0)  
    
date_of_train = {}
for row in trainHistory:
    date_of_train[row[0]] = row[6]



trainHistoryfile.seek(0)

   
rownum = 0
for row in trainHistory:
    print row
    if rownum == 10:
        break
    rownum = rownum + 1
    
trainHistoryfile.seek(0)
    
def total_days_in_date(date):
    month_length = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
    d = list(date)
    list_year = [d[0], d[1], d[2], d[3]]
    year = int(''.join(list_year))
    list_month = [d[5], d[6]]
    month = int(''.join(list_month))
    list_day = [d[8], d[9]]
    day = int(''.join(list_day))
    total_year_days = ((year - 1)/4 + 1) + year*365
    total_month_days = 0
    current_month = 1
    while current_month < month:
        if current_month != 2 or year%4 != 0:
            total_month_days = total_month_days + month_length[current_month]
        else:
            total_month_days = total_month_days + month_length[current_month] + 1
        current_month = current_month + 1
    return total_year_days + total_month_days + day


    
def time_between_dates(date_1, date_2):
    total_days_1 = total_days_in_date(date_1)
    total_days_2 = total_days_in_date(date_2)
    return total_days_2 - total_days_1

trainHistory_dict = {}
for row in trainHistory:
    trainHistory_dict[row[0]] = 0
         
 
 
 
     
trainHistoryfile.seek(0)
 
# steps = 0     
# has_bought_company = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if company_of_train.get(row[0], 'none') == row[4]:
#         has_bought_company[row[0]] = has_bought_company.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#       
# transactionsfile.seek(0)
#         
# steps = 0     
# has_bought_company_a = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if company_of_train.get(row[0], 'none') == row[4]:
#         has_bought_company_a[row[0]] = has_bought_company_a.get(row[0], 0) + float(row[10])
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#          
# transactionsfile.seek(0)
#     
# steps = 0     
# has_bought_company_q = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if company_of_train.get(row[0], 'none') == row[4]:
#         has_bought_company_q[row[0]] = has_bought_company_q.get(row[0], 0) + float(row[9])
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#          
# transactionsfile.seek(0)
#         
# steps = 0     
# has_bought_company_30 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if company_of_train.get(row[0], 'none') == row[4] and time_between_dates(row[6], date_of_train[row[0]]) <= 30:
#         has_bought_company_30[row[0]] = has_bought_company_30.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#          
# transactionsfile.seek(0)
#         
# steps = 0     
# has_bought_company_60 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if company_of_train.get(row[0], 'none') == row[4] and time_between_dates(row[6], date_of_train[row[0]]) <= 60:
#         has_bought_company_60[row[0]] = has_bought_company_60.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#          
# transactionsfile.seek(0)
     
         
# steps = 0     
# has_bought_company_180 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if company_of_train.get(row[0], 'none') == row[4] and time_between_dates(row[6], date_of_train[row[0]]) <= 180:
#         has_bought_company_180[row[0]] = has_bought_company_180.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#          
#          
# transactionsfile.seek(0)
#      
# steps = 0     
# has_bought_category = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if category_of_train.get(row[0], 'none') == row[3]:
#         has_bought_category[row[0]] = has_bought_category.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#     
# transactionsfile.seek(0)
#          
# steps = 0     
# has_bought_category_a = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if category_of_train.get(row[0], 'none') == row[3]:
#         has_bought_category_a[row[0]] = has_bought_category_a.get(row[0], 0) + float(row[10])
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
# #          
# transactionsfile.seek(0)
#       
# steps = 0     
# has_bought_category_q = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if category_of_train.get(row[0], 'none') == row[3]:
#         has_bought_category_q[row[0]] = has_bought_category_q.get(row[0], 0) + float(row[9])
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
          
# transactionsfile.seek(0)
#           
# steps = 0     
# has_bought_category_30 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if category_of_train.get(row[0], 'none') == row[3] and time_between_dates(row[6], date_of_train[row[0]]) <= 30:
#         has_bought_category_30[row[0]] = has_bought_category_30.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
          
# transactionsfile.seek(0)
#           
# steps = 0     
# has_bought_category_60 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if category_of_train.get(row[0], 'none') == row[3] and time_between_dates(row[6], date_of_train[row[0]]) <= 60:
#         has_bought_category_60[row[0]] = has_bought_category_60.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
           
# transactionsfile.seek(0)
#       
#           
# steps = 0     
# has_bought_category_180 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if category_of_train.get(row[0], 'none') == row[3] and time_between_dates(row[6], date_of_train[row[0]]) <= 180:
#         has_bought_category_180[row[0]] = has_bought_category_180.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
          
          
# transactionsfile.seek(0)
#      
# steps = 0     
# has_bought_brand = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if brand_of_train.get(row[0], 'none') == row[5]:
#         has_bought_brand[row[0]] = has_bought_brand.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
     
# transactionsfile.seek(0)
#          
# steps = 0     
# has_bought_brand_a = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if brand_of_train.get(row[0], 'none') == row[5]:
#         has_bought_brand_a[row[0]] = has_bought_brand_a.get(row[0], 0) + float(row[10])
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
          
# transactionsfile.seek(0)
#       
# steps = 0     
# has_bought_brand_q = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if brand_of_train.get(row[0], 'none') == row[5]:
#         has_bought_brand_q[row[0]] = has_bought_brand_q.get(row[0], 0) + float(row[9])
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#           
# transactionsfile.seek(0)
#           
# steps = 0     
# has_bought_brand_30 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if brand_of_train.get(row[0], 'none') == row[5] and time_between_dates(row[6], date_of_train[row[0]]) <= 30:
#         has_bought_brand_30[row[0]] = has_bought_brand_30.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
          
# transactionsfile.seek(0)
#           
# steps = 0     
# has_bought_brand_60 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if brand_of_train.get(row[0], 'none') == row[5] and time_between_dates(row[6], date_of_train[row[0]]) <= 60:
#         has_bought_brand_60[row[0]] = has_bought_brand_60.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
          
# transactionsfile.seek(0)
#       
#           
# steps = 0     
# has_bought_brand_180 = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     if brand_of_train.get(row[0], 'none') == row[5] and time_between_dates(row[6], date_of_train[row[0]]) <= 180:
#         has_bought_brand_180[row[0]] = has_bought_brand_180.get(row[0], 0) + 1
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
          
          
# transactionsfile.seek(0)
#  
# steps = 0     
# total_shopper_spend = dict(trainHistory_dict)
# header_transactions = transactions.next()
# for row in transactions:
#     total_shopper_spend[row[0]] = total_shopper_spend.get(row[0], 0) + float(row[10])
#     if steps %1000000 == 0:
#         print steps/1000000
#     steps = steps + 1
#      
# transactionsfile.seek(0)



        
# fileid = open('total_shopper_spend.txt', 'w')
# for shopper in total_shopper_spend:
#     fileid.write (shopper + ' ' + str(total_shopper_spend[shopper]) + '\n')
# fileid.close()
 
has_bought_company_file = open('has_bought_company.txt', 'r')
has_bought_category_file = open('has_bought_category.txt', 'r')
has_bought_brand_file = open('has_bought_brand.txt', 'r')
#     
#     
#         
has_never_bought_company = {}
for row in has_bought_company_file:
    if row.split()[1] == 0:
        has_never_bought_company[row.split()[0]] = 1
    else:
        has_never_bought_company[row.split()[0]] = 0
#          
#          
has_never_bought_category = {}
for row in has_bought_category_file:
    print row.split()[1]
    if row.split()[1] == 0:
        has_never_bought_category[row.split()[0]] = 1
    else:
        has_never_bought_category[row.split()[0]] = 0
          
          
has_never_bought_brand = {}
for row in has_bought_brand_file:
    if row.split()[1] == 0:
        has_never_bought_brand[row.split()[0]] = 1
    else:
        has_never_bought_brand[row.split()[0]] = 0
#  
#  
#  
has_bought_company_brand = {}
has_bought_company_category = {}
has_bought_category_brand = {} 
has_bought_company_category_brand = {}
has_never_bought_company_category = {}
has_never_bought_company_brand = {}
has_never_bought_category_brand = {}
has_never_bought_company_category_brand = {}
#  
#            
for shopper in has_never_bought_company.keys():
    if has_never_bought_company[shopper] == 1 or has_never_bought_brand[shopper] == 1:
        has_bought_company_brand[shopper] = 0
    else:
        has_bought_company_brand[shopper] = 1
           
    if has_never_bought_company[shopper] == 1 or has_never_bought_category[shopper] == 1:
        has_bought_company_category[shopper] = 0
    else:
        has_bought_company_category[shopper] = 1
   
    if has_never_bought_category[shopper] == 1 or has_never_bought_brand[shopper] == 1:
        has_bought_category_brand[shopper] = 0
    else:
        has_bought_category_brand[shopper] = 1
           
    if has_never_bought_category[shopper] == 1 or has_never_bought_category[shopper] == 1 or has_never_bought_brand == 1:
        has_bought_company_category_brand[shopper] = 0
    else: 
        has_bought_company_category_brand[shopper] = 1
  
    if has_never_bought_company[shopper] == 1 and has_never_bought_category[shopper] == 1:
        has_never_bought_company_category[shopper] = 1
    else:
        has_never_bought_company_category[shopper] = 0
  
    if has_never_bought_company[shopper] == 1 and has_never_bought_brand[shopper] == 1:
        has_never_bought_company_brand[shopper] = 1
    else:
        has_never_bought_company_brand[shopper] = 0       
  
    if has_never_bought_category[shopper] == 1 and has_never_bought_brand[shopper] == 1:
        has_never_bought_category_brand[shopper] = 1
    else:
        has_never_bought_category_brand[shopper] = 0   
      
#      
    if has_never_bought_company[shopper] == 1 and has_never_bought_category[shopper] == 1 and has_never_bought_brand[shopper] == 1:
        has_never_bought_company_category_brand[shopper] = 1
    else:
        has_never_bought_company_category_brand[shopper] = 0

# fileid = open('has_never_bought_company_category_brand.txt', 'w')
# for shopper in has_never_bought_company_category_brand:
#     fileid.write (shopper + ' ' + str(has_never_bought_company_category_brand[shopper]) + '\n')
# fileid.close()
        

    

