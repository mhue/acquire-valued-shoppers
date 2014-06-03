'''
Created on Jun 3, 2014

@author: philipp
'''
import sys
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
        

transactionsfile = file('transactions.csv', 'rU')
transactions = csv.reader(transactionsfile)
offersfile = file('offers.csv', 'rU')
offers = csv.reader(offersfile)
trainHistoryfile = file('trainHistory.csv', 'rU')
trainHistory = csv.reader(trainHistoryfile)


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
date_of_train = {}
header_trainHistory = trainHistory.next()
for row in trainHistory:
    company_of_train[row[0]] = company_of_offer[row[2]]
    category_of_train[row[0]] = category_of_offer[row[2]]
    brand_of_train[row[0]] = brand_of_offer[row[2]]
    offer_value_of_train[row[0]] = float(offer_value_of_offer[row[2]])
    offer_quantity_of_train[row[0]] = float(offer_quantity_of_offer[row[2]])
    date_of_train[row[0]] = row[6]
    
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

def saveIt(d, outfile):
    fileid = open(outfile, 'w')
    for key in d:
        fileid.write(key + ' ' + str(d[key]) + '\n')
    fileid.close()

def loadIt(infile):
    fileid = open(infile)
    d = {}
    for row in fileid:
        words = row.split()
        key = words[0]
        value = int(words[1])
        d[key] = value
    return d

def computeFeaturesFirstPass():

    transactionsfile = file('transactions.csv', 'rU')
    transactions = csv.reader(transactionsfile)

    has_bought_company = dict(trainHistory_dict)
    has_bought_company_a = dict(trainHistory_dict)
    has_bought_company_q = dict(trainHistory_dict)
    has_bought_company_30 = dict(trainHistory_dict)
    has_bought_company_60 = dict(trainHistory_dict)
    has_bought_company_180 = dict(trainHistory_dict)
    has_bought_category = dict(trainHistory_dict)
    has_bought_category_a = dict(trainHistory_dict)
    has_bought_category_q = dict(trainHistory_dict)
    has_bought_category_30 = dict(trainHistory_dict)
    has_bought_category_60 = dict(trainHistory_dict)
    has_bought_category_180 = dict(trainHistory_dict)
    has_bought_brand = dict(trainHistory_dict)
    has_bought_brand_a = dict(trainHistory_dict)
    has_bought_brand_q = dict(trainHistory_dict)
    has_bought_brand_30 = dict(trainHistory_dict)
    has_bought_brand_60 = dict(trainHistory_dict)
    has_bought_brand_180 = dict(trainHistory_dict)
    total_shopper_spend = dict(trainHistory_dict)

    header_transactions = transactions.next()
    steps = 0
    for row in transactions:
        if company_of_train.get(row[0], 'none') == row[4]:
            has_bought_company[row[0]] = has_bought_company.get(row[0], 0) + 1
        if company_of_train.get(row[0], 'none') == row[4]:
            has_bought_company_a[row[0]] = has_bought_company_a.get(row[0], 0) + float(row[10])
        if company_of_train.get(row[0], 'none') == row[4]:
            has_bought_company_q[row[0]] = has_bought_company_q.get(row[0], 0) + float(row[9])
        if company_of_train.get(row[0], 'none') == row[4] and time_between_dates(row[6], date_of_train[row[0]]) <= 30:
            has_bought_company_30[row[0]] = has_bought_company_30.get(row[0], 0) + 1
        if company_of_train.get(row[0], 'none') == row[4] and time_between_dates(row[6], date_of_train[row[0]]) <= 60:
            has_bought_company_60[row[0]] = has_bought_company_60.get(row[0], 0) + 1
        if company_of_train.get(row[0], 'none') == row[4] and time_between_dates(row[6], date_of_train[row[0]]) <= 180:
            has_bought_company_180[row[0]] = has_bought_company_180.get(row[0], 0) + 1
        if category_of_train.get(row[0], 'none') == row[3]:
            has_bought_category[row[0]] = has_bought_category.get(row[0], 0) + 1
        if category_of_train.get(row[0], 'none') == row[3]:
            has_bought_category_a[row[0]] = has_bought_category_a.get(row[0], 0) + float(row[10])
        if category_of_train.get(row[0], 'none') == row[3]:
            has_bought_category_q[row[0]] = has_bought_category_q.get(row[0], 0) + float(row[9])
        if category_of_train.get(row[0], 'none') == row[3] and time_between_dates(row[6], date_of_train[row[0]]) <= 30:
            has_bought_category_30[row[0]] = has_bought_category_30.get(row[0], 0) + 1
        if category_of_train.get(row[0], 'none') == row[3] and time_between_dates(row[6], date_of_train[row[0]]) <= 60:
            has_bought_category_60[row[0]] = has_bought_category_60.get(row[0], 0) + 1
        if category_of_train.get(row[0], 'none') == row[3] and time_between_dates(row[6], date_of_train[row[0]]) <= 180:
            has_bought_category_180[row[0]] = has_bought_category_180.get(row[0], 0) + 1
        if brand_of_train.get(row[0], 'none') == row[5]:
            has_bought_brand[row[0]] = has_bought_brand.get(row[0], 0) + 1
        if brand_of_train.get(row[0], 'none') == row[5]:
            has_bought_brand_a[row[0]] = has_bought_brand_a.get(row[0], 0) + float(row[10])
        if brand_of_train.get(row[0], 'none') == row[5]:
            has_bought_brand_q[row[0]] = has_bought_brand_q.get(row[0], 0) + float(row[9])
        if brand_of_train.get(row[0], 'none') == row[5] and time_between_dates(row[6], date_of_train[row[0]]) <= 30:
            has_bought_brand_30[row[0]] = has_bought_brand_30.get(row[0], 0) + 1
        if brand_of_train.get(row[0], 'none') == row[5] and time_between_dates(row[6], date_of_train[row[0]]) <= 60:
            has_bought_brand_60[row[0]] = has_bought_brand_60.get(row[0], 0) + 1
        if brand_of_train.get(row[0], 'none') == row[5] and time_between_dates(row[6], date_of_train[row[0]]) <= 180:
            has_bought_brand_180[row[0]] = has_bought_brand_180.get(row[0], 0) + 1
        total_shopper_spend[row[0]] = total_shopper_spend.get(row[0], 0) + float(row[10])

        N = 1000000
        if steps % N == 0:
            print >>sys.stderr, steps / N,
        steps += 1
    print >>sys.stderr

    # Save the results in text files.
    saveIt(has_bought_company, 'has_bought_company.txt')
    saveIt(has_bought_company_a, 'has_bought_company_a.txt')
    saveIt(has_bought_company_q, 'has_bought_company_q.txt')
    saveIt(has_bought_company_30, 'has_bought_company_30.txt')
    saveIt(has_bought_company_60, 'has_bought_company_60.txt')
    saveIt(has_bought_company_180, 'has_bought_company_180.txt')
    saveIt(has_bought_category, 'has_bought_category.txt')
    saveIt(has_bought_category_a, 'has_bought_category_a.txt')
    saveIt(has_bought_category_q, 'has_bought_category_q.txt')
    saveIt(has_bought_category_30, 'has_bought_category_30.txt')
    saveIt(has_bought_category_60, 'has_bought_category_60.txt')
    saveIt(has_bought_category_180, 'has_bought_category_180.txt')
    saveIt(has_bought_brand, 'has_bought_brand.txt')
    saveIt(has_bought_brand_a, 'has_bought_brand_a.txt')
    saveIt(has_bought_brand_q, 'has_bought_brand_q.txt')
    saveIt(has_bought_brand_30, 'has_bought_brand_30.txt')
    saveIt(has_bought_brand_60, 'has_bought_brand_60.txt')
    saveIt(has_bought_brand_180, 'has_bought_brand_180.txt')
    saveIt(total_shopper_spend, 'total_shopper_spend.txt')


def computeFeaturesSecondPass():
    
    has_bought_company = loadIt('has_bought_company.txt')
    has_bought_category = loadIt('has_bought_category.txt')
    has_bought_brand = loadIt('has_bought_brand.txt')

    has_never_bought_company = {}
    for key in has_bought_company:
        if has_bought_company[key] == 0:
            has_never_bought_company[key] = 1
        else:
            has_never_bought_company[key] = 0

    has_never_bought_category = {}
    for key in has_bought_category:
        if has_bought_category[key] == 0:
            has_never_bought_category[key] = 1
        else:
            has_never_bought_category[key] = 0
              
              
    has_never_bought_brand = {}
    for key in has_bought_brand:
        if has_bought_brand[key] == 0:
            has_never_bought_brand[key] = 1
        else:
            has_never_bought_brand[key] = 0

    saveIt(has_never_bought_company, 'has_never_bought_company.txt')
    saveIt(has_never_bought_category, 'has_never_bought_category.txt')
    saveIt(has_never_bought_brand, 'has_never_bought_brand.txt')
    

def computeFeaturesThirdPass():
    
    has_never_bought_company = loadIt('has_never_bought_company.txt')
    has_never_bought_category = loadIt('has_never_bought_category.txt')
    has_never_bought_brand = loadIt('has_never_bought_brand.txt')

    has_bought_company_brand = {}
    has_bought_company_category = {}
    has_bought_category_brand = {} 
    has_bought_company_category_brand = {}
    
    has_never_bought_company_category = {}
    has_never_bought_company_brand = {}
    has_never_bought_category_brand = {}
    has_never_bought_company_category_brand = {}
      
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
          

        if has_never_bought_company[shopper] == 1 and has_never_bought_category[shopper] == 1 and has_never_bought_brand[shopper] == 1:
            has_never_bought_company_category_brand[shopper] = 1
        else:
            has_never_bought_company_category_brand[shopper] = 0

    saveIt(has_bought_company_brand, 'has_bought_company_brand.txt')
    saveIt(has_bought_company_category, 'has_bought_company_category.txt')
    saveIt(has_bought_category_brand, 'has_bought_category_brand.txt')
    saveIt(has_bought_company_category_brand, 'has_bought_company_category_brand.txt')
    
    saveIt(has_never_bought_company_brand, 'has_never_bought_company_brand.txt')
    saveIt(has_never_bought_company_category, 'has_never_bought_company_category.txt')
    saveIt(has_never_bought_category_brand, 'has_never_bought_category_brand.txt')
    saveIt(has_never_bought_company_category_brand, 'has_never_bought_company_category_brand.txt')


# Uncomment to re-compute the features.

# computeFeaturesFirstPass()
# computeFeaturesSecondPass()
# computeFeaturesThirdPass()


has_bought_company_file = open('has_bought_company.txt', 'r')
has_bought_company_a_file = open('has_bought_company_a.txt', 'r')
has_bought_company_q_file = open('has_bought_company_q.txt', 'r')
has_bought_company_30_file = open('has_bought_company_30.txt', 'r')
has_bought_company_60_file = open('has_bought_company_60.txt', 'r')
has_bought_company_180_file = open('has_bought_company_180.txt', 'r')
has_bought_category_file = open('has_bought_category.txt', 'r')
has_bought_category_a_file = open('has_bought_category_a.txt', 'r')
has_bought_category_q_file = open('has_bought_category_q.txt', 'r')
has_bought_category_30_file = open('has_bought_category_30.txt', 'r')
has_bought_category_60_file = open('has_bought_category_60.txt', 'r')
has_bought_category_180_file = open('has_bought_category_180.txt', 'r')
has_bought_brand_file = open('has_bought_company.txt', 'r')
has_bought_brand_a_file = open('has_bought_company_a.txt', 'r')
has_bought_brand_q_file = open('has_bought_company_q.txt', 'r')
has_bought_brand_30_file = open('has_bought_company_30.txt', 'r')
has_bought_brand_60_file = open('has_bought_company_60.txt', 'r')
has_bought_brand_180_file = open('has_bought_company_180.txt', 'r')
has_never_bought_company_file = open('has_never_bought_company.txt', 'r')
has_never_bought_category_file = open('has_never_bought_category.txt', 'r')
has_never_bought_brand_file = open('has_never_bought_brand.txt', 'r')
has_bought_company_brand_file = open('has_bought_company_brand.txt', 'r')
has_bought_company_category_file = open('has_bought_company_category.txt', 'r')
has_bought_category_brand_file = open('has_bought_category_brand.txt', 'r')
has_bought_company_category_brand_file = open('has_bought_company_category_brand.txt', 'r')
has_never_bought_company_brand_file = open('has_bought_company_brand.txt', 'r')
has_never_bought_company_category_file = open('has_bought_company_category.txt', 'r')
has_never_bought_category_brand_file = open('has_bought_category_brand.txt', 'r')
has_never_bought_company_category_brand_file = open('has_bought_company_category_brand.txt', 'r')
total_shopper_spend_file = open('total_shopper_spend.txt', 'r')

has_bought_company = {}
has_bought_company_a = {}
has_bought_company_q = {}
has_bought_company_30 = {}
has_bought_company_60 = {}
has_bought_company_180 = {}
has_bought_category = {}
has_bought_category_a = {}
has_bought_category_q = {}
has_bought_category_30 = {}
has_bought_category_60 = {}
has_bought_category_180 = {}
has_bought_brand = {}
has_bought_brand_a = {}
has_bought_brand_q = {}
has_bought_brand_30 = {}
has_bought_brand_60 = {}
has_bought_brand_180 = {}
has_never_bought_company = {}
has_never_bought_category = {}
has_never_bought_brand = {}
has_bought_company_brand = {}
has_bought_company_category = {}
has_bought_category_brand = {}
has_bought_company_category_brand = {}
has_never_bought_company_brand = {}
has_never_bought_company_category = {}
has_never_bought_category_brand = {}
has_never_bought_company_category_brand = {}
total_shopper_spend = {}


for line in has_bought_company_file:
    row = line.split()
    has_bought_company[row[0]] = row[1]
for line in has_bought_company_a_file:
    row = line.split()
    has_bought_company_a[row[0]] = row[1]
for line in has_bought_company_q_file:
    row = line.split()
    has_bought_company_q[row[0]] = row[1]
for line in has_bought_company_30_file:
    row = line.split()
    has_bought_company_30[row[0]] = row[1]
for line in has_bought_company_60_file:
    row = line.split()
    has_bought_company_60[row[0]] = row[1]
for line in has_bought_company_180_file:
    row = line.split()
    has_bought_company_180[row[0]] = row[1]
for line in has_bought_category_file:
    row = line.split()
    has_bought_category[row[0]] = row[1]
for line in has_bought_category_a_file:
    row = line.split()
    has_bought_category_a[row[0]] = row[1]
for line in has_bought_category_q_file:
    row = line.split()
    has_bought_category_q[row[0]] = row[1]
for line in has_bought_category_30_file:
    row = line.split()
    has_bought_category_30[row[0]] = row[1]
for line in has_bought_category_60_file:
    row = line.split()
    has_bought_category_60[row[0]] = row[1]
for line in has_bought_category_180_file:
    row = line.split()
    has_bought_category_180[row[0]] = row[1]
for line in has_bought_brand_file:
    row = line.split()
    has_bought_brand[row[0]] = row[1]
for line in has_bought_brand_a_file:
    row = line.split()
    has_bought_brand_a[row[0]] = row[1]
for line in has_bought_brand_q_file:
    row = line.split()
    has_bought_brand_q[row[0]] = row[1]
for line in has_bought_brand_30_file:
    row = line.split()
    has_bought_brand_30[row[0]] = row[1]
for line in has_bought_brand_60_file:
    row = line.split()
    has_bought_brand_60[row[0]] = row[1]
for line in has_bought_brand_180_file:
    row = line.split()
    has_bought_brand_180[row[0]] = row[1]
for line in has_never_bought_company_file:
    row = line.split()
    has_never_bought_company[row[0]] = row[1]
for line in has_never_bought_category_file:
    row = line.split()
    has_never_bought_category[row[0]] = row[1]
for line in has_never_bought_brand_file:
    row = line.split()
    has_never_bought_brand[row[0]] = row[1]
for line in has_bought_company_brand_file:
    row = line.split()
    has_bought_company_brand[row[0]] = row[1]
for line in has_bought_company_category_file:
    row = line.split()
    has_bought_company_category[row[0]] = row[1]
for line in has_bought_category_brand_file:
    row = line.split()
    has_bought_category_brand[row[0]] = row[1]
for line in has_bought_company_category_brand_file:
    row = line.split()
    has_bought_company_category_brand[row[0]] = row[1]
for line in has_never_bought_company_brand_file:
    row = line.split()
    has_never_bought_company_brand[row[0]] = row[1]
for line in has_never_bought_company_category_file:
    row = line.split()
    has_never_bought_company_category[row[0]] = row[1]
for line in has_never_bought_category_brand_file:
    row = line.split()
    has_never_bought_category_brand[row[0]] = row[1]
for line in has_never_bought_company_category_brand_file:
    row = line.split()
    has_never_bought_company_category_brand[row[0]] = row[1]
for line in total_shopper_spend_file:
    row = line.split()
    total_shopper_spend[row[0]] = row[1]

shopper_values = {}
for shopper in has_bought_company:
    

    print shopper
    print has_bought_company[shopper]
    print str(has_bought_company_a[shopper])
    shopper_values[shopper] = shopper + ' ' + str(has_bought_company[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_company_a[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_company_q[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_company_30[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_company_60[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_category[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_category_a[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_category_q[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_category_30[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_category_60[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_category_180[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_brand_a[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_brand_q[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_brand_30[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_brand_60[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_brand_180[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_never_bought_company[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_never_bought_category[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_never_bought_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_company_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_company_category[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_category_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_bought_company_category_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_never_bought_company_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_never_bought_company_category[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_never_bought_category_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(has_never_bought_company_category_brand[shopper])
    shopper_values[shopper] = shopper_values[shopper] + ' ' + str(total_shopper_spend[shopper])
    
variables = 'shopper' + ' ' + 'has_bought_company'
variables = variables + ' ' + 'has_bought_company_a'
variables = variables + ' ' + 'has_bought_company_q'
variables = variables + ' ' + 'has_bought_company_30'
variables = variables + ' ' + 'has_bought_company_60'
variables = variables + ' ' + 'has_bought_company_180'
variables = variables + ' ' + 'has_bought_category'
variables = variables + ' ' + 'has_bought_category_a'
variables = variables + ' ' + 'has_bought_category_q'
variables = variables + ' ' + 'has_bought_category_30'
variables = variables + ' ' + 'has_bought_category_60'
variables = variables + ' ' + 'has_bought_category_180'
variables = variables + ' ' + 'has_bought_brand'
variables = variables + ' ' + 'has_bought_brand_a'
variables = variables + ' ' + 'has_bought_brand_q'
variables = variables + ' ' + 'has_bought_brand_30'
variables = variables + ' ' + 'has_bought_brand_60'
variables = variables + ' ' + 'has_bought_brand_180'
variables = variables + ' ' + 'has_never_bought_company'
variables = variables + ' ' + 'has_never_bought_category'
variables = variables + ' ' + 'has_never_bought_brand'
variables = variables + ' ' + 'has_bought_company_brand'
variables = variables + ' ' + 'has_bought_company_category'
variables = variables + ' ' + 'has_bought_category_brand'
variables = variables + ' ' + 'has_bought_company_category_brand'
variables = variables + ' ' + 'has_never_bought_company_brand'
variables = variables + ' ' + 'has_never_bought_company_category'
variables = variables + ' ' + 'has_never_bought_category_brand'
variables = variables + ' ' + 'has_never_bought_company_category_brand'
variables = variables + ' ' + 'total_shopper_spend'

    
    

fileid = open('shoppers_challenge_features.txt', 'w')
fileid.write(variables + '\n')
for shopper in has_bought_company:
    fileid.write(shopper_values[shopper] + '\n')



