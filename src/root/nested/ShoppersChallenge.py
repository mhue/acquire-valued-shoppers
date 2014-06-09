'''
Created on Jun 5, 2014

@author: philipp
'''
import os
import sys
from time import strftime
from datetime import datetime
from time import mktime
from collections import defaultdict
from sklearn.metrics import roc_auc_score
import sklearn
import csv
import glob
import subprocess
import gzip
import numpy


company_of_offer = {}
category_of_offer = {}
brand_of_offer = {}
offer_value_of_offer = {}
offer_quantity_of_offer = {}

def readOffers():
    
    offersfile = file('offers.csv', 'rU')
    offers = csv.reader(offersfile)
    header_offers = offers.next()
    
    offerIndex = header_offers.index('offer')
    categoryIndex = header_offers.index('category')
    quantityIndex = header_offers.index('quantity')
    companyIndex = header_offers.index('company')
    valueIndex = header_offers.index('offervalue')
    brandIndex = header_offers.index('brand')
    
    for row in offers:
        
        offer = row[offerIndex]
        category = row[categoryIndex]
        quantity = row[quantityIndex]
        company = row[companyIndex]
        brand = row[brandIndex]
        value = row[valueIndex]
        
        company_of_offer[offer] = company
        category_of_offer[offer] = category
        brand_of_offer[offer] = brand
        offer_value_of_offer[offer] = value
        offer_quantity_of_offer[offer] = quantity

company_of_shopper = {}
category_of_shopper = {}
brand_of_shopper = {}
offer_value_of_shopper = {}
offer_quantity_of_shopper = {}
date_of_shopper = {}

def readShoppers():
    for phase in ['train', 'test']:
        fid = open(phase + 'History.csv')
        cr = csv.reader(fid)
        header = cr.next()
        IDIndex = header.index('id')
        offerIndex = header.index('offer')
        dateIndex = header.index('offerdate')
        for row in cr:
            ID = row[IDIndex]
            offer = row[offerIndex]
            date = row[dateIndex]
            company_of_shopper[ID] = company_of_offer[offer]
            category_of_shopper[ID] = category_of_offer[offer]
            brand_of_shopper[ID] = brand_of_offer[offer]
            offer_value_of_shopper[ID] = float(offer_value_of_offer[offer])
            offer_quantity_of_shopper[ID] = float(offer_quantity_of_offer[offer])
            date_of_shopper[ID] = date
        fid.close()
    
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

def saveIt(d, outfile):
    fileid = open('features/' + outfile, 'w')
    for key in d:
        fileid.write(key + ' ' + str(d[key]) + '\n')
    fileid.close()

def loadIt(infile, convert = False, folder = 'features'):
    fileid = open(folder + '/' + infile)
    d = {}
    for row in fileid:
        words = row.split()
        key = words[0]
        if convert:
            value = int(words[1])
        else:
            value = words[1]
        d[key] = value
    return d

def getIds(phase):
    fid = open(phase + 'History.csv', 'r')
    cr = csv.reader(fid)
    header = cr.next()
    ids = [row[0] for row in cr]
    fid.close()
    return ids

def computeTransactionsSubset():
    '''
    Computes a subset of the transactions.
    Only keep the rows with a known company, category or offer.
    '''
    
    if os.path.exists('transactions_subset.csv.gz'):
        return

    readOffers()
    
    companies = dict(zip(company_of_offer.values(), [0] * len(company_of_offer)))
    categories = dict(zip(category_of_offer.values(), [0] * len(category_of_offer)))
    brands = dict(zip(brand_of_offer.values(), [0] * len(brand_of_offer)))
    
    fin = gzip.GzipFile('transactions.csv.gz')
    fout = gzip.GzipFile('transactions_subset.csv.gz', 'w')
    cr = csv.reader(fin)
    header = cr.next()
    fout.write(','.join(header) + '\n')

    IDIndex = header.index('id')
    categoryIndex = header.index('category')
    companyIndex = header.index('company')
    brandIndex = header.index('brand')
    dateIndex = header.index('date')
    quantityIndex = header.index('purchasequantity')

    steps = 0
    N = 1000000
    for row in cr:
        ID = row[IDIndex]
        company = row[companyIndex]
        category = row[categoryIndex]
        brand = row[brandIndex]
        quantity = row[quantityIndex]
        if company in companies or category in categories or brand in brands:
            fout.write(','.join(row) + '\n')
        if steps % N == 0:
            print >> sys.stderr, steps / N,
        steps += 1
    print >> sys.stderr
    fin.close()
    fout.close()
    

def computeFeaturesFirstPass():
    '''
    Create the first features.
    Those features are quick to compute, because only 10 % of the transactions
    are needed.
    '''

    readOffers()
    readShoppers()
    
    ids = getIds('train') + getIds('test')
    n = len(ids)
    
    company_b = dict(zip(ids, [0] * n))
    company_n = dict(zip(ids, [0] * n))
    company_a = dict(zip(ids, [0] * n))
    company_q = dict(zip(ids, [0] * n))
    company_qm_ = dict(zip(ids, [0] * n))
    company_qm_1 = dict(zip(ids, [0] * n))
    company_qm_CT = dict(zip(ids, [0] * n))
    company_qm_FT = dict(zip(ids, [0] * n))
    company_qm_GL = dict(zip(ids, [0] * n))
    company_qm_LB = dict(zip(ids, [0] * n))
    company_qm_LT = dict(zip(ids, [0] * n))
    company_qm_OZ = dict(zip(ids, [0] * n))
    company_qm_PT = dict(zip(ids, [0] * n))
    company_qm_QT = dict(zip(ids, [0] * n))
    company_qm_RL = dict(zip(ids, [0] * n))
    company_qm_YD = dict(zip(ids, [0] * n))
    company_30 = dict(zip(ids, [0] * n))
    company_60 = dict(zip(ids, [0] * n))
    company_180 = dict(zip(ids, [0] * n))
    category_b = dict(zip(ids, [0] * n))
    category_n = dict(zip(ids, [0] * n))
    category_a = dict(zip(ids, [0] * n))
    category_q = dict(zip(ids, [0] * n))
    category_qm_ = dict(zip(ids, [0] * n))
    category_qm_1 = dict(zip(ids, [0] * n))
    category_qm_CT = dict(zip(ids, [0] * n))
    category_qm_FT = dict(zip(ids, [0] * n))
    category_qm_GL = dict(zip(ids, [0] * n))
    category_qm_LB = dict(zip(ids, [0] * n))
    category_qm_LT = dict(zip(ids, [0] * n))
    category_qm_OZ = dict(zip(ids, [0] * n))
    category_qm_PT = dict(zip(ids, [0] * n))
    category_qm_QT = dict(zip(ids, [0] * n))
    category_qm_RL = dict(zip(ids, [0] * n))
    category_qm_YD = dict(zip(ids, [0] * n))
    category_30 = dict(zip(ids, [0] * n))
    category_60 = dict(zip(ids, [0] * n))
    category_180 = dict(zip(ids, [0] * n))
    brand_b = dict(zip(ids, [0] * n))
    brand_n = dict(zip(ids, [0] * n))
    brand_a = dict(zip(ids, [0] * n))
    brand_q = dict(zip(ids, [0] * n))
    brand_qm_ = dict(zip(ids, [0] * n))
    brand_qm_1 = dict(zip(ids, [0] * n))
    brand_qm_CT = dict(zip(ids, [0] * n))
    brand_qm_FT = dict(zip(ids, [0] * n))
    brand_qm_GL = dict(zip(ids, [0] * n))
    brand_qm_LB = dict(zip(ids, [0] * n))
    brand_qm_LT = dict(zip(ids, [0] * n))
    brand_qm_OZ = dict(zip(ids, [0] * n))
    brand_qm_PT = dict(zip(ids, [0] * n))
    brand_qm_QT = dict(zip(ids, [0] * n))
    brand_qm_RL = dict(zip(ids, [0] * n))
    brand_qm_YD = dict(zip(ids, [0] * n))
    brand_30 = dict(zip(ids, [0] * n))
    brand_60 = dict(zip(ids, [0] * n))
    brand_180 = dict(zip(ids, [0] * n))
    company_brand_b = dict(zip(ids, [0] * n))
    company_brand_n = dict(zip(ids, [0] * n))
    company_brand_a = dict(zip(ids, [0] * n))
    company_brand_q = dict(zip(ids, [0] * n))
    company_brand_qm_ = dict(zip(ids, [0] * n))
    company_brand_qm_1 = dict(zip(ids, [0] * n))
    company_brand_qm_CT = dict(zip(ids, [0] * n))
    company_brand_qm_FT = dict(zip(ids, [0] * n))
    company_brand_qm_GL = dict(zip(ids, [0] * n))
    company_brand_qm_LB = dict(zip(ids, [0] * n))
    company_brand_qm_LT = dict(zip(ids, [0] * n))
    company_brand_qm_OZ = dict(zip(ids, [0] * n))
    company_brand_qm_PT = dict(zip(ids, [0] * n))
    company_brand_qm_QT = dict(zip(ids, [0] * n))
    company_brand_qm_RL = dict(zip(ids, [0] * n))
    company_brand_qm_YD = dict(zip(ids, [0] * n))
    company_brand_30 = dict(zip(ids, [0] * n))
    company_brand_60 = dict(zip(ids, [0] * n))
    company_brand_180 = dict(zip(ids, [0] * n))
    company_category_b = dict(zip(ids, [0] * n))
    company_category_n = dict(zip(ids, [0] * n))
    company_category_a = dict(zip(ids, [0] * n))
    company_category_q = dict(zip(ids, [0] * n))
    company_category_qm_ = dict(zip(ids, [0] * n))
    company_category_qm_1 = dict(zip(ids, [0] * n))
    company_category_qm_CT = dict(zip(ids, [0] * n))
    company_category_qm_FT = dict(zip(ids, [0] * n))
    company_category_qm_GL = dict(zip(ids, [0] * n))
    company_category_qm_LB = dict(zip(ids, [0] * n))
    company_category_qm_LT = dict(zip(ids, [0] * n))
    company_category_qm_OZ = dict(zip(ids, [0] * n))
    company_category_qm_PT = dict(zip(ids, [0] * n))
    company_category_qm_QT = dict(zip(ids, [0] * n))
    company_category_qm_RL = dict(zip(ids, [0] * n))
    company_category_qm_YD = dict(zip(ids, [0] * n))
    company_category_30 = dict(zip(ids, [0] * n))
    company_category_60 = dict(zip(ids, [0] * n))
    company_category_180 = dict(zip(ids, [0] * n))
    category_brand_b = dict(zip(ids, [0] * n))
    category_brand_n = dict(zip(ids, [0] * n))
    category_brand_a = dict(zip(ids, [0] * n))
    category_brand_q = dict(zip(ids, [0] * n))
    category_brand_qm_ = dict(zip(ids, [0] * n))
    category_brand_qm_1 = dict(zip(ids, [0] * n))
    category_brand_qm_CT = dict(zip(ids, [0] * n))
    category_brand_qm_FT = dict(zip(ids, [0] * n))
    category_brand_qm_GL = dict(zip(ids, [0] * n))
    category_brand_qm_LB = dict(zip(ids, [0] * n))
    category_brand_qm_LT = dict(zip(ids, [0] * n))
    category_brand_qm_OZ = dict(zip(ids, [0] * n))
    category_brand_qm_PT = dict(zip(ids, [0] * n))
    category_brand_qm_QT = dict(zip(ids, [0] * n))
    category_brand_qm_RL = dict(zip(ids, [0] * n))
    category_brand_qm_YD = dict(zip(ids, [0] * n))
    category_brand_30 = dict(zip(ids, [0] * n))
    category_brand_60 = dict(zip(ids, [0] * n))
    category_brand_180 = dict(zip(ids, [0] * n))
    company_category_brand_b = dict(zip(ids, [0] * n))
    company_category_brand_n = dict(zip(ids, [0] * n))
    company_category_brand_a = dict(zip(ids, [0] * n))
    company_category_brand_q = dict(zip(ids, [0] * n))
    company_category_brand_qm_ = dict(zip(ids, [0] * n))
    company_category_brand_qm_1 = dict(zip(ids, [0] * n))
    company_category_brand_qm_CT = dict(zip(ids, [0] * n))
    company_category_brand_qm_FT = dict(zip(ids, [0] * n))
    company_category_brand_qm_GL = dict(zip(ids, [0] * n))
    company_category_brand_qm_LB = dict(zip(ids, [0] * n))
    company_category_brand_qm_LT = dict(zip(ids, [0] * n))
    company_category_brand_qm_OZ = dict(zip(ids, [0] * n))
    company_category_brand_qm_PT = dict(zip(ids, [0] * n))
    company_category_brand_qm_QT = dict(zip(ids, [0] * n))
    company_category_brand_qm_RL = dict(zip(ids, [0] * n))
    company_category_brand_qm_YD = dict(zip(ids, [0] * n))
    company_category_brand_30 = dict(zip(ids, [0] * n))
    company_category_brand_60 = dict(zip(ids, [0] * n))
    company_category_brand_180 = dict(zip(ids, [0] * n))

    fid = gzip.GzipFile('transactions_subset.csv.gz', 'rU')
    transactions = csv.reader(fid)
    header = transactions.next()

    IDIndex = header.index('id')
    chainIndex = header.index('chain')
    deptIndex = header.index('dept')
    categoryIndex = header.index('category')
    companyIndex = header.index('company')
    brandIndex = header.index('brand')
    dateIndex = header.index('date')
    productsizeIndex = header.index('productsize')
    measureIndex = header.index('productmeasure')
    quantityIndex = header.index('purchasequantity')
    amountIndex = header.index('purchaseamount')
    
    steps = 0
    for row in transactions:

        ID = row[IDIndex]
        chain = row[chainIndex]
        dept = row[deptIndex]
        category = row[categoryIndex]
        company = row[companyIndex]
        brand = row[brandIndex]
        date = row[dateIndex]
        productsize = row[productsizeIndex]
        measure = row[measureIndex]
        quantity = float(row[quantityIndex])
        amount = float(row[amountIndex])

        dt = time_between_dates(date, date_of_shopper[ID])
                
        if company_of_shopper[ID] == company:
            company_b[ID] = 1
            company_n[ID] += 1
            company_a[ID] += amount
            company_q[ID] += quantity
            if measure == '':
                company_qm_[ID] += quantity
            if measure == '1':
                company_qm_1[ID] += quantity
            if measure == 'CT':
                company_qm_CT[ID] += quantity
            if measure == 'FT':
                company_qm_FT[ID] += quantity
            if measure == 'GL':
                company_qm_GL[ID] += quantity
            if measure == 'LB':
                company_qm_LB[ID] += quantity
            if measure == 'LT':
                company_qm_LT[ID] += quantity
            if measure == 'OZ':
                company_qm_OZ[ID] += quantity
            if measure == 'PT':
                company_qm_PT[ID] += quantity
            if measure == 'QT':
                company_qm_QT[ID] += quantity
            if measure == 'RL':
                company_qm_RL[ID] += quantity
            if measure == 'YD':
                company_qm_YD[ID] += quantity
            if dt <= 30:
                company_30[ID] += 1
            if dt <= 60:
                company_60[ID] += 1
            if dt <= 180:
                company_180[ID] += 1
        if category_of_shopper[ID] == category:
            category_b[ID] = 1
            category_n[ID] += 1
            category_a[ID] += amount
            category_q[ID] += quantity
            if measure == '':
                category_qm_[ID] += quantity
            if measure == '1':
                category_qm_1[ID] += quantity
            if measure == 'CT':
                category_qm_CT[ID] += quantity
            if measure == 'FT':
                category_qm_FT[ID] += quantity
            if measure == 'GL':
                category_qm_GL[ID] += quantity
            if measure == 'LB':
                category_qm_LB[ID] += quantity
            if measure == 'LT':
                category_qm_LT[ID] += quantity
            if measure == 'OZ':
                category_qm_OZ[ID] += quantity
            if measure == 'PT':
                category_qm_PT[ID] += quantity
            if measure == 'QT':
                category_qm_QT[ID] += quantity
            if measure == 'RL':
                category_qm_RL[ID] += quantity
            if measure == 'YD':
                category_qm_YD[ID] += quantity
            if dt <= 30:
                category_30[ID] += 1
            if dt <= 60:
                category_60[ID] += 1
            if dt <= 180:
                category_180[ID] += 1
        if brand_of_shopper[ID] == brand:
            brand_b[ID] = 1
            brand_n[ID] += 1
            brand_a[ID] += amount
            brand_q[ID] += quantity
            if measure == '':
                brand_qm_[ID] += quantity
            if measure == '1':
                brand_qm_1[ID] += quantity
            if measure == 'CT':
                brand_qm_CT[ID] += quantity
            if measure == 'FT':
                brand_qm_FT[ID] += quantity
            if measure == 'GL':
                brand_qm_GL[ID] += quantity
            if measure == 'LB':
                brand_qm_LB[ID] += quantity
            if measure == 'LT':
                brand_qm_LT[ID] += quantity
            if measure == 'OZ':
                brand_qm_OZ[ID] += quantity
            if measure == 'PT':
                brand_qm_PT[ID] += quantity
            if measure == 'QT':
                brand_qm_QT[ID] += quantity
            if measure == 'RL':
                brand_qm_RL[ID] += quantity
            if measure == 'YD':
                brand_qm_YD[ID] += quantity
            if dt <= 30:
                brand_30[ID] += 1
            if dt <= 60:
                brand_60[ID] += 1
            if dt <= 180:
                brand_180[ID] += 1
        if company_of_shopper[ID] == company and brand_of_shopper[ID] == brand:
            company_brand_b[ID] = 1
            company_brand_n[ID] += 1
            company_brand_a[ID] += amount
            company_brand_q[ID] += quantity
            if measure == '':
                company_brand_qm_[ID] += quantity
            if measure == '1':
                company_brand_qm_1[ID] += quantity
            if measure == 'CT':
                company_brand_qm_CT[ID] += quantity
            if measure == 'FT':
                company_brand_qm_FT[ID] += quantity
            if measure == 'GL':
                company_brand_qm_GL[ID] += quantity
            if measure == 'LB':
                company_brand_qm_LB[ID] += quantity
            if measure == 'LT':
                company_brand_qm_LT[ID] += quantity
            if measure == 'OZ':
                company_brand_qm_OZ[ID] += quantity
            if measure == 'PT':
                company_brand_qm_PT[ID] += quantity
            if measure == 'QT':
                company_brand_qm_QT[ID] += quantity
            if measure == 'RL':
                company_brand_qm_RL[ID] += quantity
            if measure == 'YD':
                company_brand_qm_YD[ID] += quantity

            if dt <= 30:
                company_brand_30[ID] += 1
            if dt <= 60:
                company_brand_60[ID] += 1
            if dt <= 180:
                company_brand_180[ID] += 1
        if company_of_shopper[ID] == company and category_of_shopper[ID] == category:
            company_category_b[ID] = 1
            company_category_n[ID] += 1
            company_category_a[ID] += amount
            company_category_q[ID] += quantity
            if measure == '':
                company_category_qm_[ID] += quantity
            if measure == '1':
                company_category_qm_1[ID] += quantity
            if measure == 'CT':
                company_category_qm_CT[ID] += quantity
            if measure == 'FT':
                company_category_qm_FT[ID] += quantity
            if measure == 'GL':
                company_category_qm_GL[ID] += quantity
            if measure == 'LB':
                company_category_qm_LB[ID] += quantity
            if measure == 'LT':
                company_category_qm_LT[ID] += quantity
            if measure == 'OZ':
                company_category_qm_OZ[ID] += quantity
            if measure == 'PT':
                company_category_qm_PT[ID] += quantity
            if measure == 'QT':
                company_category_qm_QT[ID] += quantity
            if measure == 'RL':
                company_category_qm_RL[ID] += quantity
            if measure == 'YD':
                company_category_qm_YD[ID] += quantity

            if dt <= 30:
                company_category_30[ID] += 1
            if dt <= 60:
                company_category_60[ID] += 1
            if dt <= 180:
                company_category_180[ID] += 1
        if category_of_shopper[ID] == category and brand_of_shopper[ID] == brand:
            category_brand_b[ID] = 1
            category_brand_n[ID] += 1
            category_brand_a[ID] += amount
            category_brand_q[ID] += quantity
            if measure == '':
                category_brand_qm_[ID] += quantity
            if measure == '1':
                category_brand_qm_1[ID] += quantity
            if measure == 'CT':
                category_brand_qm_CT[ID] += quantity
            if measure == 'FT':
                category_brand_qm_FT[ID] += quantity
            if measure == 'GL':
                category_brand_qm_GL[ID] += quantity
            if measure == 'LB':
                category_brand_qm_LB[ID] += quantity
            if measure == 'LT':
                category_brand_qm_LT[ID] += quantity
            if measure == 'OZ':
                category_brand_qm_OZ[ID] += quantity
            if measure == 'PT':
                category_brand_qm_PT[ID] += quantity
            if measure == 'QT':
                category_brand_qm_QT[ID] += quantity
            if measure == 'RL':
                category_brand_qm_RL[ID] += quantity
            if measure == 'YD':
                category_brand_qm_YD[ID] += quantity
            if dt <= 30:
                category_brand_30[ID] += 1
            if dt <= 60:
                category_brand_60[ID] += 1
            if dt <= 180:
                category_brand_180[ID] += 1
        if company_of_shopper[ID] == company and category_of_shopper[ID] == category and brand_of_shopper[ID] == brand:
            company_category_brand_b[ID] = 1
            company_category_brand_n[ID] += 1
            company_category_brand_a[ID] += amount
            company_category_brand_q[ID] += quantity
            if measure == '':
                company_category_brand_qm_[ID] += quantity
            if measure == '1':
                company_category_brand_qm_1[ID] += quantity
            if measure == 'CT':
                company_category_brand_qm_CT[ID] += quantity
            if measure == 'FT':
                company_category_brand_qm_FT[ID] += quantity
            if measure == 'GL':
                company_category_brand_qm_GL[ID] += quantity
            if measure == 'LB':
                company_category_brand_qm_LB[ID] += quantity
            if measure == 'LT':
                company_category_brand_qm_LT[ID] += quantity
            if measure == 'OZ':
                company_category_brand_qm_OZ[ID] += quantity
            if measure == 'PT':
                company_category_brand_qm_PT[ID] += quantity
            if measure == 'QT':
                company_category_brand_qm_QT[ID] += quantity
            if measure == 'RL':
                company_category_brand_qm_RL[ID] += quantity
            if measure == 'YD':
                company_category_brand_qm_YD[ID] += quantity
            if dt <= 30:
                company_category_brand_30[ID] += 1
            if dt <= 60:
                company_category_brand_60[ID] += 1
            if dt <= 180:
                company_category_brand_180[ID] += 1

        N = 1000000
        if steps % N == 0:
            print >>sys.stderr, steps / N,
        steps += 1
        

    fid.close()
    print >>sys.stderr

    # Save the results in text files.
    saveIt(company_b, 'company_b.txt')
    saveIt(company_n, 'company_n.txt')
    saveIt(company_a, 'company_a.txt')
    saveIt(company_q, 'company_q.txt')
    saveIt(company_qm_, 'company_qm_.txt')
    saveIt(company_qm_1, 'company_qm_1.txt')
    saveIt(company_qm_CT, 'company_qm_CT.txt')
    saveIt(company_qm_FT, 'company_qm_FT.txt')
    saveIt(company_qm_GL, 'company_qm_GL.txt')
    saveIt(company_qm_LB, 'company_qm_LB.txt')
    saveIt(company_qm_LT, 'company_qm_LT.txt')
    saveIt(company_qm_OZ, 'company_qm_OZ.txt')
    saveIt(company_qm_PT, 'company_qm_PT.txt')
    saveIt(company_qm_QT, 'company_qm_QT.txt')
    saveIt(company_qm_RL, 'company_qm_RL.txt')
    saveIt(company_qm_YD, 'company_qm_YD.txt')
    saveIt(company_30, 'company_30.txt')
    saveIt(company_60, 'company_60.txt')
    saveIt(company_180, 'company_180.txt')
    saveIt(category_b, 'category_b.txt')
    saveIt(category_n, 'category_n.txt')
    saveIt(category_a, 'category_a.txt')
    saveIt(category_q, 'category_q.txt')
    saveIt(category_qm_, 'category_qm_.txt')
    saveIt(category_qm_1, 'category_qm_1.txt')
    saveIt(category_qm_CT, 'category_qm_CT.txt')
    saveIt(category_qm_FT, 'category_qm_FT.txt')
    saveIt(category_qm_GL, 'category_qm_GL.txt')
    saveIt(category_qm_LB, 'category_qm_LB.txt')
    saveIt(category_qm_LT, 'category_qm_LT.txt')
    saveIt(category_qm_OZ, 'category_qm_OZ.txt')
    saveIt(category_qm_PT, 'category_qm_PT.txt')
    saveIt(category_qm_QT, 'category_qm_QT.txt')
    saveIt(category_qm_RL, 'category_qm_RL.txt')
    saveIt(category_qm_YD, 'category_qm_YD.txt')
    saveIt(category_30, 'category_30.txt')
    saveIt(category_60, 'category_60.txt')
    saveIt(category_180, 'category_180.txt')
    saveIt(brand_b, 'brand_b.txt')
    saveIt(brand_n, 'brand_n.txt')
    saveIt(brand_a, 'brand_a.txt')
    saveIt(brand_q, 'brand_q.txt')
    saveIt(brand_qm_, 'brand_qm_.txt')
    saveIt(brand_qm_1, 'brand_qm_1.txt')
    saveIt(brand_qm_CT, 'brand_qm_CT.txt')
    saveIt(brand_qm_FT, 'brand_qm_FT.txt')
    saveIt(brand_qm_GL, 'brand_qm_GL.txt')
    saveIt(brand_qm_LB, 'brand_qm_LB.txt')
    saveIt(brand_qm_LT, 'brand_qm_LT.txt')
    saveIt(brand_qm_OZ, 'brand_qm_OZ.txt')
    saveIt(brand_qm_PT, 'brand_qm_PT.txt')
    saveIt(brand_qm_QT, 'brand_qm_QT.txt')
    saveIt(brand_qm_RL, 'brand_qm_RL.txt')
    saveIt(brand_qm_YD, 'brand_qm_YD.txt')
    saveIt(brand_30, 'brand_30.txt')
    saveIt(brand_60, 'brand_60.txt')
    saveIt(brand_180, 'brand_180.txt')
    saveIt(company_brand_b, 'company_brand_b.txt')
    saveIt(company_brand_n, 'company_brand_n.txt')
    saveIt(company_brand_a, 'company_brand_a.txt')
    saveIt(company_brand_q, 'company_brand_q.txt')
    saveIt(company_brand_qm_, 'company_brand_qm_.txt')
    saveIt(company_brand_qm_1, 'company_brand_qm_1.txt')
    saveIt(company_brand_qm_CT, 'company_brand_qm_CT.txt')
    saveIt(company_brand_qm_FT, 'company_brand_qm_FT.txt')
    saveIt(company_brand_qm_GL, 'company_brand_qm_GL.txt')
    saveIt(company_brand_qm_LB, 'company_brand_qm_LB.txt')
    saveIt(company_brand_qm_LT, 'company_brand_qm_LT.txt')
    saveIt(company_brand_qm_OZ, 'company_brand_qm_OZ.txt')
    saveIt(company_brand_qm_PT, 'company_brand_qm_PT.txt')
    saveIt(company_brand_qm_QT, 'company_brand_qm_QT.txt')
    saveIt(company_brand_qm_RL, 'company_brand_qm_RL.txt')
    saveIt(company_brand_qm_YD, 'company_brand_qm_YD.txt')
    saveIt(company_brand_30, 'company_brand_30.txt')
    saveIt(company_brand_60, 'company_brand_60.txt')
    saveIt(company_brand_180, 'company_brand_180.txt')
    saveIt(company_category_b, 'company_category_b.txt')
    saveIt(company_category_n, 'company_category_n.txt')
    saveIt(company_category_a, 'company_category_a.txt')
    saveIt(company_category_q, 'company_category_q.txt')
    saveIt(company_category_qm_, 'company_category_qm_.txt')
    saveIt(company_category_qm_1, 'company_category_qm_1.txt')
    saveIt(company_category_qm_CT, 'company_category_qm_CT.txt')
    saveIt(company_category_qm_FT, 'company_category_qm_FT.txt')
    saveIt(company_category_qm_GL, 'company_category_qm_GL.txt')
    saveIt(company_category_qm_LB, 'company_category_qm_LB.txt')
    saveIt(company_category_qm_LT, 'company_category_qm_LT.txt')
    saveIt(company_category_qm_OZ, 'company_category_qm_OZ.txt')
    saveIt(company_category_qm_PT, 'company_category_qm_PT.txt')
    saveIt(company_category_qm_QT, 'company_category_qm_QT.txt')
    saveIt(company_category_qm_RL, 'company_category_qm_RL.txt')
    saveIt(company_category_qm_YD, 'company_category_qm_YD.txt')
    saveIt(company_category_30, 'company_category_30.txt')
    saveIt(company_category_60, 'company_category_60.txt')
    saveIt(company_category_180, 'company_category_180.txt')
    saveIt(category_brand_b, 'category_brand_b.txt')
    saveIt(category_brand_n, 'category_brand_n.txt')
    saveIt(category_brand_a, 'category_brand_a.txt')
    saveIt(category_brand_q, 'category_brand_q.txt')
    saveIt(category_brand_qm_, 'category_brand_qm_.txt')
    saveIt(category_brand_qm_1, 'category_brand_qm_1.txt')
    saveIt(category_brand_qm_CT, 'category_brand_qm_CT.txt')
    saveIt(category_brand_qm_FT, 'category_brand_qm_FT.txt')
    saveIt(category_brand_qm_GL, 'category_brand_qm_GL.txt')
    saveIt(category_brand_qm_LB, 'category_brand_qm_LB.txt')
    saveIt(category_brand_qm_LT, 'category_brand_qm_LT.txt')
    saveIt(category_brand_qm_OZ, 'category_brand_qm_OZ.txt')
    saveIt(category_brand_qm_PT, 'category_brand_qm_PT.txt')
    saveIt(category_brand_qm_QT, 'category_brand_qm_QT.txt')
    saveIt(category_brand_qm_RL, 'category_brand_qm_RL.txt')
    saveIt(category_brand_qm_YD, 'category_brand_qm_YD.txt')
    saveIt(category_brand_30, 'category_brand_30.txt')
    saveIt(category_brand_60, 'category_brand_60.txt')
    saveIt(category_brand_180, 'category_brand_180.txt')
    saveIt(company_category_brand_b, 'company_category_brand_b.txt')
    saveIt(company_category_brand_n, 'company_category_brand_n.txt')
    saveIt(company_category_brand_a, 'company_category_brand_a.txt')
    saveIt(company_category_brand_q, 'company_category_brand_q.txt')
    saveIt(company_category_brand_qm_, 'company_category_brand_qm_.txt')
    saveIt(company_category_brand_qm_1, 'company_category_brand_qm_1.txt')
    saveIt(company_category_brand_qm_CT, 'company_category_brand_qm_CT.txt')
    saveIt(company_category_brand_qm_FT, 'company_category_brand_qm_FT.txt')
    saveIt(company_category_brand_qm_GL, 'company_category_brand_qm_GL.txt')
    saveIt(company_category_brand_qm_LB, 'company_category_brand_qm_LB.txt')
    saveIt(company_category_brand_qm_LT, 'company_category_brand_qm_LT.txt')
    saveIt(company_category_brand_qm_OZ, 'company_category_brand_qm_OZ.txt')
    saveIt(company_category_brand_qm_PT, 'company_category_brand_qm_PT.txt')
    saveIt(company_category_brand_qm_QT, 'company_category_brand_qm_QT.txt')
    saveIt(company_category_brand_qm_RL, 'company_category_brand_qm_RL.txt')
    saveIt(company_category_brand_qm_YD, 'company_category_brand_qm_YD.txt')
    saveIt(company_category_brand_30, 'company_category_brand_30.txt')
    saveIt(company_category_brand_60, 'company_category_brand_60.txt')
    saveIt(company_category_brand_180, 'company_category_brand_180.txt')


def computeFeaturesSecondPass():
    '''
    Those features are slower to compute, because all of the transactions are read.

    '''
    
    readOffers()
    readShoppers()
    
    ids = getIds('train') + getIds('test')
    n = len(ids)
    
    total_b = dict(zip(ids, [0] * n))
    total_n = dict(zip(ids, [0] * n))
    total_a = dict(zip(ids, [0] * n))
    total_q = dict(zip(ids, [0] * n))
    total_qm_ = dict(zip(ids, [0] * n))
    total_qm_1 = dict(zip(ids, [0] * n))
    total_qm_CT = dict(zip(ids, [0] * n))
    total_qm_FT = dict(zip(ids, [0] * n))
    total_qm_GL = dict(zip(ids, [0] * n))
    total_qm_LB = dict(zip(ids, [0] * n))
    total_qm_LT = dict(zip(ids, [0] * n))
    total_qm_OZ = dict(zip(ids, [0] * n))
    total_qm_PT = dict(zip(ids, [0] * n))
    total_qm_QT = dict(zip(ids, [0] * n))
    total_qm_RL = dict(zip(ids, [0] * n))
    total_qm_YD = dict(zip(ids, [0] * n))
    total_30 = dict(zip(ids, [0] * n))
    total_60 = dict(zip(ids, [0] * n))
    total_180 = dict(zip(ids, [0] * n))

    fid = gzip.GzipFile('transactions.csv.gz', 'rU')
    transactions = csv.reader(fid)
    header = transactions.next()

    IDIndex = header.index('id')
    chainIndex = header.index('chain')
    deptIndex = header.index('dept')
    categoryIndex = header.index('category')
    companyIndex = header.index('company')
    brandIndex = header.index('brand')
    dateIndex = header.index('date')
    productsizeIndex = header.index('productsize')
    measureIndex = header.index('productmeasure')
    quantityIndex = header.index('purchasequantity')
    amountIndex = header.index('purchaseamount')
    
    steps = 0
    for row in transactions:

        ID = row[IDIndex]
        chain = row[chainIndex]
        dept = row[deptIndex]
        category = row[categoryIndex]
        company = row[companyIndex]
        brand = row[brandIndex]
        date = row[dateIndex]
        productsize = row[productsizeIndex]
        measure = row[measureIndex]
        quantity = float(row[quantityIndex])
        amount = float(row[amountIndex])

        dt = time_between_dates(date, date_of_shopper[ID])

        total_b[ID] = 1
        total_n[ID] += 1
        total_a[ID] += amount
        total_q[ID] += quantity
        if measure == '':
            total_qm_[ID] += quantity
        if measure == '1':
            total_qm_1[ID] += quantity
        if measure == 'CT':
            total_qm_CT[ID] += quantity
        if measure == 'FT':
            total_qm_FT[ID] += quantity
        if measure == 'GL':
            total_qm_GL[ID] += quantity
        if measure == 'LB':
            total_qm_LB[ID] += quantity
        if measure == 'LT':
            total_qm_LT[ID] += quantity
        if measure == 'OZ':
            total_qm_OZ[ID] += quantity
        if measure == 'PT':
            total_qm_PT[ID] += quantity
        if measure == 'QT':
            total_qm_QT[ID] += quantity
        if measure == 'RL':
            total_qm_RL[ID] += quantity
        if measure == 'YD':
            total_qm_YD[ID] += quantity
        if dt <= 30:
            total_30[ID] += 1
        if dt <= 60:
            total_60[ID] += 1
        if dt <= 180:
            total_180[ID] += 1

        N = 1000000
        if steps % N == 0:
            print >>sys.stderr, steps / N,
        steps += 1


    print >>sys.stderr
    fid.close()

    # Save the results in text files.
    saveIt(total_b, 'total_b.txt')
    saveIt(total_n, 'total_n.txt')
    saveIt(total_a, 'total_a.txt')
    saveIt(total_q, 'total_q.txt')
    saveIt(total_qm_, 'total_qm_.txt')
    saveIt(total_qm_1, 'total_qm_1.txt')
    saveIt(total_qm_CT, 'total_qm_CT.txt')
    saveIt(total_qm_FT, 'total_qm_FT.txt')
    saveIt(total_qm_GL, 'total_qm_GL.txt')
    saveIt(total_qm_LB, 'total_qm_LB.txt')
    saveIt(total_qm_LT, 'total_qm_LT.txt')
    saveIt(total_qm_OZ, 'total_qm_OZ.txt')
    saveIt(total_qm_PT, 'total_qm_PT.txt')
    saveIt(total_qm_QT, 'total_qm_QT.txt')
    saveIt(total_qm_RL, 'total_qm_RL.txt')
    saveIt(total_qm_YD, 'total_qm_YD.txt')
    saveIt(total_30, 'total_30.txt')
    saveIt(total_60, 'total_60.txt')
    saveIt(total_180, 'total_180.txt')



def readTargets():
    target_of_shopper = {}
    fid = open('trainHistory.csv')
    cr = csv.reader(fid)
    header = cr.next()
    IDIndex = header.index('id')
    repeatIndex = header.index('repeater')
    for row in cr:
        ID = row[IDIndex]
        repeat = row[repeatIndex]
        target = int(repeat == 't')
        target_of_shopper[ID] = target
    fid.close()
    return target_of_shopper
    
def createFeatureFiles(ids, features, folder, library, phase):

    outputFolder = 'experiments' + '/' + library
    if not os.path.exists(outputFolder):
        os.makedirs(outputFolder)
    target_of_shopper = readTargets()
    nf = len(features)
    f = [{} for i in range(nf)]
    
    # Load the dictionaries
    for i in range(nf):
        
        featuresFile = features[i] +'.txt'
        f[i] = loadIt(featuresFile, folder = folder)    
    
    # Write the features

   
    n = len(ids)
    lines = []
    for i in range(n):
        target = '0'
        if phase == 'train':
            target = target_of_shopper[ids[i]]
        if library == 'liblinear':
            words = [str(target)]
        if library == 'vowpalwabbit':
            words = [str(target) + ' |']
        for j in range(nf):
            words.append(str(j+1) + ':' + f[j][ids[i]])
        line = ' '.join(words)
        lines.append(line)
    fid = open(outputFolder + '/' + phase + '.txt', 'w')
    fid.write('\n'.join(lines)+'\n')
    fid.close()

def parseLiblinearResults(ids_test, resultsFile, submissionFile):
    ids = ids_test
    n = len(ids)
    predictions = []
    lines = open(resultsFile).readlines()[1:]
#     resultsFile.seek(0)
    header = 'id,repeatProbability'
    t = [header]
    for i in range(n):
        t.append(ids[i] + ',' + lines[i].split()[1])
    fid = gzip.GzipFile(submissionFile, 'w')
    fid.write('\n'.join(t)+'\n')
    fid.close()
    
def parsevowpalwabbitResults(ids_test, resultsFile, submissionFile):
    ids = ids_test
    n = len(ids)
    predictions = []
    lines = open(resultsFile).readlines()
    
    header = 'id,repeatProbability'
    t = [header]
    for i in range(n):
        t.append(ids[i] + ',' + lines[i].strip())
    fid = gzip.GzipFile(submissionFile, 'w')
    fid.write('\n'.join(t)+'\n')
    fid.close()

def processResults(ids, library, results, submissionFile):
    if library == 'liblinear':
        parseLiblinearResults(ids, results, submissionFile)
    if library == 'vowpalwabbit':
        parsevowpalwabbitResults(ids, results, submissionFile)

def runExperiments(ids_train, ids_test, library):
    
    if library == 'liblinear':

        trainFile = 'experiments/%s/train.txt' % library
        modelFile = 'experiments/%s/model.txt' % library
        testFile = 'experiments/%s/test.txt' % library
        resultsFile = 'experiments/%s/out.txt'% library

        c = 'liblinear-train -s 0 -w0 43438 -w1 116619 -B 1 ' + \
            trainFile + ' ' + modelFile + \
            '&& liblinear-predict -b 1 ' + \
            testFile + ' ' + \
            modelFile + ' ' + \
            resultsFile
        subprocess.call(c, shell=True)

    if library == 'vowpalwabbit':
        
        trainFile = 'experiments/%s/train.txt' % library
        modelFile = 'experiments/%s/model.txt' % library
        testFile = 'experiments/%s/test.txt' % library
        resultsFile = 'experiments/%s/out.txt' % library

        c = 'vw ' + trainFile + ' -f ' + modelFile + ' ' + \
            ' && vw -i ' + modelFile + ' --loss_function quantile ' + \
            '-t ' + testFile + ' ' + \
            '-p ' + resultsFile
        print c
        subprocess.call(c, shell=True)
        return resultsFile
        
def create_submission_file(library, folder, submissionFile, createTrainTest = True):
    ids_train = getIds('train')
    ids_test = getIds('test')
    featuresFiles = [p for p in glob.glob(folder + '/*.txt')]
    features = [p.split('/')[-1][:-4] for p in featuresFiles]
    if createTrainTest:
        createFeatureFiles(ids_train, features, folder, library, 'train')
        createFeatureFiles(ids_test, features, folder, library, 'test')
    resultsFile = runExperiments(ids_train, ids_test, library)
    processResults(library, resultsFile, submissionFile)

def normalizeFeatures(inputFolder, outputFolder):
    if not os.path.exists(outputFolder):
        os.makedirs(outputFolder)
    paths = glob.glob(inputFolder + '/*.txt')
    features = [ p.split('/')[-1][:-4] for p in paths]
    for f in features:
        lines = open(inputFolder + '/' + f + '.txt').readlines()
        minValue, maxValue = None, None
        d = {}
        for line in lines:
            words = line.split()
            k = words[0]
            v = float(words[1])
            d[k] = v
            if minValue == None:
                minValue = v
                maxValue = v
            if v < minValue:
                minValue = v
            if v > maxValue:
                maxValue = v
        toWrite = []
        for k in d:
            normalizedValue = (d[k] - minValue) / (maxValue - minValue)
            toWrite.append(k + ' ' + str(normalizedValue))
        fout = open(outputFolder + '/' + f + '.txt', 'w')
        fout.write('\n'.join(toWrite)+'\n')
        fout.close()
        
def cross_validation(ids_train, ids_test, library, folder, createTrainTest = True):
    featuresFiles = [p for p in glob.glob(folder + '/*.txt')]
    features = [p.split('/')[-1][:-4] for p in featuresFiles]
    if createTrainTest:
        createFeatureFiles(ids_train, features, folder, library, 'train')
        createFeatureFiles(ids_test, features, folder, library, 'test')
    resultsFile = runExperiments(ids_train, ids_test, library)
    processResults(ids_test, library, resultsFile, 'cross_validation' + '/' + library + '/results.csv.gz')
    true_values = []
    probabilities = []
    targets = readTargets()
    process_results_file = gzip.GzipFile('cross_validation' + '/' + library + '/results.csv.gz', 'r')
    process_results = csv.reader(process_results_file)
    header_process_results = process_results.next()
    for row in process_results:
        value = targets[row[0]]
        true_values.append(int(value))
        probabilities.append(float(row[1]))
    true_values_array = numpy.asarray(true_values)
    probabilities_array = numpy.asarray(probabilities)
    
    return roc_auc_score(true_values_array, probabilities_array)
        
def testCrossValidation():
    ids_train = []
    ids_test = []
    train_history_file = file('trainHistory.csv', 'rU')
    train_history = csv.reader(train_history_file)
    header_train_history = train_history.next()
    rownum = 0
    for row in train_history:
        if rownum < 80030:
            ids_train.append(row[0])
        else: ids_test.append(row[0])
        rownum = rownum + 1
    print cross_validation(ids_train, ids_test, 'vowpalwabbit', 'features', createTrainTest = True)

        

if __name__ == '__main__':
    
    # Uncomment to re-compute the features.
#     computeTransactionsSubset()
     computeFeaturesFirstPass()
     computeFeaturesSecondPass()
#    testCrossValidation()
    

    # normalizeFeatures('features', 'normalizedFeatures')    
    # runExperiments('liblinear','normalizedFeatures', 'submissions/sub-liblinear-normalized.csv.gz', createTrainTest = False)
    
    # runExperiments('vowpalwabbit', 'features', 'submissions/sub-vw.csv.gz', createTrainTest = True)



