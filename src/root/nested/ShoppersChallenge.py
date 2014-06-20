"""
Created on Jun 5, 2014

@author: philipp, martial
"""
import os
import sys
import csv
import glob
import subprocess
import gzip

from sklearn.metrics import roc_auc_score
import numpy


class Error(Exception):
    pass


class ConstantFeatureError(Error):
    pass


company_of_offer = {}
category_of_offer = {}
brand_of_offer = {}
offer_value_of_offer = {}
offer_quantity_of_offer = {}


def readOffers():
    offersFile = file('offers.csv', 'rU')
    offers = csv.reader(offersFile)
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
            offer_quantity_of_shopper[ID] = float(
                offer_quantity_of_offer[offer])
            date_of_shopper[ID] = date
        fid.close()


def total_days_in_date(date):
    month_length = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31,
                    9: 30, 10: 31, 11: 30, 12: 31}
    d = list(date)
    list_year = [d[0], d[1], d[2], d[3]]
    year = int(''.join(list_year))
    list_month = [d[5], d[6]]
    month = int(''.join(list_month))
    list_day = [d[8], d[9]]
    day = int(''.join(list_day))
    total_year_days = ((year - 1) / 4 + 1) + year * 365
    total_month_days = 0
    current_month = 1
    while current_month < month:
        if current_month != 2 or year % 4 != 0:
            total_month_days = total_month_days + month_length[current_month]
        else:
            total_month_days = total_month_days + month_length[
                current_month] + 1
        current_month += 1
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


def loadIt(feature, valueType=None, folder='features'):
    fileid = open(folder + '/' + feature + '.txt')
    d = {}
    for row in fileid:
        words = row.split()
        key = words[0]
        if valueType == 'int':
            value = int(words[1])
        elif valueType == 'float':
            value = float(words[1])
        else:
            value = words[1]
        d[key] = value
    return d


def getIds(phase):
    fid = open(phase + 'History.csv', 'r')
    cr = csv.reader(fid)
    cr.next()
    ids = [row[0] for row in cr]
    fid.close()
    return ids


def getTrainingSubsetIds(startDate='2013-03-01', endDate='2013-04-01'):
    """ Return a list of shoppers IDs with an offer in [startDate, endDate). """
    ids = []
    fid = open('trainHistory.csv')

    cr = csv.reader(fid)
    header = cr.next()
    i = header.index('offerdate')
    for row in cr:
        ID = row[0]
        date = row[i]
        if startDate <= date < endDate:
            ids.append(ID)
    fid.close()
    return ids


def computeTransactionsSubset():
    """
    Computes a subset of the transactions.
    Only keep the rows with a known company, category or offer.
    """

    if os.path.exists('transactions_subset.csv.gz'):
        return

    readOffers()

    companies = dict(
        zip(company_of_offer.values(), [0] * len(company_of_offer)))
    categories = dict(
        zip(category_of_offer.values(), [0] * len(category_of_offer)))
    brands = dict(zip(brand_of_offer.values(), [0] * len(brand_of_offer)))

    fin = gzip.GzipFile('transactions.csv.gz')
    fout = gzip.GzipFile('transactions_subset.csv.gz', 'w')
    cr = csv.reader(fin)
    header = cr.next()
    fout.write(','.join(header) + '\n')

    categoryIndex = header.index('category')
    companyIndex = header.index('company')
    brandIndex = header.index('brand')

    steps = 0
    N = 1000000
    for row in cr:
        company = row[companyIndex]
        category = row[categoryIndex]
        brand = row[brandIndex]
        if company in companies or category in categories or brand in brands:
            fout.write(','.join(row) + '\n')
        if steps % N == 0:
            print >> sys.stderr, steps / N,
        steps += 1
    print >> sys.stderr
    fin.close()
    fout.close()


def computeFeaturesFirstPass():
    """
    Create the first features.
    Those features are quick to compute, because only 10 % of the transactions
    are needed.
    """

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
    categoryIndex = header.index('category')
    companyIndex = header.index('company')
    brandIndex = header.index('brand')
    dateIndex = header.index('date')
    measureIndex = header.index('productmeasure')
    quantityIndex = header.index('purchasequantity')
    amountIndex = header.index('purchaseamount')

    steps = 0
    for row in transactions:

        ID = row[IDIndex]
        category = row[categoryIndex]
        company = row[companyIndex]
        brand = row[brandIndex]
        date = row[dateIndex]
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
        if company_of_shopper[ID] == company and \
                category_of_shopper[ID] == category:
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
        if category_of_shopper[ID] == category and \
                brand_of_shopper[ID] == brand:
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
        if company_of_shopper[ID] == company and \
                category_of_shopper[ID] == category and \
                brand_of_shopper[ID] == brand:
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
            print >> sys.stderr, steps / N,
        steps += 1

    fid.close()
    print >> sys.stderr

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
    """
    Those features are slower to compute, because all of the transactions are
    read.

    """

    readOffers()
    readShoppers()

    ids = getIds('train') + getIds('test')
    n = len(ids)

    total_b = dict(zip(ids, [0] * n))
    total_n = dict(zip(ids, [0] * n))
    total_d = dict(zip(ids, [0] * n))
    days = dict(zip(ids, [set()] * n))
    days_30 = dict(zip(ids, [set()] * n))
    days_60 = dict(zip(ids, [set()] * n))
    days_180 = dict(zip(ids, [set()] * n))
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
    total_d_30 = dict(zip(ids, [0] * n))
    total_d_60 = dict(zip(ids, [0] * n))
    total_d_180 = dict(zip(ids, [0] * n))
    total_a_30 = dict(zip(ids, [0] * n))
    total_a_60 = dict(zip(ids, [0] * n))
    total_a_180 = dict(zip(ids, [0] * n))
    total_q_30 = dict(zip(ids, [0] * n))
    total_q_60 = dict(zip(ids, [0] * n))
    total_q_180 = dict(zip(ids, [0] * n))

    fid = gzip.GzipFile('transactions.csv.gz', 'rU')
    transactions = csv.reader(fid)
    header = transactions.next()

    IDIndex = header.index('id')
    dateIndex = header.index('date')
    measureIndex = header.index('productmeasure')
    quantityIndex = header.index('purchasequantity')
    amountIndex = header.index('purchaseamount')

    steps = 0
    for row in transactions:

        ID = row[IDIndex]
        date = row[dateIndex]
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
            total_a_30[ID] += amount
            total_q_30[ID] += quantity
            if date not in days_30[ID]:
                total_d_30[ID] += 1
                days_30[ID].add(date)
        if dt <= 60:
            total_60[ID] += 1
            total_a_60[ID] += amount
            total_q_60[ID] += quantity
            if date not in days_60[ID]:
                total_d_60[ID] += 1
                days_60[ID].add(date)
        if dt <= 180:
            total_180[ID] += 1
            total_a_180[ID] += amount
            total_q_180[ID] += quantity
            if date not in days_180[ID]:
                total_d_180[ID] += 1
                days_180[ID].add(date)
        if date not in days[ID]:
            total_d[ID] += 1
            days[ID].add(date)

        N = 1000000
        if steps % N == 0:
            print >> sys.stderr, steps / N,
        steps += 1

    print >> sys.stderr
    fid.close()

    #     Save the results in text files.
    saveIt(total_b, 'total_b.txt')
    saveIt(total_a, 'total_a.txt')
    saveIt(total_a_30, 'total_a_30.txt')
    saveIt(total_a_60, 'total_a_60.txt')
    saveIt(total_a_180, 'total_a_180.txt')
    saveIt(total_n, 'total_n.txt')
    saveIt(total_d, 'total_d.txt')
    saveIt(total_d_30, 'total_d_30.txt')
    saveIt(total_d_60, 'total_d_60.txt')
    saveIt(total_d_180, 'total_d_180.txt')
    saveIt(total_q, 'total_q.txt')
    saveIt(total_q_30, 'total_q_30.txt')
    saveIt(total_q_60, 'total_q_60.txt')
    saveIt(total_q_180, 'total_q_180.txt')
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


def computeFeaturesThirdPass():
    readOffers()
    readShoppers()

    ids = getIds('train') + getIds('test')
    n = len(ids)

    offer_value = dict(zip(ids, [0] * n))
    offer_quantity = dict(zip(ids, [0] * n))
    total_30 = dict(zip(ids, [0] * n))
    total_60 = dict(zip(ids, [0] * n))
    total_180 = dict(zip(ids, [0] * n))
    average_transaction_a = dict(zip(ids, [0] * n))
    average_transaction_a_30 = dict(zip(ids, [0] * n))
    average_transaction_a_60 = dict(zip(ids, [0] * n))
    average_transaction_a_180 = dict(zip(ids, [0] * n))
    average_transaction_q = dict(zip(ids, [0] * n))
    average_transaction_q_30 = dict(zip(ids, [0] * n))
    average_transaction_q_60 = dict(zip(ids, [0] * n))
    average_transaction_q_180 = dict(zip(ids, [0] * n))
    average_day_a = dict(zip(ids, [0] * n))
    average_day_a_30 = dict(zip(ids, [0] * n))
    average_day_a_60 = dict(zip(ids, [0] * n))
    average_day_a_180 = dict(zip(ids, [0] * n))
    average_day_q = dict(zip(ids, [0] * n))
    average_day_q_30 = dict(zip(ids, [0] * n))
    average_day_q_60 = dict(zip(ids, [0] * n))
    average_day_q_180 = dict(zip(ids, [0] * n))

    for shopper in ids:
        offer_value[shopper] = offer_value_of_shopper[shopper]
        offer_quantity[shopper] = offer_quantity_of_shopper[shopper]

    total_a = loadIt('total_a', valueType='float')
    total_a_30 = loadIt('total_a_30', valueType='float')
    total_a_60 = loadIt('total_a_60', valueType='float')
    total_a_180 = loadIt('total_a_180', valueType='float')
    total_n = loadIt('total_n', valueType='float')
    total_q = loadIt('total_q', valueType='float')
    total_q_30 = loadIt('total_q_30', valueType='float')
    total_q_60 = loadIt('total_q_60', valueType='float')
    total_q_180 = loadIt('total_q_180', valueType='float')
    total_d = loadIt('total_d', valueType='float')
    total_d_30 = loadIt('total_d_30', valueType='float')
    total_d_60 = loadIt('total_d_60', valueType='float')
    total_d_180 = loadIt('total_d_180', valueType='float')

    for shopper in ids:
        if total_n[shopper] == 0:
            average_transaction_a[shopper] = 0
        else:
            average_transaction_a[shopper] = total_a[shopper] / total_n[shopper]
        if total_30[shopper] == 0:
            average_transaction_a_30[shopper] = 0
        else:
            average_transaction_a_30[shopper] = total_a_30[shopper] / total_30[
                shopper]
        if total_60[shopper] == 0:
            average_transaction_a_60[shopper] = 0
        else:
            average_transaction_a_60[shopper] = total_a_60[shopper] / total_60[
                shopper]
        if total_180[shopper] == 0:
            average_transaction_a_180[shopper] = 0
        else:
            average_transaction_a_180[shopper] = \
                total_a_180[shopper] / total_180[shopper]
        if total_n[shopper] == 0:
            average_transaction_q[shopper] = 0
        else:
            average_transaction_q[shopper] = total_q[shopper] / total_n[shopper]
        if total_30[shopper] == 0:
            average_transaction_q_30[shopper] = 0
        else:
            average_transaction_q_30[shopper] = total_q_30[shopper] / total_30[
                shopper]
        if total_60[shopper] == 0:
            average_transaction_q_60[shopper] = 0
        else:
            average_transaction_q_60[shopper] = total_q_60[shopper] / total_60[
                shopper]
        if total_180[shopper] == 0:
            average_transaction_q_180[shopper] = 0
        else:
            average_transaction_q_180[shopper] = \
                total_q_180[shopper] / total_180[shopper]
        if total_d[shopper] == 0:
            average_day_a[shopper] = 0
        else:
            average_day_a[shopper] = total_a[shopper] / total_d[shopper]
        if total_d_30[shopper] == 0:
            average_day_a_30[shopper] = 0
        else:
            average_day_a_30[shopper] = \
                total_a_30[shopper] / total_d_30[shopper]
        if total_d_60[shopper] == 0:
            average_day_a_60[shopper] = 0
        else:
            average_day_a_60[shopper] = total_a_60[shopper] / total_d_60[
                shopper]
        if total_d_180[shopper] == 0:
            average_day_a_180[shopper] = 0
        else:
            average_day_a_180[shopper] = total_a_180[shopper] / total_d_180[
                shopper]
        if total_d[shopper] == 0:
            average_day_q[shopper] = 0
        else:
            average_day_q[shopper] = total_q[shopper] / total_d[shopper]
        if total_d_30[shopper] == 0:
            average_day_q_30[shopper] = 0
        else:
            average_day_q_30[shopper] = total_q_30[shopper] / total_d_30[
                shopper]
        if total_d_60[shopper] == 0:
            average_day_q_60[shopper] = 0
        else:
            average_day_q_60[shopper] = total_q_60[shopper] / total_d_60[
                shopper]
        if total_d_180[shopper] == 0:
            average_day_q_180[shopper] = 0
        else:
            average_day_q_180[shopper] = total_q_180[shopper] / total_d_180[
                shopper]

    saveIt(offer_value, 'offer_value.txt')
    saveIt(offer_quantity, 'offer_quantity.txt')
    saveIt(average_transaction_a, 'average_transaction_a.txt')
    saveIt(average_transaction_a_30, 'average_transaction_a_30.txt')
    saveIt(average_transaction_a_60, 'average_transaction_a_60.txt')
    saveIt(average_transaction_a_180, 'average_transaction_a_180.txt')
    saveIt(average_transaction_q, 'average_transaction_q.txt')
    saveIt(average_transaction_q_30, 'average_transaction_q_30.txt')
    saveIt(average_transaction_q_60, 'average_transaction_q_60.txt')
    saveIt(average_transaction_q_180, 'average_transaction_q_180.txt')
    saveIt(average_day_a, 'average_day_a.txt')
    saveIt(average_day_a_30, 'average_day_a_30.txt')
    saveIt(average_day_a_60, 'average_day_a_60.txt')
    saveIt(average_day_a_180, 'average_day_a_180.txt')
    saveIt(average_day_q, 'average_day_q.txt')
    saveIt(average_day_q_30, 'average_day_q_30.txt')
    saveIt(average_day_q_60, 'average_day_q_60.txt')
    saveIt(average_day_q_180, 'average_day_q_180.txt')


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


def createFeatureFiles(ids, features, libraryFormat, outFile, normalize=False):
    """
    Creates a file corresponding to ids and features. 
    
    Args:
        ids: a list of strings
        features: a list of strings, the names of the features to use.
        libraryFormat: 'liblinear' or 'vw'
        outFile: a string.
        
    The function creates intermediate files, one per feature, to fit in RAM.
    """
    target_of_shopper = readTargets()
    n = len(ids)
    nf = len(features)

    # Normalize ~features only for liblinear.
    assert (libraryFormat == 'liblinear' and normalize) or \
        (libraryFormat != 'liblinear' and not normalize)

    # Prepare for each feature.
    tmp_dir = outFile + '_temporary_files'
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    for i in range(nf):

        print i, 'reading', features[i]
        f = loadIt(features[i], valueType='float')

        # Check whether the feature is constant.
        value = f.itervalues().next()
        minValue, maxValue = value, value
        for ID in f:
            if f[ID] < minValue:
                minValue = f[ID]
            if f[ID] > maxValue:
                maxValue = f[ID]

        if minValue == maxValue:
            message = 'Feature %s is constant; aborting' % features[i]
            raise ConstantFeatureError(message)

        if normalize:
            toWrite = [
                str((f[ID] - minValue) / (maxValue - minValue)) for ID in ids]
        else:
            toWrite = [str(f[ID]) for ID in ids]
        out = open('%s/%s.txt' % (tmp_dir, features[i]), 'w')
        out.write('\n'.join(toWrite) + '\n')
        out.close()

    # Write the features
    fileIDs = {}
    for i in range(nf):
        feature = features[i]
        fileIDs[feature] = open('%s/%s.txt' % (tmp_dir, feature))

    lines = []
    for i in range(n):
        target = target_of_shopper.get(ids[i], 0)
        if libraryFormat == 'liblinear':
            words = [str(target)]
        if libraryFormat == 'vw':
            words = ['%f 1.0 %s|' % (target, ids[i])]
        for j in range(nf):
            value = fileIDs[features[j]].readline().strip()
            if value == '0':
                continue
            else:
                words.append('%d:%s' % (j + 1, value))
        line = ' '.join(words)
        lines.append(line)
    fid = open(outFile, 'w')
    fid.write('\n'.join(lines) + '\n')
    fid.close()

    for feature in fileIDs:
        fileIDs[feature].close()


def parseLiblinearResults(ids, resultsFile, predictionsFile):
    """
    Converts Liblinear results into a CSV format.
    Args:
        ids: a list of strings; the shoppers ids in the results.
        resultsFile: a string; the file where Liblinear results were stored.
        predictionsFile: a string: a CSV file with "id,probability" columns.
    """

    n = len(ids)
    lines = open(resultsFile).readlines()[1:]
    header = 'id,repeatProbability'
    t = [header]
    for i in range(n):
        t.append(ids[i] + ',' + lines[i].split()[1])
    fid = gzip.GzipFile(predictionsFile, 'w')
    fid.write('\n'.join(t) + '\n')
    fid.close()


def parseVowpalWabbitResults(resultsFile, predictionsFile):
    """
    Converts Vowpal-Wabbit results into a CSV format.
    Args:
        ids: a list of strings; the shoppers ids in the results.
        resultsFile: a string; the file where Liblinear results were stored.
        predictionsFile: a string: a CSV file with "id,probability" columns.
    """
    lines = open(resultsFile).readlines()
    header = 'id,repeatProbability'
    t = [header]
    for line in lines:
        probability, ID = line.strip().split()
        t.append('%s,%s' % (ID, probability))
    fid = gzip.GzipFile(predictionsFile, 'w')
    fid.write('\n'.join(t) + '\n')
    fid.close()


def computePredictions(library, parameters, trainFile, testFile,
                       predictionsFile, test_ids=None):
    """
    Training and testing phase.

    Args:
        library: 'liblinear' or 'vw'
        parameters: a dictionary with two keys, 'train' and 'predict', and
            with values the list of string parameters used when calling the
            library.
        trainFile: string; the name of the training file.
        testFile: string; the name of the test file.
            predictionsFile: string, the name of the output file.

        test_ids: a list of string's; needed only for
            'liblinear'.
    """

    modelFile = trainFile + '.model'
    resultsFile = testFile + '.results'
    if library == 'liblinear':
        c = ['liblinear-train'] + parameters['train'] + [trainFile, modelFile]
        c2 = ['liblinear-predict'] + parameters['predict'] + [
            testFile,
            modelFile,
            resultsFile]
        subprocess.call(c)
        subprocess.call(c2)
        parseLiblinearResults(test_ids, resultsFile, predictionsFile)

    if library == 'vw':
        c = ['vw', trainFile, '-f', modelFile]
        c2 = ['vw', '-i', modelFile,
              '-t', testFile, '-p', resultsFile] + parameters['predict']
        subprocess.call(c)
        subprocess.call(c2)
        parseVowpalWabbitResults(resultsFile, predictionsFile)


def getListOfAllFeatures(folder='features', force=False):
    """ Return the list of available features.

        The constant features are discarded.
        featuresList.txt is used as a cache.
        Args:
            folder: the folder containing the feature files.
            force: if True, ignore the cache.
    """
    cache = 'featuresList.txt'
    doIt = force or not os.path.exists(cache)
    if doIt:
        featuresFiles = [p for p in glob.glob(folder + '/*.txt')]
        allFeatures = [p.split('/')[-1][:-4] for p in featuresFiles]
        features = []
        nFeatures = len(allFeatures)
        for i in range(nFeatures):
            f = loadIt(allFeatures[i])
            if len(set(f.values())) > 1:
                features.append(allFeatures[i])
            else:
                print allFeatures[i], 'is constant.'
        fid = open(cache, 'w')
        fid.write('\n'.join(features))
        fid.close()
    else:
        features = open(cache).read().strip().split()
    return features


def getListFeatures(listFile, folder='features'):
    """ Return the list of available features."""
    features = []
    fid = open(listFile)
    allFeaturesFound = True
    for line in fid:
        f = line.strip()
        if f == '' or f.startswith('#'):
            continue
        featureFile = '%s/%s.txt' % (folder, f)
        if not os.path.exists(featureFile):
            print >> sys.stderr, 'could not find %s' % featureFile
        allFeaturesFound &= os.path.exists(featureFile)
        features.append(line)
    fid.close()
    if allFeaturesFound:
        return features
    return []


def checkFeatures(features):
    """
    Check that all features correspond to an existing file.
    """
    allFeaturesFound = True
    for f in features:
        featureFile = 'features/%s.txt' % f
        if not os.path.exists(featureFile):
            print >> sys.stderr, 'could not find %s' % featureFile
        allFeaturesFound &= os.path.exists(featureFile)
    return allFeaturesFound


def computeAUCScores(predictionsFile):
    """ Returns the AUC score."""
    true_values = []
    probabilities = []
    targets = readTargets()
    process_results_file = gzip.GzipFile(predictionsFile, 'r')
    process_results = csv.reader(process_results_file)
    process_results.next()  # Skip the header.
    for row in process_results:
        value = targets[row[0]]
        true_values.append(int(value))
        probabilities.append(float(row[1]))
    true_values_array = numpy.asarray(true_values)
    probabilities_array = numpy.asarray(probabilities)

    return roc_auc_score(true_values_array, probabilities_array)


def runExperiment(experimentName, ids_train, ids_test, features, library,
                  parameters, createTrainTest=None, predictionScores=False):
    """Training, computing predictions, and evaluating or preparing to submit.
    
    Args:
        experimentName: a string, the subfolder of 'experiments' containing
            the necessary files for each step.
        ids_train: list of ids, contained in the ids in trainHistory.csv
        ids_test: list of ids for which we want a prediction.
        features: a list of strings.
        library: 'liblinear' or 'vw'
        parameters: a dictionary, with two keys, 'train' and 'predict', and 
            with values the list of string parameters used when calling the 
            library.
        featuresFolder: a string with the folder containing the feature files.
        createTrainTest: boolean.  If it's True, force to write train.txt 
            and test.txt.
        predictionScores: If it is True, return the AUC score of the
            predictions.  Only use it with ids_test being a subset of the ids
            in trainHistory.csv.  If it's False, returns None
        returns list of float, or None
    """
    folder = 'experiments/' + experimentName
    if not os.path.exists(folder):
        os.makedirs(folder)
    trainFile = folder + '/train.txt'
    testFile = folder + '/test.txt'
    predictionsFile = '%s/predictions-%s.csv.gz' % (folder, experimentName)

    if createTrainTest is None:
        createTrainTest = not (os.path.exists(trainFile) and
                               os.path.exists(testFile))
    if createTrainTest:
        normalize = library == 'liblinear'
        createFeatureFiles(ids_train, features, library, trainFile,
                           normalize=normalize)
        createFeatureFiles(ids_test, features, library, testFile,
                           normalize=normalize)

    computePredictions(library, parameters, trainFile, testFile,
                       predictionsFile, ids_test)

    if predictionScores:
        return computeAUCScores(predictionsFile)


def testCrossValidation():
    train_ids = getTrainingSubsetIds('2013-03-01', '2013-04-07')
    test_ids = getTrainingSubsetIds('2013-04-07', '2013-05-01')
    allFeatures = getListOfAllFeatures()

    scores = []
    if True:
        # Testing with liblinear.
        for nf in [1, 3, 10, 40]:
            features = allFeatures[:nf]
            parameters = {
                'train': [
                    '-s', '0', '-w0', '43438', '-w1', '116619', '-B', '1'],
                'predict': ['-b', '1']}
            score = runExperiment(
                'lib-1', train_ids, test_ids, features, 'liblinear', parameters,
                createTrainTest=True, predictionScores=True)
            print score
            scores.append(score)

    if True:
        # Testing with vowpal-wabbit.
        for nf in [1, 3, 10, 40]:
            features = allFeatures[:nf]
            parameters = {
                'train': [],
                'predict': ['--loss_function', 'quantile']}
            score = runExperiment(
                'vw-1', train_ids, test_ids, features, 'vw', parameters,
                createTrainTest=True, predictionScores=True)
            print score
            scores.append(score)
    print scores


def main():

# Uncomment to re-compute the features.
#     computeTransactionsSubset()
#      computeFeaturesFirstPass()
#     computeFeaturesSecondPass()
#     computeFeaturesThirdPass()

    getListOfAllFeatures()

    testCrossValidation()


if __name__ == '__main__':
    main()
