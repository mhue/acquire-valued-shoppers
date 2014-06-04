'''
Created on Jun 4, 2014

@author: philipp
'''
import sys
from time import strftime
from datetime import datetime
from time import mktime
from collections import defaultdict
import csv
import glob
import subprocess

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

def computeFeaturesFirstPass():

    readOffers()
    readShoppers()
    
    ids = getIds('train') + getIds('test')
    has_bought_company = dict(zip(ids, [0] * len(ids)))
    has_bought_company_a = dict(zip(ids, [0] * len(ids)))
    has_bought_company_q = dict(zip(ids, [0] * len(ids)))
    has_bought_company_30 = dict(zip(ids, [0] * len(ids)))
    has_bought_company_60 = dict(zip(ids, [0] * len(ids)))
    has_bought_company_180 = dict(zip(ids, [0] * len(ids)))
    has_bought_category = dict(zip(ids, [0] * len(ids)))
    has_bought_category_a = dict(zip(ids, [0] * len(ids)))
    has_bought_category_q = dict(zip(ids, [0] * len(ids)))
    has_bought_category_30 = dict(zip(ids, [0] * len(ids)))
    has_bought_category_60 = dict(zip(ids, [0] * len(ids)))
    has_bought_category_180 = dict(zip(ids, [0] * len(ids)))
    has_bought_brand = dict(zip(ids, [0] * len(ids)))
    has_bought_brand_a = dict(zip(ids, [0] * len(ids)))
    has_bought_brand_q = dict(zip(ids, [0] * len(ids)))
    has_bought_brand_30 = dict(zip(ids, [0] * len(ids)))
    has_bought_brand_60 = dict(zip(ids, [0] * len(ids)))
    has_bought_brand_180 = dict(zip(ids, [0] * len(ids)))
    total_shopper_spend = dict(zip(ids, [0] * len(ids)))

    transactionsfile = file('transactions.csv', 'rU')
    transactions = csv.reader(transactionsfile)
    header = transactions.next()

    IDIndex = header.index('id')
    categoryIndex = header.index('category')
    companyIndex = header.index('company')
    brandIndex = header.index('brand')
    dateIndex = header.index('date')
    quantityIndex = header.index('purchasequantity')
    
    steps = 0
    for row in transactions:
        ID = row[IDIndex]
        company = row[companyIndex]
        category = row[categoryIndex]
        brand = row[brandIndex]
        quantity = row[quantityIndex]
        dt = time_between_dates(row[6], date_of_shopper[ID])
        
        if company_of_shopper[ID] == company:
            has_bought_company[ID] += 1
        if company_of_shopper[ID] == company:
            has_bought_company_a[ID] += float(row[10])
        if company_of_shopper[ID] == company:
            has_bought_company_q[ID] += float(row[9])
        if company_of_shopper[ID] == company and dt <= 30:
            has_bought_company_30[ID] += 1
        if company_of_shopper[ID] == company and dt <= 60:
            has_bought_company_60[ID] += 1
        if company_of_shopper[ID] == company and dt <= 180:
            has_bought_company_180[ID] += 1
        if category_of_shopper[ID] == row[3]:
            has_bought_category[ID] += 1
        if category_of_shopper[ID] == row[3]:
            has_bought_category_a[ID] += float(row[10])
        if category_of_shopper[ID] == row[3]:
            has_bought_category_q[ID] += float(row[9])
        if category_of_shopper[ID] == row[3] and dt <= 30:
            has_bought_category_30[ID] += 1
        if category_of_shopper[ID] == row[3] and dt <= 60:
            has_bought_category_60[ID] += 1
        if category_of_shopper[ID] == row[3] and dt <= 180:
            has_bought_category_180[ID] += 1
        if brand_of_shopper[ID] == row[5]:
            has_bought_brand[ID] += 1
        if brand_of_shopper[ID] == row[5]:
            has_bought_brand_a[ID] += float(row[10])
        if brand_of_shopper[ID] == row[5]:
            has_bought_brand_q[ID] += float(row[9])
        if brand_of_shopper[ID] == row[5] and time_between_dates(row[6], date_of_shopper[ID]) <= 30:
            has_bought_brand_30[ID] += 1
        if brand_of_shopper[ID] == row[5] and time_between_dates(row[6], date_of_shopper[ID]) <= 60:
            has_bought_brand_60[ID] += 1
        if brand_of_shopper[ID] == row[5] and time_between_dates(row[6], date_of_shopper[ID]) <= 180:
            has_bought_brand_180[ID] += 1
        total_shopper_spend[ID] += float(row[10])

        N = 1000000
        if steps % N == 0:
            print >>sys.stderr, steps / N,
        steps += 1
        
        # DEBUG
        #~ if steps == 100000:
            #~ break
        
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
    
    has_bought_company = loadIt('has_bought_company.txt', convert = True)
    has_bought_category = loadIt('has_bought_category.txt', convert = True)
    has_bought_brand = loadIt('has_bought_brand.txt', convert = True)

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
    
def createTrainTestFiles(features, folder, library):
    target_of_shopper = readTargets()
    nf = len(features)

    f = [{} for i in range(nf)]
    # Load the dictionaries
    for i in range(nf):
        featuresFile = features[i] +'.txt'
        f[i] = loadIt(featuresFile, folder = folder)    
    # Write the features
    for phase in ['train', 'test']:

        ids = getIds(phase)
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
        fid = open('experiments/' + phase + '.txt', 'w')
        fid.write('\n'.join(lines)+'\n')
        fid.close()

def parseLiblinearResults(outFile):
    ids = getIds('test')
    n = len(ids)
    predictions = []
    lines = open(outFile).readlines()[1:]
    header = 'id,repeatProbability'
    t = [header]
    for i in range(n):
        t.append(ids[i] + ',' + lines[i].split()[1])
    fid = open('submissions/sub.csv', 'w')
    fid.write('\n'.join(t)+'\n')
    fid.close()

def runExperiments():
    folder = 'features'
    featuresFiles = [p for p in glob.glob(folder + '/*.txt')]
    features = [p.split('/')[-1][:-4] for p in featuresFiles]
    createTrainTestFiles(features, folder)
    tr = 'experiments/train.txt'
    te = 'experiments/test.txt'
    c = '~/liblinear-train -s 0 -w0 43438 -w1 116619 -B 1 experiments/train.txt ' + \
        '&& ~/liblinear-predict -b 1 experiments/test.txt ' + \
        'train.txt.model experiments/out.txt'
    subprocess.call(c, shell=True)

    parseLiblinearResults('experiments/out.txt')

if __name__ == '__main__':

    # Uncomment to re-compute the features.
    # computeFeaturesFirstPass()
    #~ computeFeaturesSecondPass()
    #~ computeFeaturesThirdPass()
    
    runExperiments()
