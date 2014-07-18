
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from os import listdir
from os.path import isfile, join
from ShoppersChallenge import getIds, readTargets

#pd.set_option('display.max_columns', None)

#get the files from the 'features' directory
FeaturesPath = 'features/'
FeatureFiles = [ f for f in listdir(FeaturesPath) if isfile(join(FeaturesPath,f)) ]

#load the files inside a dataframe
features = pd.DataFrame()
for FileName in FeatureFiles:
    feature = pd.read_csv('features/' + FileName, delimiter=' ', header=None).ix[:,1]
    if feature.std()==0:
        #print FileName[:-4] + ' is constant, ignoring...'
        pass
    else:
        features[FileName[:-4]] = feature

#set the labels as customer ids        
features.set_index(np.array(pd.read_csv('features/market_imp.txt', delimiter=' ', header=None).ix[:,0]).astype(str),inplace=True)

#features.values.nbytes + features.index.nbytes + features.columns.nbytes
#features.sort_index(inplace=True)

#subset train and test
trainIds = getIds('train')
testIds = getIds('test')
train = features.loc[trainIds]
test = features.loc[testIds]

#get target
target = readTargets()


# In[3]:

from sklearn.preprocessing import MinMaxScaler

train_unscaled=train
test_unscaled=test

scaler = MinMaxScaler()
scaler.fit(train.append(test))

feature_names = train_unscaled.columns.values

train = scaler.transform(train)
test = scaler.transform(test)

train = pd.DataFrame(train, columns=feature_names, index=trainIds)
test = pd.DataFrame(test, columns=feature_names, index=testIds)


# In[4]:

#stupid validation
from ShoppersChallenge import getTrainingSubsetIds
rows_tr = getTrainingSubsetIds('2013-03-01', '2013-04-01')
rows_val = set(target.keys()) - set(rows_tr)
train_tr = train.ix[rows_tr]
train_val = train.ix[rows_val]
target_tr = [target[k] for k in rows_tr]
target_val = [target[k] for k in rows_val]

from sklearn.ensemble import RandomForestRegressor
classifier = RandomForestRegressor(n_estimators=10)
clf = classifier.fit(train_tr, target_tr)
pred_tr = clf.predict(train_tr)
pred_val = clf.predict(train_val)

from sklearn.metrics import roc_auc_score
print 'ROC train score: ',roc_auc_score(target_tr,pred_tr)
print 'ROC test score: ',roc_auc_score(target_val,pred_val)

import pylab as pl
###############################################################################
# Plot feature importance
feature_importance = clf.feature_importances_
# make importances relative to max importance
feature_importance = 100.0 * (feature_importance / feature_importance.max())
sorted_idx = np.argsort(feature_importance)
pos = np.arange(sorted_idx.shape[0]) + .5
pl.subplot(1, 2, 2)
pl.barh(pos, feature_importance[sorted_idx], align='center')
pl.yticks(pos, train.columns.values[sorted_idx])
pl.xlabel('Relative Importance')
pl.title('Variable Importance')
pl.show()


# In[44]:

#This snippet uses SeleckKBest to find a good subset for training
'''
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.metrics import roc_auc_score
#for k in range(10):#range(len(feature_names)):
k=10
ch2 = SelectKBest(chi2,k)
sub_tr = ch2.fit_transform(train_tr, target_tr)
sub_val = ch2.transform(train_val)
print feature_names[ch2.get_support()]

clf = classifier.fit(sub_tr, target_tr)
pred_tr = clf.predict(sub_tr)
pred_val = clf.predict(sub_val)

print 'ROC train score: ',roc_auc_score(target_tr,pred_tr)
print 'ROC test score: ',roc_auc_score(target_val,pred_val)
'''


# In[5]:

#Create submission
from sklearn.ensemble import RandomForestRegressor
classifier = RandomForestRegressor(n_estimators=10)

model = classifier.fit(train, target.values())
#model = classifier.fit(train[feature_names[ch2.get_support()]], target.values())

pred = model.predict(test)
#pred = model.predict(test[feature_names[ch2.get_support()]])

submission = pd.DataFrame({'id': pd.Series(test.index.values), 'repeatProbability': pd.Series(pred)})
submission.to_csv('sklearn2.csv', sep=',', index=False)

print 'percantage of returning customers in train set:',float(sum(target.values()))/len(target.values())
print 'percantage of returning customers in test set:',float(sum(pred))/len(pred)

