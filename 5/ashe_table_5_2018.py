#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 2//018 15:29:33

@author: jamesbryant
"""
'''
Takes around 45 mins to run
Have split tabName into sex and working pattern
number of jobs column has been excluded
change input file in geogLabelLookup when codelist is uploaded
'''

import pandas as pd
from databaker.framework import *
from databakerUtils.writers import v4Writer
import glob
from databakerUtils.api import getAllCodes, getAllLabels
import os
import math
import time
import requests


locationTable5_2018 = 'table_5/*'
output_file = 'v4_ashe_table_5_2018.csv'
current_year = '2018'

#all files in location
allFiles5_2018 = glob.glob(locationTable5_2018)


allFiles = allFiles5_2018

#separate data and CV interval data
files = [file for file in allFiles if not file.endswith('CV.xls')]
filesCV = [file for file in allFiles if file.endswith('CV.xls')]

files = sorted(files)
filesCV = sorted(filesCV)

#loading in all tabs for data
allTabs = []
for file in files:
    readFile = loadxlstabs(file)
    allTabs.append(readFile)

#loading in all tabs for CV interval data
allTabsCV = []
for file in filesCV:
    readFile = loadxlstabs(file)
    allTabsCV.append(readFile)

#above process creates a list of lists
#need to flatten the lists    
flatList = [item for subitem in allTabs for item in subitem]
flatListCV = [item for subitem in allTabsCV for item in subitem]

#removing the info tabs from each spreadsheet
tabs = [tab for tab in flatList if tab.name != 'Notes']
tabsCV = [tab for tab in flatListCV if tab.name != 'CV notes']
   
#quick check to make sure number of files or number of tabs hasn't changed
if len(tabs) == len(tabsCV) != len(files)*9:
    raise Exception('Number of files or number of tabs has changed')


maxLength = max(len(tabs[1].excel_ref('A')),len(tabs[0].excel_ref('A')))
batchNumber = 34
numberOfIterations = math.ceil(maxLength/batchNumber)
regions = ['NORTH EAST','NORTH WEST','EAST MIDLANDS','WEST MIDLANDS','YORKSHIRE AND THE HUMBER','EAST','LONDON','SOUTH EAST','SOUTH WEST','ENGLAND','WALES','SCOTLAND','NORTHERN IRELAND','ENGLAND AND WALES','GREAT BRITAIN','UNITED KINGDOM']
region_codes = ['K02000001','K03000001','K04000001','E92000001','E12000001','E12000002','E12000003','E12000004','E12000005','E12000006','E12000007','E12000008','E12000009','W92000004','S92000003','N92000002']


'''databaking data'''
print('Databaking...')
conversionsegments = []


for i in range(0,numberOfIterations):    

    Min = str(6+batchNumber*i)
    Max = str(39+batchNumber*i)
    

    for tab in tabs:
        
        #columns are named badly
        #quick check to make sure they haven't changed
        if tab.excel_ref('C5').value != '(thousand)':
            raise Exception("Column names aren't right")
            
        if tab.excel_ref('S7').value != 'Key':
            raise Exception('Key has moved')
            
        key = tab.excel_ref('S7').expand(RIGHT).expand(DOWN)    #referenced but not used (waffle)
        junk = tab.excel_ref('A').filter(contains_string('a  Employees')).expand(DOWN)
        
        geographyNames = tab.excel_ref('A'+Min+':A'+Max) - junk 
        geographyCodes = tab.excel_ref('B'+Min+':B'+Max)
        
            
        #ignoring the annual percentage change and number of jobs
        columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
        variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore 
        
        tabName = tab.name
        
        sheetName = tab.excel_ref('a1').value.split(' ')[2]
    
        tableNumber = sheetName.split('.')[0]
    
        obs = tab.excel_ref('D'+Min+':D'+Max).expand(RIGHT) - junk - columnsToIgnore - key  #waffle used incase gaps in data
       
        
    
        dimensions = [
                HDimConst(TIME,current_year),
                HDim(geographyCodes,GEOG,DIRECTLY,LEFT),
                HDim(geographyNames,'GeogNames',DIRECTLY,LEFT),
                HDim(variable,'Variable',DIRECTLY,ABOVE),
                HDimConst('tabName',tabName),
                HDimConst('sheetName',sheetName),
                HDimConst('tableNumber',tableNumber)
                ]
        
        
        if len(obs) != 0:
           conversionsegment = ConversionSegment(tab,dimensions,obs).topandas()
           
          
            
        conversionsegments.append(conversionsegment)
       
        if tabName == 'Female Part-Time':
            print('{} is done'.format(sheetName))
            
        
    
data = pd.concat(conversionsegments)

#remove nulls
data = data[data.GeogNames.notnull()]
data = data[data.Variable.notnull()]


data = data.reset_index(drop=True)
data['region'] = ''

data['GeogNames'] = data['GeogNames'].str.strip()
data['GeogNamesOriginal'] = data['GeogNames']
data['region'] = data['region'].str.upper()
data['GeogNames'] = data['GeogNames'].str.replace('All ', ', All ')
data['GeogNames'] = data['GeogNames'].str.replace('ALL ', ', ALL ')      
data['GeogNames'] = data['GeogNames'].str.replace(' , , ', ', ')
data['GeogNames'] = data['GeogNames'].str.replace(', , ', ', ')
data['GeogNames'] = data['GeogNames'].str.replace(',, ', ', ')
data['GeogNames'].loc[data['GeogNames'].isnull()] = 'None'

#split region faster
f = lambda x: x["GeogNames"].split(", ")[0]
data['region'] = data.apply(f, axis=1)


data['region'] = data['region'].str.strip()
data['region'] = data['region'].str.upper()

#change regions that weren't back to UK  
data.loc[~data['region'].isin(regions), 'region'] = 'UNITED KINGDOM'

#remove wrong ONS codes from SIC code
data.loc[data[GEOG].isin(region_codes), GEOG] = ''

#set up industry
data['industry'] = ''

#change GeogNames to upper
data['GeogNames'] = data['GeogNames'].str.upper()

#get industry from GeogNames by splitting against region
g = lambda x: x['GeogNames'].replace(x['region'],'')
data['industry'] = data.apply(g, axis=1)

data['industry'] = data['industry'].str.replace(', ALL','ALL')
data['industry'] = data['industry'].str.replace(',  ','ALL')
  

#remove commas by getting first few rows
data['industry'] = data['industry'].str.strip()
data['first'] = data['industry'].astype(str).str[0]


#remove first comma if applicable
data['industry']= data.apply(lambda x: x['industry'][2:] if x['first'] == ',' else x['industry'],axis=1)

#convert to proper
data['industry'] = data['industry'].str.title() 
data['region'] = data['region'].str.title()

#tidying industries after proper
data['industry'] = data['industry'].str.replace(' And ',' and ')
data['industry'] = data['industry'].str.replace(' Of ',' of ')
data['industry'] = data['industry'].str.replace(' With ',' with ')
data['industry'] = data['industry'].str.replace(' Via ',' via ')
data['region'] = data['region'].str.replace(' And ',' and ')
data['industry'] = data['industry'].str.replace(', All ','ALL')

#change back to all if it's a region
data['industry2'] = data['industry'].str.upper()
data['industry2']= data.apply(lambda x: 'All' if x['industry'] == '' else x['industry'],axis=1)

#backup
data2 = data.copy(deep = True)

#remove unnecessary cols
data = data.drop('first', 1)
data = data.drop('GeogNamesOriginal', 1)
data = data.drop('GeogNames', 1)
data = data.drop('tableNumber', 1)

#make df
df = v4Writer(output_file,data,asFrame=True) 


'''databaking CV interval data'''
print('Databaking the CV intervals...')

conversionsegments = []
  
for i in range(0,numberOfIterations):    

    Min = str(6+batchNumber*i)
    Max = str(39+batchNumber*i)
    
    

    for tab in tabsCV:
        
        #columns are named badly
        #quick check to make sure they haven't changed
        if tab.excel_ref('C5').value != '(thousand)':
            raise Exception("Column names aren't right")
            
        if tab.excel_ref('S7').value != 'Key':
            raise Exception('Key has moved')
            
        key = tab.excel_ref('S7').expand(RIGHT).expand(DOWN)    #referenced but not used (waffle)
        junk = tab.excel_ref('A').filter(contains_string('a  Employees')).expand(DOWN)
        
        geographyNames = tab.excel_ref('A'+Min+':A'+Max) - junk 
        geographyCodes = tab.excel_ref('B'+Min+':B'+Max)
        
            
        #ignoring the annual percentage change and number of jobs
        columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
        variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
        
        tabName = tab.name
        
        sheetName = tab.excel_ref('a1').value.split(' ')[2]
    
        tableNumber = sheetName.split('.')[0]
    
        obs = tab.excel_ref('D'+Min+':D'+Max).expand(RIGHT) - junk - columnsToIgnore - key  #waffle used incase gaps in data
       
        
    
        dimensions = [
                HDimConst(TIME,current_year),
                HDim(geographyCodes,GEOG,DIRECTLY,LEFT),
                HDim(geographyNames,'GeogNames',DIRECTLY,LEFT),
                HDim(variable,'Variable',DIRECTLY,ABOVE),
                HDimConst('tabName',tabName),
                HDimConst('sheetName',sheetName),
                HDimConst('tableNumber',tableNumber)
                ]
        
        
        if len(obs) != 0:
           conversionsegment = ConversionSegment(tab,dimensions,obs).topandas()
           
          
            
        conversionsegments.append(conversionsegment)
       
        if tabName == 'Female Part-Time':
            print('{} is done'.format(sheetName))
            
        
    
dataCV = pd.concat(conversionsegments)
dataCV = dataCV[dataCV.GeogNames.notnull()]
dataCV = dataCV[dataCV.Variable.notnull()]
dataCV = dataCV.reset_index(drop=True)
dfCV = v4Writer(output_file,dataCV,asFrame=True) 

#quick check to make sure data and CV data is same length
if len(df.index) != len(dfCV.index):
    raise Exception('Data and CV interval data lengths don\'t match')

#V4 column for dfCV is the CV intervals for data
df['CV'] = dfCV['V4_0']



#more tidying
df = df.drop('Geography',1)
df['industry_codelist'] = df['Geography_codelist'].copy(deep = True)  
df = df.drop('Geography_codelist',1)

df2 = df.copy(deep = True)
df = df2.copy(deep = True)


#create codelist if all
df['industry3'] = df['industry2'].copy().str.lower().str.replace(' ','-')
df['industry_codelist']= df.apply(lambda x: x['industry3'] if x['industry_codelist'] == '' else x['industry_codelist'],axis=1)

'''add in codelists'''

adminURL = 'https://api.cmd-dev.onsdigital.co.uk/v1/code-lists/admin-geography/editions/one-off/codes'
r = requests.get(adminURL)
wholeDict = r.json()
GeogDict = {}
for item in wholeDict['items']:
   GeogDict.update({item['label']:item['id']})


def geogLabelLookup(value):
    '''returns region codes'''
    lookup = {
            'North East':'E12000001',
            'North West':'E12000002',
            'Yorkshire and The Humber':'E12000003',
            'East Midlands':'E12000004',
            'West Midlands':'E12000005',
            'East':'E12000006',
            'London':'E12000007',
            'South East':'E12000008',
            'South West':'E12000009',
            'England':'E92000001',
            'Wales':'W92000004',
            'Scotland':'S92000003',
            'Northern Ireland':'N92000002',
            'England and Wales':'K04000001',
            'Great Britain':'K03000001',
            'United Kingdom':'K02000001'
            }
    return lookup[value]

#pull in codelist for sheetName (ashe-earnings)
sheetNameURL = 'https://api.beta.ons.gov.uk/v1/code-lists/ashe-earnings/editions/one-off/codes'
dataSheetNameCodes = getAllCodes(sheetNameURL)
dataSheetNameLabels = getAllLabels(sheetNameURL)
sheetNameDict = dict(zip(dataSheetNameLabels,dataSheetNameCodes))

def sheetNameLookup(value):
    '''returns ashe-earnings labels from sheetName'''
    value = '.'+value.split('.')[1]
    lookup = {
            '.1a':'Weekly pay - Gross',
            '.2a':'Weekly pay - Excluding overtime',
            '.3a':'Basic pay - Including other pay',
            '.4a':'Overtime pay',
            '.5a':'Hourly pay - Gross',
            '.6a':'Hourly pay - Excluding overtime',
            '.7a':'Annual pay - Gross',
            '.8a':'Annual pay - Incentive',
            '.9a':'Paid hours worked - Total',
            '.10a':'Paid hours worked - Basic',
            '.11a':'Paid hours worked - Overtime'
            }
    return lookup[value]

def sheetNameCodeLookup(value):
    '''returns ashe-earnings codes from labels'''
    return sheetNameDict.get(value,value.lower().replace(' - ','-').replace(' ','-'))


#currently there is a codelist for 'variable' so will use it but will need changing/updating
#pull in codelist for 'variable' (ashe-statistics)
variableURL = 'https://api.beta.ons.gov.uk/v1/code-lists/ashe-statistics/editions/one-off/codes'
dataVariableCodes = getAllCodes(variableURL)
dataVariableLabels = getAllLabels(variableURL)
variableDict = dict(zip(dataVariableLabels,dataVariableCodes))

def variableTypeCodeLookup(value):
    '''returns ashe-statistics code from label'''
    return variableDict.get(value,value)

def variableType(value):
    #one of these lookups needs removing
    '''returns variable labels in a more useable format (string) also matches labels'''
    lookup = {
            '(thousand)':'Number of jobs',
            '10.0':'Percentile - 10',
            '20.0':'Percentile - 20',
            '25.0':'Percentile - 25',
            '30.0':'Percentile - 30',
            '40.0':'Percentile - 40',
            '60.0':'Percentile - 60',
            '70.0':'Percentile - 70',
            '75.0':'Percentile - 75',
            '80.0':'Percentile - 80',
            '90.0':'Percentile - 90'
            }
    lookup2 = {
            '10.0':'10', 
            '20.0':'20', 
            '25.0':'25', 
            '30.0':'30',
            '40.0':'40', 
            '60.0':'60', 
            '70.0':'70', 
            '75.0':'75', 
            '80.0':'80', 
            '90.0':'90',
            '(thousand)':'Number of jobs'
            }
    return lookup2.get(value,value)

#splitting tabName into sex and working pattern

def sexLabels(value):
    '''returns ashe-sex labels from tabName'''
    lookup = {
            'Full-Time':'All', 
            'Part-Time':'All',
            'Male Full-Time':'Male', 
            'Male Part-Time':'Male', 
            'Female Full-Time':'Female',
            'Female Part-Time':'Female'
            }
    return lookup.get(value,value)

def sexCodes(value):
    '''returns ashe-sex codes from labels'''
    return value.lower()

def workingPatternLabels(value):
    '''returns working patterns labels from tabName'''
    lookup = {
            'Male':'All', 
            'Female':'All',
            'Male Full-Time':'Full-Time', 
            'Male Part-Time':'Part-Time', 
            'Female Full-Time':'Full-Time',
            'Female Part-Time':'Part-Time'
            }
    return lookup.get(value,value)

def workingPatternCodes(value):
    '''returns working pattern codes from labels'''
    return value.lower()

#renaming columns
colsRename = {
        'V4_0':'V4_2',
        'Time':'time',
        'Time_codelist':'calendar-years',
        'region':'geography',
        'region_codelist':'admin-geography',
        'Variable':'statistics',
        'Variable_codelist':'ashe-statistics',
        'sheetName':'hoursandearnings',
        'sheetName_codelist':'ashe-hours-and-earnings',
        'industry2':'standardindustrialclassification',
        'industry_codelist':'sic'
        }


#sorting geography
df['region_codelist'] = df['region'].apply(geogLabelLookup)


'''applying functions'''

df['sheetName'] = df['sheetName'].apply(sheetNameLookup)
df['sheetName_codelist'] = df['sheetName'].apply(sheetNameCodeLookup)
df['sheetName_codelist'] = df['sheetName_codelist'].apply(lambda x:x.replace(' ','-'))


df['Variable'] = df['Variable'].apply(variableType)
df['Variable_codelist'] = df['Variable'].apply(variableTypeCodeLookup)

df['tabName_codelist'] = df['tabName'].apply(lambda x:x.lower())

df['Time_codelist'] = df['Time']
df = df.drop('industry',1)


#get gender
def sexLabels(value):
    '''returns ashe-sex labels from tabName'''
    lookup = {
            'Full-Time':'All', 
            'Part-Time':'All',
            'Male Full-Time':'Male', 
            'Male Part-Time':'Male', 
            'Female Full-Time':'Female',
            'Female Part-Time':'Female'
            }
    return lookup.get(value,value)

def sexCodes(value):
    '''returns ashe-sex codes from labels'''
    return value.lower()




#get working pattern
    
def workingPatternLabels(value):
    '''returns working patterns labels from tabName'''
    lookup = {
            'Male':'All', 
            'Female':'All',
            'Male Full-Time':'Full-Time', 
            'Male Part-Time':'Part-Time', 
            'Female Full-Time':'Full-Time',
            'Female Part-Time':'Part-Time'
            }
    return lookup.get(value,value)

def workingPatternCodes(value):
    '''returns working pattern codes from labels'''
    return value.lower()


#change to percentiles
def percentileChange(value):
    #one of these lookups needs removing
    '''matches percentiles'''
    lookup = {
            '10':'10th percentile',
            '20':'20th percentile',
            '25':'25th percentile',
            '30':'30th percentile',
            '40':'40th percentile',
            '60':'60th percentile',
            '70':'70th percentile',
            '75':'75th percentile',
            '80':'80th percentile',
            '90':'90th percentile',
            'Median':'Median',
            'Mean':'Mean'
            }
    return lookup.get(value,value)

#change industries
    
#leading zero first
def leadingZero(value):
    #one of these lookups needs removing
    '''matches percentiles'''
    lookup = {
            '1':'01',
            '2':'02',
            '3':'03',
            '4':'04',
            '5':'05',
            '6':'06',
            '7':'07',
            '8':'08',
            '9':'09'
            }
    return lookup.get(value,value)


def industryChange(value):
    #one of these lookups needs removing
    '''matches labels that need changing back'''
    lookup = {
            'all : All':'Total',
            'all-manufacturing : All Manufacturing':'All Manufacturing',
            'all-index-of-production-industries : All Index of Production Industries':'All Index of Production Industries',
            'all-industries-and-services : All Industries and Services':'All Industries and Services',
            'all-service-industries : All Service Industries':'All Service Industries'
            }
    return lookup.get(value,value)

def industryLabelChange(value):
    #one of these lookups needs removing
    '''changes all'''
    lookup = {
            'all':'total'
            }
    return lookup.get(value,value)

df['workingpattern'] = df['tabName'].apply(workingPatternLabels)
df['ashe-working-pattern'] = df['workingpattern'].apply(industryChange).str.lower()

df['sex'] = df['tabName'].apply(sexLabels)
df['ashe-sex'] = df['sex'].apply(sexCodes)

df3 = df.copy(deep = True)
df = df3.copy(deep = True)


#add code to label
df['industry_codelist'] = df['industry_codelist'].apply(leadingZero)
df['industry2'] = df['industry_codelist'] + ' : ' + df['industry2']
df['industry2'] = df['industry2'].apply(industryChange)
df['industry_codelist'] = df['industry_codelist'].apply(industryLabelChange)


#reordering columns
df = df[['V4_0', 'Data_Marking','CV','Time_codelist', 'Time',
         'region_codelist','region','Variable_codelist','Variable',
         'industry_codelist','industry2', 
         'sheetName_codelist','sheetName','ashe-sex','sex', 'ashe-working-pattern', 'workingpattern']]

df = df.rename(columns = colsRename)
df['statistics'] = df['statistics'].apply(percentileChange)

#data markers for CV's need to be filled in
df.loc[df['CV'] == '','CV'] = 'x'

#find cases where both data marking and obs are NA

df.loc[df['V4_2'] == '','Data_Marking'] = 'x'

#print
df.to_csv(output_file,index=False)

