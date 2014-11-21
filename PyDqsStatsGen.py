# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

import codecs
import collections
import csv
import datetime
import heapq
import logging
import mako
import os
import sys
import time

# imports to support mako templates
from mako.template import Template
from mako.runtime import Context
from io import StringIO

from pprint import pprint

flushCount = 100000
maxRows = 1000

maxHtmlCount = 5
maxJdbcCount = 10

iniFullPath = ''

logFullPath = '~/temp/logFiles/PyIniGenerator.log'

srcFullPath = '~/data/voters/nc/ncvoter48.txt'
srcFullPath = '~/data/apcd/NPPES_Data_Dissemination_November_2014/npidata_20050523-20141112.csv'
srcDelim = ','
srcQuote = csv.QUOTE_MINIMAL
srcHeaderRows = 1

makoFullPath = 'DqsStatsHtml.txt'

tgtFullPath = '~/temp/tgtFiles'
tgtDelim = '|'
tgtQuote = csv.QUOTE_MINIMAL
tgtDqsFileName = 'dqsHtml.html'

srcIdColName = 'SRC_RCD_ID'
apcdSrcIdFmt = '%s.%s.%09d'
apcdSrcIdBgnNbr = 1
apcdSrcIdEndNbr = apcdSrcIdBgnNbr

initVals = {'string':0, 'empty':0, 'whitespace':0, 'alphanumeric':0, 'alpha':0, 'lower':0, 'upper':0, 'date':0, 'datetime':0, 'decimal':0, 'digit':0, 'numeric':0}    

colUniqs = {}

frqValues = {}
frqWidths = {}
minWidths = {}
maxWidths = {}
totWidths = {}
avgWidths = {}
nonBlanks = {}
cvgPrcnts = {}

colCountMisMatches = collections.OrderedDict()

uniqueColNames = ['voter_reg_num','ncid']
uniqueColNames = []

# =============================================================================    
# Inline class definitions
# =============================================================================    

# implement min/max filters
# for restraining logging output
class MinLogLevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return (record.levelno > self.level)

class MaxLogLevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return (record.levelno <= self.level)


# =============================================================================    
# Main routine:
# =============================================================================    

def main():

    successful = False
    err = None
    
    # -----------------------------
    # Logging settings
    # -----------------------------
    # maximum logging level that
    # will output to the STDOUT stream
    MAX_STDOUT_LEVEL = logging.INFO

    logPathExpanded = logFullPath
        
    # if the log file
    # starts with a tilde (~)
    if logFullPath.startswith("~"):
        # derive the log file name's expanded path
        logPathExpanded = os.path.expanduser(logFullPath)
    
    # if the expanded log file name contains a folder prefix
    if os.path.dirname(logPathExpanded) != '':    
        # if the expanded log file's parent folder does not yet exist
        if not os.path.exists(os.path.dirname(logPathExpanded)):
            try:
                # create the log file's parent folder
                os.makedirs(os.path.dirname(logPathExpanded))
            except Exception as e:
                logging.error(str(e))
    
    # if the specified log file exists            
    if os.path.exists(logPathExpanded):
        # delete it
        os.remove(logPathExpanded)

    maxStdOutLvl = "INFO"

    if maxStdOutLvl.lower() == 'info':
        MAX_STDOUT_LEVEL = logging.INFO
    elif maxStdOutLvl.lower() == 'debug':
        MAX_STDOUT_LEVEL = logging.DEBUG
    elif maxStdOutLvl.lower() == 'warning':
        MAX_STDOUT_LEVEL = logging.WARNING
    elif maxStdOutLvl.lower() == 'error':
        MAX_STDOUT_LEVEL = logging.ERROR
    elif maxStdOutLvl.lower() == 'critical':
        MAX_STDOUT_LEVEL = logging.CRITICAL
    else:
        MAX_STDOUT_LEVEL = logging.INFO
    
    # instantiate the logger object
    logger = logging.getLogger(__name__)
    
    # remove any existing log handlers
    logging.getLogger('').handlers = []
    
    # set the default logger's values
    logging.basicConfig(level=MAX_STDOUT_LEVEL,
                        format='%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s',
                        datefmt='%Y-%m-%d %H:%M',
                        filename=logPathExpanded,
                        filemode='w')
    
    # attach stdout to the logger
    # so that outputing to the log also
    # outputs to the stdout console
    logStdOut = logging.StreamHandler(sys.stdout)
    logStdOut.addFilter(MaxLogLevelFilter(MAX_STDOUT_LEVEL))
    logging.getLogger('').addHandler(logStdOut)                    
    
    # attach stderr to the logger
    # so that outputing to the log also
    # outputs to the stderr console
    logStdErr = logging.StreamHandler(sys.stderr)
    logStdErr.addFilter(MinLogLevelFilter(MAX_STDOUT_LEVEL))
    logging.getLogger('').addHandler(logStdErr)
    
    # output a message to the log file
    # with the log file's location info
    logging.info('Log file located at %s', logPathExpanded)
    # logging.debug('DEBUG: Log file located at %s', logFileNameExpanded)
    # logging.warning('WARNING: Log file located at %s', logFileNameExpanded)
    # logging.error('Log file located at %s', logFileNameExpanded)

    # -------------------------------------------------------------------------
    
    srcPathExpanded = srcFullPath
    if srcFullPath.startswith('~'):
        srcPathExpanded = os.path.expanduser(srcFullPath)
    srcPathExpanded = os.path.abspath(srcPathExpanded)
    logging.info("SRC file %s" % srcPathExpanded)
    if not os.path.exists(srcPathExpanded):
        logging.error("SRC file does NOT exist: %s" % srcPathExpanded)
        successful = False
        return successful, err

    tgtPathExpanded = tgtFullPath
    if tgtFullPath.startswith('~'):
        tgtPathExpanded = os.path.expanduser(tgtFullPath)
    tgtPathExpanded = os.path.join(tgtPathExpanded, os.path.splitext(os.path.basename(srcPathExpanded))[0] + ".html")
    tgtPathExpanded = os.path.abspath(tgtPathExpanded)
    logging.info("TGT file %s" % tgtPathExpanded)
    if not os.path.exists(os.path.dirname(tgtPathExpanded)):
        os.makedirs(os.path.dirname(tgtPathExpanded))
        
    makoPathExpanded = makoFullPath
    if makoPathExpanded.startswith('~'):
        makoPathExpanded = os.path.expanduser(makoFullPath)
    makoPathExpanded = os.path.abspath(makoPathExpanded)
    logging.info("MAKO file %s" % makoPathExpanded)
        
    colNames = []
    colStats = collections.OrderedDict()
    
    # open the source file for reading
    srcFile = codecs.open(srcPathExpanded, 'r', 'cp1252')
    csvReader = csv.reader(srcFile, delimiter=srcDelim, quoting=srcQuote)

    bgnTime = time.time()
    
    rows = 0
    dataRows = 0
    for rowData in csvReader:
        rows += 1
        if rows == 1:
            colNames, colStats = analyzeHead(rowData, colNames, colStats)
        else:
            dataRows += 1
            analyzeData(rowData, colNames,uniqueColNames, rows)
        if maxRows > 0 and rows > maxRows:
            break
        if dataRows > 0 and dataRows % flushCount == 0:
            endTime = time.time()
            seconds = endTime - bgnTime
            if seconds > 0:
                rcdsPerSec = dataRows / seconds
            else:
                rcdsPerSec = 0
            print ("Read {:,} data rows in {:,.0f} seconds @ {:,.0f} records/second".format(dataRows, seconds, rcdsPerSec))
            
    del csvReader
    srcFile.close()

    endTime = time.time()
    seconds = endTime - bgnTime
    if seconds > 0:
        rcdsPerSec = dataRows / seconds
    else:
        rcdsPerSec = 0
        
    print ('')
    print ("Read {:,} data rows in {:,.0f} seconds @ {:,.0f} records/second".format(dataRows, seconds, rcdsPerSec))
    
    for colName in colNames:
        if dataRows > 0:
            avgWidths[colName] = (totWidths[colName] * 1.0) / (dataRows * 1.0)
            cvgPrcnts[colName] = (nonBlanks[colName] * 1.0) / (dataRows * 1.0)
        else:
            avgWidths[colName] = 0.0
            cvgPrcnts[colName] = 0.0
            
#    print ("rowNbr,colCount")
#    for row, colCount in colCountMisMatches.items():
#        print (row, colCount)
            
#    print ("srcColName,minWidth,maxWidth,avgWidth,nonBlanks,cvgPrcnt,unique")    
#    for colName in colNames:
#        print ("%s,%d,%d,%f,%d,%f,%s" % (colName, minWidths[colName], maxWidths[colName], avgWidths[colName], nonBlanks[colName], cvgPrcnts[colName] * 100, colName in uniqueColNames))
            
#    print ("srcColName,colValue,frequency")    
    frqValueAscs = collections.OrderedDict()
    for colName in colNames:
        frqValueAscs[colName] = {}
        # bypass columns with unique values since
        # no value frequencies were tracked for them
        if not colName in uniqueColNames:
            frqValueAscs[colName] = sorted(frqValues[colName].items(), key=lambda x:x[0])
#            for value,frequency in frqValueAsc:
#                nonBlank = nonBlanks[colName]
#                if nonBlank > 0:
#                    frqPct = frequency*100.0/nonBlank
#                else:
#                    frqPct = 0
#                print ("%s,%s,%d,%f" % (colName, value, frequency, frqPct))
            
#    print ("srcColName,colWidth,frequency")    
    frqWidthAscs = collections.OrderedDict()
    for colName in colNames:
        frqWidthAscs[colName] = sorted(frqWidths[colName].items(), key=lambda x:x[0])
#        for width,frequency in frqWidthAsc: 
#            nonBlank = nonBlanks[colName]
#            if nonBlank > 0:
#                frqPct = frequency*100.0/nonBlank
#            else:
#                frqPct = 0
#            print ("%s,%d,%d,%f" % (colName, width, frequency, frqPct))
#            
#    print ("srcColName,valvalasc,frequency,valfrqasc,frequency,valfrqdsc,frequency")
    valueFreqs = collections.OrderedDict()
    for colName in colNames:
        valueFreqs[colName] = {}
        valueFreqs[colName]['frqValValAsc'] = {}
        valueFreqs[colName]['frqValFrqAsc'] = {}
        valueFreqs[colName]['frqValFrqDsc'] = {}
        if not colName in uniqueColNames:
            # frqValValAsc = sorted(frqValues[colName].items(), key=lambda x:x[0])[:maxValues] 
            # frqValFrqAsc = sorted(frqValues[colName].items(), key=lambda x:x[1])[:maxValues] 
            # frqValFrqDsc = sorted(frqValues[colName].items(), key=lambda x:x[1], reverse=True)[:maxValues]
            valueFreqs[colName]['frqValValAsc'] = heapq.nsmallest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[0])
            valueFreqs[colName]['frqValFrqAsc'] = heapq.nsmallest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[1])
            valueFreqs[colName]['frqValFrqDsc'] = heapq.nlargest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[1])
            size = len(valueFreqs[colName]['frqValValAsc'])
            for i in range(0, size):
                nonBlank = nonBlanks[colName]
                if nonBlank > 0:
                    frqValValAscPct = valueFreqs[colName]['frqValValAsc'][i][1]*100.0/nonBlank
                    frqValFrqAscPct = valueFreqs[colName]['frqValFrqAsc'][i][1]*100.0/nonBlank
                    frqValFrqDscPct = valueFreqs[colName]['frqValFrqDsc'][i][1]*100.0/nonBlank
                else:
                    frqValValAscPct = 0
                    frqValFrqAscPct = 0
                    frqValFrqDscPct = 0
#                if nonBlanks[colName] > 0:
#                    print ("%s,%s,%d,%f,%s,%d,%f,%s,%d,%f" % (colName, valueFreqs[colName]['frqValValAsc'][i][0], valueFreqs[colName]['frqValValAsc'][i][1], frqValValAscPct, valueFreqs[colName]['frqValFrqAsc'][i][0], valueFreqs[colName]['frqValFrqAsc'][i][1], frqValFrqAscPct, valueFreqs[colName]['frqValFrqDsc'][i][0], valueFreqs[colName]['frqValFrqDsc'][i][1], frqValFrqDscPct))
               
    htmWriter = codecs.open(tgtPathExpanded, 'w', 'cp1252')
                    
    makoTemplate = Template(filename=makoFullPath)
    buffer = StringIO()
    attrs = {}
    parms = {
        'attrs':attrs,
        'srcPathExpanded':srcPathExpanded,
        'srcDelim':srcDelim,
        'srcIdColName':srcIdColName,
        'srcHeaderRows':srcHeaderRows,
        'tgtDelim':tgtDelim,
        'maxRows':maxRows,
        'maxHtmlCount':maxHtmlCount,
        'maxJdbcCount':maxJdbcCount,
        'tgtDqsFileNameExpanded':tgtPathExpanded,
        'inputRows':dataRows,
        'inputCols':len(colNames),
        'apcdSrcIdFmt':apcdSrcIdFmt,
        'apcdSrcIdBgnNbr':apcdSrcIdBgnNbr,
        'apcdSrcIdEndNbr':(apcdSrcIdBgnNbr + dataRows - 1),
        'colNames':colNames,
        'nonBlanks':nonBlanks,
        'valueFreqs':valueFreqs,
        'minWidths':minWidths,
        'maxWidths':maxWidths,
        'avgWidths':avgWidths,
        'frqValueAscs':frqValueAscs,
        'frqWidthAscs':frqWidthAscs
        }
    context = Context(buffer, **parms)
    makoTemplate.render_context(context)
    
    htmWriter.write(buffer.getvalue())
    htmWriter.close()
    
    return

#    pprint (minWidths)
#    pprint (maxWidths)
#    pprint (avgWidths)
#    pprint (nonBlanks)
#    pprint (cvgPrcnts)
#    pprint (frqWidths)
#    pprint (frqValues)

def analyzeHead(rowCells, colNames, colStats):
    for colName in rowCells:
        colNames.append(colName)
        colStats[colName] = collections.OrderedDict(initVals)
        colUniqs[colName] = {}
        frqValues[colName] = {}
        frqWidths[colName] = {}
        minWidths[colName] = sys.maxsize
        maxWidths[colName] = 0
        totWidths[colName] = 0
        avgWidths[colName] = 0.0
        nonBlanks[colName] = 0
        cvgPrcnts[colName] = 0.0
    return colNames, colStats
    
def analyzeData(rowCells, colNames, uniqueColNames, row):
    cells = 0
    # only evaluate rows with
    # expected number of columns
    if len(rowCells) == len(colNames): 
        for cellValue in rowCells:
            colName = colNames[cells]
            value = cellValue.strip()
            width = len(value)
            totWidths[colName] += width
            try:
                frqWidths[colName][width] += 1
            except:
                frqWidths[colName][width] = 1
            if width > 0:
                nonBlanks[colName] += 1
            if width > maxWidths[colName]:
                maxWidths[colName] = width
            if width < minWidths[colName]:
                minWidths[colName] = width
            if not colName in uniqueColNames:
                try:
                    frqValues[colName][value] += 1
                except:
                    frqValues[colName][value] = 1
            else:
                frqValues[colName] = 1
            cells += 1
    else:
        colCountMisMatches[row] = len(rowCells)
    return
    
# ============================================================================
# execute the mainline processing routine
# ============================================================================

if (__name__ == "__main__"):
    retval = main()
