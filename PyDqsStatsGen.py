# -*- coding: utf-8 -*-

# TODO: output statistics to Sql Server database

import codecs
import collections
import csv
import getpass
import heapq
import html
import logging
import mysql.connector
import os
import pymssql
import psycopg2
import sqlite3
import sys
import time

# imports to support mako templates
from mako.template import Template
from mako.runtime import Context
from io import StringIO

from pprint import pprint

maxRows = 10000
flushCount = 10000

maxHtmlCount = 5
maxJdbcCount = 1

iniFullPath = ''

dataProvider = 'NPPES'
executorName = ''

# srcFullPath = '~/data/voters/nc/ncvoter48.txt'
srcFullPath = '~/data/apcd/NPPES_Data_Dissemination_November_2014/npidata_20050523-20141112.csv'
srcDelim = ','
srcQuote = csv.QUOTE_MINIMAL
srcHeaderRows = 1

uniqueColNames = ['voter_reg_num','ncid']
uniqueColNames = ['NPI']
ignoreColNames = ['voter_reg_num','ncid']
ignoreColNames = ['Entity Type Code']

makoHtmlFullPath = 'DqsStatsHtml.mako'
makoJdbcFullPath = 'DqsStatsJdbc.mako'

tgtFullPath = '~/temp/tgtFiles'
tgtDelim = '|'
tgtQuote = csv.QUOTE_MINIMAL
tgtDqsStatsHtml = 'dqsStats.html'
tgtDqsStatsJdbc = 'dqsStats.sqlite'

# PostgreSQL parameters
jdbcType = 'pgsql'
jdbcHost = 'localhost'
jdbcPort = 5432
jdbcDatabase = 'dqsvalidator'
jdbcUID = 'dqsvalidator'
jdbcPWD = '[redacted]'
jdbcDropCompliant = True
jdbcParms = {
    'host':jdbcHost,
    'port': jdbcPort,
    'dbname':jdbcDatabase,
    'user':jdbcUID,
    'password':jdbcPWD        
    }

# MySQL parameters
jdbcType = 'mysql'
jdbcHost = 'localhost'
jdbcPort = 3306
jdbcDatabase = 'dqsvalidator'
jdbcUID = 'dqsvalidator'
jdbcPWD = '[redacted]'
jdbcDropCompliant = True
jdbcParms = {
    'host':jdbcHost,
    'port': jdbcPort,
    'database':jdbcDatabase,
    'user':jdbcUID,
    'password':jdbcPWD        
    }

# MsSQL parameters
jdbcType = 'mssql'
jdbcHost = 'Khepry-ASUS-LT1'
jdbcPort = 1433
jdbcDatabase = 'dqsvalidator'
jdbcUID = 'dqsvalidator'
jdbcPWD = '[redacted]'
jdbcDropCompliant = False
jdbcParms = {
    'server':jdbcHost + ":" + str(jdbcPort),
    'port': jdbcPort,
    'database':jdbcDatabase,
    'user':jdbcUID,
    'password':jdbcPWD        
    }

srcIdColName = 'SRC_RCD_ID'
apcdSrcIdFmt = '%s.%s.%09d'
apcdSrcIdBgnNbr = 1
apcdSrcIdEndNbr = apcdSrcIdBgnNbr

logFullPath = '~/temp/logFiles/PyIniGenerator.log'

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

frqHtmlValues = {}
frqJdbcValues = {}

colCountMisMatches = collections.OrderedDict()

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
#    logger = logging.getLogger(__name__)
    
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
    
    executorName = getpass.getuser()
    runDate = time.strftime('%A %d %b %Y %I:%M %p %Z')
    
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
    tgtDqsStatsHtmlExpanded = os.path.join(tgtPathExpanded, os.path.splitext(os.path.basename(srcPathExpanded))[0] + ".html")
    tgtDqsStatsJdbcExpanded = os.path.join(tgtPathExpanded, os.path.splitext(os.path.basename(srcPathExpanded))[0] + ".sqlite")
    tgtDqsStatsHtmlExpanded = os.path.abspath(tgtDqsStatsHtmlExpanded)
    tgtDqsStatsJdbcExpanded = os.path.abspath(tgtDqsStatsJdbcExpanded)
    logging.info("TGT DQS Statistics HTML file %s" % tgtDqsStatsHtmlExpanded)
    logging.info("TGT DQS Statistics JDBC file %s" % tgtDqsStatsJdbcExpanded)
    if not os.path.exists(os.path.dirname(tgtDqsStatsHtmlExpanded)):
        os.makedirs(os.path.dirname(tgtDqsStatsHtmlExpanded))
    if not os.path.exists(os.path.dirname(tgtDqsStatsJdbcExpanded)):
        os.makedirs(os.path.dirname(tgtDqsStatsJdbcExpanded))
        
    makoHtmlPathExpanded = makoHtmlFullPath
    if makoHtmlPathExpanded.startswith('~'):
        makoHtmlPathExpanded = os.path.expanduser(makoHtmlPathExpanded)
    makoHtmlPathExpanded = os.path.abspath(makoHtmlPathExpanded)
    logging.info("DQS Statistics HTML MAKO template file %s" % makoHtmlPathExpanded)
        
    makoJdbcPathExpanded = makoJdbcFullPath
    if makoJdbcPathExpanded.startswith('~'):
        makoJdbcPathExpanded = os.path.expanduser(makoJdbcPathExpanded)
    makoJdbcPathExpanded = os.path.abspath(makoJdbcPathExpanded)
    logging.info("DQS Statistics JDBC MAKO template file %s" % makoJdbcPathExpanded)
        
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
            analyzeData(rowData, colNames,uniqueColNames, ignoreColNames, rows)
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
            
    frqValueAscs = collections.OrderedDict()
    for colName in colNames:
        frqValueAscs[colName] = {}
        # bypass columns with unique values since
        # no value frequencies were tracked for them
        if (not colName in uniqueColNames) and (not colName in ignoreColNames):
            frqValueAscs[colName] = sorted(frqValues[colName].items(), key=lambda x:x[0])
            
    frqWidthAscs = collections.OrderedDict()
    for colName in colNames:
        frqWidthAscs[colName] = sorted(frqWidths[colName].items(), key=lambda x:x[0])

    # -------------------------------------------------------------------------
    # Output DQS statistics to HTML file               
    # -------------------------------------------------------------------------

    valueFreqs = collections.OrderedDict()
    for colName in colNames:
        valueFreqs[colName] = {}
        valueFreqs[colName]['frqValValAsc'] = {}
        valueFreqs[colName]['frqValFrqAsc'] = {}
        valueFreqs[colName]['frqValFrqDsc'] = {}
        if not colName in uniqueColNames and not colName in ignoreColNames:
            if maxHtmlCount > 0:
                valueFreqs[colName]['frqValValAsc'] = heapq.nsmallest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[0])
                valueFreqs[colName]['frqValFrqAsc'] = heapq.nsmallest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[1])
                valueFreqs[colName]['frqValFrqDsc'] = heapq.nlargest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[1])
            else:
                valueFreqs[colName]['frqValValAsc'] = sorted(frqValues[colName].items(), key=lambda x:x[0])
                valueFreqs[colName]['frqValFrqAsc'] = sorted(frqValues[colName].items(), key=lambda x:x[1])
                valueFreqs[colName]['frqValFrqDsc'] = sorted(frqValues[colName].items(), key=lambda x:x[1], reverse=True)

    htmlWriter = codecs.open(tgtDqsStatsHtmlExpanded, 'w', 'cp1252')
                    
    makoHtmlTemplate = Template(filename=makoHtmlFullPath)
    buffer = StringIO()
    attrs = {}
    parms = {
        'attrs':attrs,
        'dataProvider': dataProvider,
        'executorName': executorName,
        'runDate': runDate,
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
        'uniqueColNames':uniqueColNames,
        'ignoreColNames':ignoreColNames,
        'nonBlanks':nonBlanks,
        'valueFreqs':valueFreqs,
        'minWidths':minWidths,
        'maxWidths':maxWidths,
        'avgWidths':avgWidths,
        'frqValueAscs':frqValueAscs,
        'frqWidthAscs':frqWidthAscs
        }
    context = Context(buffer, **parms)
    makoHtmlTemplate.render_context(context)
    
    htmlWriter.write(buffer.getvalue())
    htmlWriter.close()

    # -------------------------------------------------------------------------
    # Output DQS statistics to JDBC file (SQLite)               
    # -------------------------------------------------------------------------

    valueFreqs.clear()
    for colName in colNames:
        valueFreqs[colName] = {}
        valueFreqs[colName]['frqValValAsc'] = {}
        valueFreqs[colName]['frqValFrqAsc'] = {}
        valueFreqs[colName]['frqValFrqDsc'] = {}
        if not colName in uniqueColNames and not colName in ignoreColNames:
            if maxJdbcCount > 0:
                valueFreqs[colName]['frqValValAsc'] = heapq.nsmallest(maxJdbcCount, frqValues[colName].items(), key=lambda x:x[0])
                valueFreqs[colName]['frqValFrqAsc'] = heapq.nsmallest(maxJdbcCount, frqValues[colName].items(), key=lambda x:x[1])
                valueFreqs[colName]['frqValFrqDsc'] = heapq.nlargest(maxJdbcCount, frqValues[colName].items(), key=lambda x:x[1])
            else:
                valueFreqs[colName]['frqValValAsc'] = sorted(frqValues[colName].items(), key=lambda x:x[0])
                valueFreqs[colName]['frqValFrqAsc'] = sorted(frqValues[colName].items(), key=lambda x:x[1])
                valueFreqs[colName]['frqValFrqDsc'] = sorted(frqValues[colName].items(), key=lambda x:x[1], reverse=True)
    
    # TODO: implement SQL as parameterized queries, necessitating NOT using MAKO template

    # push statistic records to SQLite database
    sqliteConn = sqlite3.connect(tgtDqsStatsJdbcExpanded)
    sqliteCursor = sqliteConn.cursor()
                    
    makoJdbcTemplate = Template(filename=makoJdbcFullPath)
    buffer = StringIO()
    attrs = {}
    parms = {
        'attrs':attrs,
        'dataProvider': dataProvider,
        'executorName': executorName,
        'runDate': runDate,
        'srcPathExpanded':srcPathExpanded,
        'srcDelim':srcDelim,
        'srcIdColName':srcIdColName,
        'srcHeaderRows':srcHeaderRows,
        'tgtDelim':tgtDelim,
        'maxRows':maxRows,
        'maxHtmlCount':maxHtmlCount,
        'maxJdbcCount':maxJdbcCount,
        'tgtDqsStatsHtmlExpanded':tgtDqsStatsHtmlExpanded,
        'tgtDqsStatsJdbcExpanded':tgtDqsStatsJdbcExpanded,
        'inputRows':dataRows,
        'inputCols':len(colNames),
        'apcdSrcIdFmt':apcdSrcIdFmt,
        'apcdSrcIdBgnNbr':apcdSrcIdBgnNbr,
        'apcdSrcIdEndNbr':(apcdSrcIdBgnNbr + dataRows - 1),
        'colNames':colNames,
        'uniqueColNames':uniqueColNames,
        'ignoreColNames':ignoreColNames,
        'nonBlanks':nonBlanks,
        'valueFreqs':valueFreqs,
        'minWidths':minWidths,
        'maxWidths':maxWidths,
        'avgWidths':avgWidths,
        'frqValueAscs':frqValueAscs,
        'frqWidthAscs':frqWidthAscs,
        'jdbcDropCompliant': True # True for SQLite databases
        }
    context = Context(buffer, **parms)
    makoJdbcTemplate.render_context(context)
    
    sqlCmd = ''
    lines = buffer.getvalue().split(os.linesep)
    for line in lines:
        sqlCmd = sqlCmd + line.strip()
        if line.strip().endswith(';'):
            # print (sqlCmd)
            # print ('')
            try:
                sqliteCursor.execute(sqlCmd)
            except Exception as e:
                print (sqlCmd)
                print (str(e))
                break;
            sqlCmd = ''
    
    sqliteConn.commit()
    sqliteConn.close()
        
    # push statistic records to traditional database

    # if jdbcType not found
    # default to PostgreSQL
    jdbcConn = None
    if jdbcType.lower() == 'mysql':
        jdbcConn = mysql.connector.connect(**jdbcParms)
    elif jdbcType.lower() == 'mssql':
        jdbcConn = pymssql.connect(**jdbcParms)
    else:
        jdbcConn = psycopg2.connect(**jdbcParms)
    jdbcCursor = jdbcConn.cursor()
                    
    makoJdbcTemplate = Template(filename=makoJdbcFullPath)
    buffer = StringIO()
    attrs = {}
    parms = {
        'attrs':attrs,
        'dataProvider': dataProvider,
        'executorName': executorName,
        'runDate': runDate,
        'srcPathExpanded':srcPathExpanded,
        'srcDelim':srcDelim,
        'srcIdColName':srcIdColName,
        'srcHeaderRows':srcHeaderRows,
        'tgtDelim':tgtDelim,
        'maxRows':maxRows,
        'maxHtmlCount':maxHtmlCount,
        'maxJdbcCount':maxJdbcCount,
        'tgtDqsStatsHtmlExpanded':tgtDqsStatsHtmlExpanded,
        'tgtDqsStatsJdbcExpanded':tgtDqsStatsJdbcExpanded,
        'inputRows':dataRows,
        'inputCols':len(colNames),
        'apcdSrcIdFmt':apcdSrcIdFmt,
        'apcdSrcIdBgnNbr':apcdSrcIdBgnNbr,
        'apcdSrcIdEndNbr':(apcdSrcIdBgnNbr + dataRows - 1),
        'colNames':colNames,
        'uniqueColNames':uniqueColNames,
        'ignoreColNames':ignoreColNames,
        'nonBlanks':nonBlanks,
        'valueFreqs':valueFreqs,
        'minWidths':minWidths,
        'maxWidths':maxWidths,
        'avgWidths':avgWidths,
        'frqValueAscs':frqValueAscs,
        'frqWidthAscs':frqWidthAscs,
        'jdbcDropCompliant': jdbcDropCompliant # True for PostgreSQL and MySQL databases, False for MsSQL
        }
    context = Context(buffer, **parms)
    makoJdbcTemplate.render_context(context)
    
    sqlCmd = ''
    lines = buffer.getvalue().split(os.linesep)
    for line in lines:
        sqlCmd = sqlCmd + line.strip()
        if line.strip().endswith(';'):
            # print (sqlCmd)
            # print ('')
            try:
                jdbcCursor.execute(sqlCmd)
            except Exception as e:
                print (sqlCmd)
                print (str(e))
                break;
            sqlCmd = ''

    jdbcConn.commit()
    jdbcConn.close()
    
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
    
def analyzeData(rowCells, colNames, uniqueColNames, ignoreColNames, row):
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
            if not colName in uniqueColNames and not colName in ignoreColNames:
                try:
                    frqValues[colName][value] += 1
                except:
                    frqValues[colName][value] = 1
            else:
                frqValues[colName] = {}
            cells += 1
    else:
        colCountMisMatches[row] = len(rowCells)
    return
    
# ============================================================================
# execute the mainline processing routine
# ============================================================================

if (__name__ == "__main__"):
    retval = main()
