# -*- coding: utf-8 -*-

import codecs
import collections
import configparser
import csv
import getpass
import heapq
import logging
import mysql.connector
import os
import pymssql
import psycopg2
import sqlite3
import sys
import time

# import(s) to support INI file parsing
from configparser import BasicInterpolation, ExtendedInterpolation

# import(s) to support MAKO template processing
from mako.template import Template
from mako.runtime import Context
from io import StringIO

from pprint import pprint

iniFilePath = 'PyDqsStatsGen.ini'

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

colCountMisMatches = []

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

    global iniFilePath

    # obtain any command-line arguments
    # overriding any values set so far
    nextArg = ""
    for argv in sys.argv:
        if nextArg != "":
            if nextArg == "iniFilePath":
                iniFilePath = argv
            nextArg = ""
        else:
            if argv.lower() == "--inifilepath" or argv.lower() == "-inifilepath":
                nextArg = "iniFilePath"
        
    # expand any leading tilde
    # to the user's home path
    if iniFilePath.startswith("~"):
        iniFilePath = os.path.expanduser(iniFilePath)
    
    iniFilePath = os.path.abspath(iniFilePath)
    
    print ("Attempting to load INI file: %s" % iniFilePath)
    
    # if INI file path does not exist
    if not os.path.exists(iniFilePath):
        # output error message
        sys.stderr.write('iniFilePath does not exist: "%s"\n' % iniFilePath)
        # cease further processing
        sys.exit(0)
    
    # obtain the settings
    # from the INI file path
    config = configparser.ConfigParser(interpolation=ExtendedInterpolation(), delimiters=('='))
    config.optionxform = str #this will preserve the case of the section names
    config.read(iniFilePath)
    
    # -----------------------------
    # Logging settings
    # -----------------------------

    logFilePathExpanded = 'PyDqsStatsGen.log'
    # if the log file
    # starts with a tilde (~)
    if logFilePathExpanded.startswith("~"):
        # derive the log file name's expanded path
        logFilePathExpanded = os.path.expanduser(logFilePathExpanded)
    
    # if the expanded log file name contains a folder prefix
    if os.path.dirname(logFilePathExpanded) != '':    
        # if the expanded log file's parent folder does not yet exist
        if not os.path.exists(os.path.dirname(logFilePathExpanded)):
            try:
                # create the log file's parent folder
                os.makedirs(os.path.dirname(logFilePathExpanded))
            except Exception as e:
                logging.error(str(e))
    
    # if the specified log file exists            
    if os.path.exists(logFilePathExpanded):
        # delete it
        os.remove(logFilePathExpanded)
    
    # maximum logging level that
    # will output to the STDOUT stream
    MAX_STDOUT_LEVEL = logging.INFO
    
    # obtain the [folders] section's settings from the INI file
    outFolder = config['folders'].get('outFolder', '~/temp')
    logSubFolder = config['folders'].get('logSubFolder', 'logFiles')
    tgtSubFolder = config['folders'].get('tgtSubFolder', 'tgtFiles')

    tgtFullPath = os.path.join(outFolder, tgtSubFolder)

    # obtain the [logging] section's settings from the INI file
    logFileName = config['logging'].get('logFileName', 'PyDqsStatsGen.log')
    maxStdOutLvl = config['logging'].get('MAX_STDOUT_LEVEL', 'info')
    
    # if log file name
    # was not specified    
    if logFileName == "":
        # default the log file name
        logFileName = "PyDqsStatsGen.log"

    logFullPathExpanded = logFileName
    # if the log file
    # starts with a tilde (~)
    if logFullPathExpanded.startswith("~"):
        # derive the log file name's expanded path
        logFullPathExpanded = os.path.expanduser(logFullPathExpanded)

    # if the expanded log file name does NOT contain a folder prefix
    if os.path.dirname(logFullPathExpanded) == '':
        # expand the output folder joined to the default log subfolder joined to the log file name
        logFullPathExpanded = os.path.expanduser(os.path.join(outFolder, logSubFolder, logFullPathExpanded))
    
    # if the expanded log file name contains a folder prefix
    if os.path.dirname(logFullPathExpanded) != '':    
        # if the expanded log file's parent folder does not yet exist
        if not os.path.exists(os.path.dirname(logFullPathExpanded)):
            try:
                # create the log file's parent folder
                os.makedirs(os.path.dirname(logFullPathExpanded))
            except Exception as e:
                logging.error(str(e))
    
    # if the specified log file exists            
    if os.path.exists(logFullPathExpanded):
        # delete it
        os.remove(logFullPathExpanded)
    
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
    # logger = logging.getLogger(__name__)
    
    # remove any existing log handlers
    logging.getLogger('').handlers = []
    
    # set the default logger's values
    logging.basicConfig(level=MAX_STDOUT_LEVEL,
                        format='%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s',
                        datefmt='%Y-%m-%d %H:%M',
                        filename=logFullPathExpanded,
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
    logging.info('Log file: %s', logFullPathExpanded)
    # logging.debug('DEBUG: Log file located at %s', logFilePathExpanded)
    # logging.warning('WARNING: Log file located at %s', logFilePathExpanded)
    # logging.error('Log file located at %s', logFilePathExpanded)

    # -------------------------------------------------------------------------

    # max data rows
    # to be processed
    # 0 means unlimited
    maxRows = int(config['DEFAULT'].get('maxRows', '0'))
    
    # show progress messages
    # every 'flushCount' number
    # of data rows
    flushCount = int(config['DEFAULT'].get('flushCount', '10000'))
    
    # maximum allowed column
    # count mismatches before
    # terminating the program
    maxColCountMisMatches = int(config['DEFAULT'].get('maxColCountMisMatches', '0'))
    
    # max number of HTML value
    # frequencies per column to report
    maxHtmlCount = int(config['DEFAULT'].get('maxHtmlCount', '5'))
    
    # max number of JDBC value
    # frequencies per column to report
    maxJdbcCount = int(config['DEFAULT'].get('maxJdbcCount', '10'))
    
    # the date of execution's output format string    
    runDateFormatString = config['DEFAULT'].get('runDateFormatString', '%A %d %b %Y %I:%M %p %Z')
    
    # MAKO template path
    # for HTML output generation
    makoHtmlTemplateName = config['DEFAULT'].get('makoHtmlTemplateName', 'DqsStatsHtml.mako')

    # MAKO template path
    # for JDBC output generation
    makoJdbcTemplateName = config['DEFAULT'].get('makoJdbcTemplateName', 'DqsStatsJdbc.mako')
     
    srcFullPath = config['srcSpecs'].get('srcFullPath')
    srcDelim = config['srcSpecs'].get('srcDelim', ',')
    srcHeaderRows = int(config['srcSpecs'].get('srcHeaderRows', '1'))
    srcQuote = csv.QUOTE_MINIMAL

    # data provider's acronym    
    dataProvider = config['srcSpecs'].get('dataProvider', 'unspecified')
    
    # comma-delimited list of columns upon
    # which statistics are to be calculated,
    # an empty ACCEPT list will signal the
    # processing of ALL of the row's columns
    acceptColNames = config['srcSpecs'].get('acceptColNames','').split(',')
    
    # comma-delimited IGNORE list will suppress calculations
    # of the value frequency statistics for the specified columns
    # ignoreColNames = 'voter_reg_num','ncid'
    ignoreColNames = config['srcSpecs'].get('ignoreColNames','').split(',')
    
    # comma-delimited UNIQUE list will suppress calculations
    # of the value frequency statistics for the specified columns
    # uniqueColNames = 'voter_reg_num','ncid'
    uniqueColNames = config['srcSpecs'].get('uniqueColNames','').split(',')

    # obtain the JDBC database connection parameters    
    jdbcType = config['jdbcSpecs'].get('jdbcType', 'pgsql').lower()
    jdbcHost = config['jdbcSpecs'].get('jdbcHost', 'localhost')
    jdbcPort = int(config['jdbcSpecs'].get('jdbcPort', '5432')) # defaults to PostgreSQL's port
    jdbcDatabase = config['jdbcSpecs'].get('jdbcDatabase', 'dqsvalidator')
    jdbcUID = config['jdbcSpecs'].get('jdbcUID', 'dqsvalidator')
    jdbcPWD = config['jdbcSpecs'].get('jdbcPWD', '[redacted]')
    jdbcDropTableIfExistsCompliant = (config['jdbcSpecs'].get('jdbcDropTableIfExistsCompliant', 'True') == 'True')
    
    # tweak the connection parameters
    # depending upon the target database
    
    # is it MySQL
    if jdbcType == 'mysql':
        jdbcParms = {
            'host':jdbcHost,
            'port': jdbcPort,
            'database':jdbcDatabase,
            'user':jdbcUID,
            'password':jdbcPWD        
            }
    # is it SQL Server?
    elif jdbcType == 'mssql':
        jdbcParms = {
            'server':jdbcHost + ":" + str(jdbcPort),
            'port': jdbcPort,
            'database':jdbcDatabase,
            'user':jdbcUID,
            'password':jdbcPWD        
            }
    # otherwise
    else:
        # default to PostgreSQL
        jdbcParms = {
            'host':jdbcHost,
            'port': jdbcPort,
            'dbname':jdbcDatabase,
            'user':jdbcUID,
            'password':jdbcPWD        
    }

    executorName = getpass.getuser()
    runDate = time.strftime(runDateFormatString)
    
    srcPathExpanded = srcFullPath
    if srcFullPath.startswith('~'):
        srcPathExpanded = os.path.expanduser(srcFullPath)
    srcPathExpanded = os.path.abspath(srcPathExpanded)
    logging.info("SRC file: %s" % srcPathExpanded)
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
    logging.info("TGT DQS Statistics HTML file: %s" % tgtDqsStatsHtmlExpanded)
    logging.info("TGT DQS Statistics JDBC file: %s" % tgtDqsStatsJdbcExpanded)
    if not os.path.exists(os.path.dirname(tgtDqsStatsHtmlExpanded)):
        os.makedirs(os.path.dirname(tgtDqsStatsHtmlExpanded))
    if not os.path.exists(os.path.dirname(tgtDqsStatsJdbcExpanded)):
        os.makedirs(os.path.dirname(tgtDqsStatsJdbcExpanded))
        
    makoHtmlPathExpanded = makoHtmlTemplateName
    if makoHtmlPathExpanded.startswith('~'):
        makoHtmlPathExpanded = os.path.expanduser(makoHtmlPathExpanded)
    makoHtmlPathExpanded = os.path.abspath(makoHtmlPathExpanded)
    logging.info("DQS Statistics HTML MAKO template file: %s" % makoHtmlPathExpanded)
    if not os.path.exists(makoHtmlPathExpanded):
        logging.error("MAKO template file for HTML output does NOT exist: %s" % makoHtmlPathExpanded)
        successful = False
        return successful, err
        
    makoJdbcPathExpanded = makoJdbcTemplateName
    if makoJdbcPathExpanded.startswith('~'):
        makoJdbcPathExpanded = os.path.expanduser(makoJdbcPathExpanded)
    makoJdbcPathExpanded = os.path.abspath(makoJdbcPathExpanded)
    logging.info("DQS Statistics JDBC MAKO template file: %s" % makoJdbcPathExpanded)
    if not os.path.exists(makoJdbcPathExpanded):
        logging.error("MAKO template file for JDBC output does NOT exist: %s" % makoJdbcPathExpanded)
        successful = False
        return successful, err
        
    colNames = []

    logging.info('Accept columns: %s' % acceptColNames)
    logging.info('Unique columns: %s' % uniqueColNames)
    logging.info('Ignore columns: %s' % ignoreColNames)
    
    # derive the columns for which NO value frequencies are to be calculated    
    bypassColNames = list(set(uniqueColNames)|set(ignoreColNames))
    logging.info("Bypass value frequency processing for columns: %s" % bypassColNames)
       
    # open the source file for reading
    srcFile = codecs.open(srcPathExpanded, 'r', 'cp1252')
    csvReader = csv.reader(srcFile, delimiter=srcDelim, quoting=srcQuote)

    bgnTime = time.time()
    
    fileRows = 0
    dataRows = 0
    for rowData in csvReader:
        fileRows += 1
        if fileRows == 1:
            colNames = analyzeHead(rowData, colNames)
        else:
            dataRows += 1
            analyzeData(rowData, colNames, acceptColNames, bypassColNames, fileRows, dataRows)
        if maxRows > 0 and dataRows >= maxRows:
            break
        if dataRows > 0 and dataRows % flushCount == 0:
            endTime = time.time()
            seconds = endTime - bgnTime
            if seconds > 0:
                rcdsPerSec = dataRows / seconds
            else:
                rcdsPerSec = 0
            logging.info("Read {:,} data rows in {:,.0f} seconds @ {:,.0f} records/second".format(dataRows, seconds, rcdsPerSec))
        # if maximum column count mismatches value exceeded
        if maxColCountMisMatches > 0 and len(colCountMisMatches) >= maxColCountMisMatches:
            # cease further processing
            logging.error("Processing terminated due to the number of column count mismatches.")
            break
            
    del csvReader
    srcFile.close()

    endTime = time.time()
    seconds = endTime - bgnTime
    if seconds > 0:
        rcdsPerSec = dataRows / seconds
    else:
        rcdsPerSec = 0
        
    logging.info('')
    logging.info("Read {:,} data rows in {:,.0f} seconds @ {:,.0f} records/second".format(dataRows, seconds, rcdsPerSec))
    
    # column-by-column
    for colName in colNames:
        # if there were
        # row of data found
        if dataRows > 0:
            # calculate the average width
            avgWidths[colName] = (totWidths[colName] * 1.0) / (dataRows * 1.0)
            # calculate the coverage percent
            cvgPrcnts[colName] = (nonBlanks[colName] * 1.0) / (dataRows * 1.0)
        else:
            avgWidths[colName] = 0.0
            cvgPrcnts[colName] = 0.0

    # column-by-column sort the value frequencies            
    frqValueAscs = collections.OrderedDict()
    for colName in colNames:
        frqValueAscs[colName] = {}
        # bypass columns with unprocessed columns
        # since no value frequencies were tracked for them
        if not colName in bypassColNames:
            frqValueAscs[colName] = sorted(frqValues[colName].items(), key=lambda x:x[0])

    # column-by-column, sort the width frequencies            
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
        if not colName in bypassColNames:
            if maxHtmlCount > 0:
                valueFreqs[colName]['frqValValAsc'] = heapq.nsmallest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[0])
                valueFreqs[colName]['frqValFrqAsc'] = heapq.nsmallest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[1])
                valueFreqs[colName]['frqValFrqDsc'] = heapq.nlargest(maxHtmlCount, frqValues[colName].items(), key=lambda x:x[1])
            else:
                valueFreqs[colName]['frqValValAsc'] = sorted(frqValues[colName].items(), key=lambda x:x[0])
                valueFreqs[colName]['frqValFrqAsc'] = sorted(frqValues[colName].items(), key=lambda x:x[1])
                valueFreqs[colName]['frqValFrqDsc'] = sorted(frqValues[colName].items(), key=lambda x:x[1], reverse=True)

    htmlWriter = codecs.open(tgtDqsStatsHtmlExpanded, 'w', 'cp1252')
                    
    makoHtmlTemplate = Template(filename=makoHtmlPathExpanded)
    buffer = StringIO()
    attrs = {}
    parms = {
        'attrs':attrs,
        'dataProvider': dataProvider,
        'executorName': executorName,
        'runDate': runDate,
        'srcPathExpanded':srcPathExpanded,
        'srcPathBaseName':os.path.basename(srcPathExpanded),
        'srcDelim':srcDelim,
        'srcHeaderRows':srcHeaderRows,
        'maxRows':maxRows,
        'maxHtmlCount':maxHtmlCount,
        'maxJdbcCount':maxJdbcCount,
        'tgtDqsStatsJdbcExpanded':tgtDqsStatsJdbcExpanded,
        'inputRows':dataRows,
        'inputCols':len(colNames),
        'colNames':colNames,
        'acceptColNames':acceptColNames,
        'ignoreColNames':ignoreColNames,
        'uniqueColNames':uniqueColNames,
        'nonBlanks':nonBlanks,
        'valueFreqs':valueFreqs,
        'minWidths':minWidths,
        'maxWidths':maxWidths,
        'avgWidths':avgWidths,
        'frqValueAscs':frqValueAscs,
        'frqWidthAscs':frqWidthAscs,
        'colCountMisMatches':colCountMisMatches
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
        # don't sort unprocessed columns as no
        # value frequencies were calculated for them
        if not colName in bypassColNames:
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
                    
    makoJdbcTemplate = Template(filename=makoJdbcPathExpanded)
    buffer = StringIO()
    attrs = {}
    parms = {
        'attrs':attrs,
        'dataProvider': dataProvider,
        'executorName': executorName,
        'runDate': runDate,
        'srcPathExpanded':srcPathExpanded,
        'srcPathBaseName':os.path.basename(srcPathExpanded),
        'srcDelim':srcDelim,
        'srcHeaderRows':srcHeaderRows,
        'maxRows':maxRows,
        'maxHtmlCount':maxHtmlCount,
        'maxJdbcCount':maxJdbcCount,
        'tgtDqsStatsHtmlExpanded':tgtDqsStatsHtmlExpanded,
        'tgtDqsStatsJdbcExpanded':tgtDqsStatsJdbcExpanded,
        'inputRows':dataRows,
        'inputCols':len(colNames),
        'colNames':colNames,
        'acceptColNames':acceptColNames,
        'ignoreColNames':ignoreColNames,
        'uniqueColNames':uniqueColNames,
        'nonBlanks':nonBlanks,
        'valueFreqs':valueFreqs,
        'minWidths':minWidths,
        'maxWidths':maxWidths,
        'avgWidths':avgWidths,
        'frqValueAscs':frqValueAscs,
        'frqWidthAscs':frqWidthAscs,
        'colCountMisMatches':colCountMisMatches,
        'jdbcDropTableIfExistsCompliant': True # True for SQLite databases
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
                logging.error(sqlCmd)
                logging.error(str(e))
                break;
            sqlCmd = ''
    
    sqliteConn.commit()
    sqliteConn.close()
        
    # push statistic records to traditional database

    # if jdbcType not found
    # default to PostgreSQL
    jdbcConn = None
    if jdbcType.lower() == 'mysql':
        try:
            jdbcConn = mysql.connector.connect(**jdbcParms)
        except Exception as e:
            logging.error('Failed to connect to MySQL database')
            logging.error(jdbcParms)
            logging.error(str(e))
    elif jdbcType.lower() == 'mssql':
        try:
            jdbcConn = pymssql.connect(**jdbcParms)
        except Exception as e:
            logging.error('Failed to connect to MySQL database')
            logging.error(jdbcParms)
            logging.error(str(e))
    else:
        try:
            jdbcConn = psycopg2.connect(**jdbcParms)
        except Exception as e:
            logging.error('Failed to connect to MySQL database')
            logging.error(jdbcParms)
            logging.error(str(e))
    jdbcCursor = jdbcConn.cursor()
                    
    makoJdbcTemplate = Template(filename=makoJdbcPathExpanded)
    buffer = StringIO()
    attrs = {}
    parms = {
        'attrs':attrs,
        'dataProvider': dataProvider,
        'executorName': executorName,
        'runDate': runDate,
        'srcPathExpanded':srcPathExpanded,
        'srcPathBaseName':os.path.basename(srcPathExpanded),
        'srcDelim':srcDelim,
        'srcHeaderRows':srcHeaderRows,
        'maxRows':maxRows,
        'maxHtmlCount':maxHtmlCount,
        'maxJdbcCount':maxJdbcCount,
        'tgtDqsStatsHtmlExpanded':tgtDqsStatsHtmlExpanded,
        'tgtDqsStatsJdbcExpanded':tgtDqsStatsJdbcExpanded,
        'inputRows':dataRows,
        'inputCols':len(colNames),
        'colNames':colNames,
        'acceptColNames':acceptColNames,
        'ignoreColNames':ignoreColNames,
        'uniqueColNames':uniqueColNames,
        'nonBlanks':nonBlanks,
        'valueFreqs':valueFreqs,
        'minWidths':minWidths,
        'maxWidths':maxWidths,
        'avgWidths':avgWidths,
        'frqValueAscs':frqValueAscs,
        'frqWidthAscs':frqWidthAscs,
        'colCountMisMatches':colCountMisMatches,
        'jdbcDropTableIfExistsCompliant': jdbcDropTableIfExistsCompliant # True for PostgreSQL and MySQL databases, False for Sql Server
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
                logging.error(sqlCmd)
                logging.error(str(e))
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

def analyzeHead(rowCells, colNames):
    for colName in rowCells:
        colNames.append(colName)
        colUniqs[colName] = {}
        frqValues[colName] = {}
        frqWidths[colName] = {}
        minWidths[colName] = sys.maxsize
        maxWidths[colName] = 0
        totWidths[colName] = 0
        avgWidths[colName] = 0.0
        nonBlanks[colName] = 0
        cvgPrcnts[colName] = 0.0
    return colNames
    
def analyzeData(rowCells, colNames, acceptColNames, bypassColNames, fileRow, dataRow):
    cells = 0
    # only evaluate rows with
    # expected number of columns
    if len(rowCells) == len(colNames): 
        for cellValue in rowCells:
            colName = colNames[cells]
            # either process ALL or just the specified columns
            if len(acceptColNames) == 0 or colName in acceptColNames:
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
                if not colName in bypassColNames:
                    try:
                        frqValues[colName][value] += 1
                    except:
                        frqValues[colName][value] = 1
                else:
                    frqValues[colName] = {}
            # increment column index
            cells += 1
    else:
        colCountMisMatches.append({'fileRow':fileRow, 'dataRow': dataRow, 'nbrCols':len(rowCells)})
    return
    
# ============================================================================
# execute the mainline processing routine
# ============================================================================

if (__name__ == "__main__"):
    retval = main()
