% if jdbcDropTableIfExistsCompliant:
DROP TABLE IF EXISTS DqsFileStats;
% else:
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DqsFileStats') DROP TABLE DqsFileStats;
% endif
CREATE TABLE DqsFileStats ( 
    dataProvider VARCHAR(50),
    runDate VARCHAR(50),
    runBy VARCHAR(50),
    srcFileFullPath VARCHAR(255),
    srcFileBaseName VARCHAR(255),
    inputRows INT,
    inputCols INT,
    srcDelim VARCHAR(10),
    srcHeaderRows INT,
    tgtDelim VARCHAR(10),
	apcdSrcIdFmt VARCHAR(50),
	srcIdColName VARCHAR(50),
    apcdSrcIdBgnNbr VARCHAR(50),
    apcdSrcIdEndNbr VARCHAR(50),
    maxRows INT,
    maxHtmlCount INT,
    maxJdbcCount INT,
    dqsHtmlPath VARCHAR(255),  
    dqsJdbcPath VARCHAR(255)  
);
insert into DqsFileStats (
		dataProvider,
		runDate,
		runBy,
		srcFileFullPath,
		srcFileBaseName,
		inputRows,
		inputCols,
		srcDelim,
		srcHeaderRows,
		maxRows,
		maxHtmlCount,
		maxJdbcCount,
		dqsHtmlPath,
		dqsJdbcPath) values(
		'${str.replace(dataProvider,"'","''")}',
		'${str.replace(runDate,"'","''")}',
		'${str.replace(executorName,"'","''")}',
		'${str.replace(srcPathExpanded,"'","''")}',
		'${str.replace(srcPathBaseName,"'","''")}',
		${inputRows},
		${inputCols},
		'${str.replace(srcDelim,"'","''")}',
		${srcHeaderRows},
		${maxRows},
		${maxHtmlCount},
		${maxJdbcCount},
		'${str.replace(tgtDqsStatsHtmlExpanded,"'","''")}',
		'${str.replace(tgtDqsStatsJdbcExpanded,"'","''")}');

% if jdbcDropTableIfExistsCompliant:
<%! import string %>
DROP TABLE IF EXISTS ColCountMisMatches;
% else:
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ColCountMisMatches') DROP TABLE ColCountMisMatches;
% endif
CREATE TABLE ColCountMisMatches ( 
    dataProvider VARCHAR(50),
    runDate VARCHAR(50),
    runBy VARCHAR(50),
    srcFileBaseName VARCHAR(255),
    fileRow INT,
	dataRow INT,
	nbrCols INT
);
% for colCountMisMatch in colCountMisMatches:
insert into ColCountMisMatches (
	dataProvider,
	runDate,runBy,
	srcFileBaseName,
	srcColName,
	fileRow,
	dataRow,
	nbrCols) values(
	'${str.replace(dataProvider,"'","''")}',
	'${str.replace(runDate,"'","''")}',
	'${str.replace(executorName,"'","''")}',
	'${str.replace(srcPathBaseName,"'","''")}',
	${colCountMisMatch['fileRow']},
	${colCountMisMatch['dataRow']},
	${colCountMisMatch['nbrCols']});
% endfor

% if jdbcDropTableIfExistsCompliant:
<%! import string %>
DROP TABLE IF EXISTS DqsMinMaxAvgCvgStats;
% else:
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DqsMinMaxAvgCvgStats') DROP TABLE DqsMinMaxAvgCvgStats;
% endif
CREATE TABLE DqsMinMaxAvgCvgStats ( 
    dataProvider VARCHAR(50),
    runDate VARCHAR(50),
    runBy VARCHAR(50),
    srcFileBaseName VARCHAR(255),
    srcColName VARCHAR(255),
	minWidth INT,
	maxWidth INT,
	avgWidth FLOAT,
    nonBlanks INT,
	cvgPercnt FLOAT
);
% for colName in colNames:
% if len(acceptColNames) == 0 or colName in acceptColNames:
insert into DqsMinMaxAvgCvgStats (
	dataProvider,
	runDate,runBy,
	srcFileBaseName,
	srcColName,
	minWidth,
	maxWidth,
	avgWidth,
	nonBlanks,
	cvgPercnt) values(
	'${str.replace(dataProvider,"'","''")}',
	'${str.replace(runDate,"'","''")}',
	'${str.replace(executorName,"'","''")}',
	'${str.replace(srcPathBaseName,"'","''")}',
	'${str.replace(colName,"'","''")}',
	${minWidths[colName]},
	${maxWidths[colName]},
	${avgWidths[colName]},
	${nonBlanks[colName]},
	% if inputRows > 0:
	${nonBlanks[colName]*100.0/inputRows*1.0});
	% else:
	0.00);
	% endif
% endif
% endfor

% if jdbcDropTableIfExistsCompliant:
DROP TABLE IF EXISTS DqsValueFreqs;
% else:
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DqsValueFreqs') DROP TABLE DqsValueFreqs;
% endif
CREATE TABLE DqsValueFreqs ( 
    dataProvider VARCHAR(50),
    runDate VARCHAR(50),
    runBy VARCHAR(50),
    srcFileBaseName VARCHAR(255),
    srcColName VARCHAR(255),
    valValue VARCHAR(255),
    valCount INT,
    valValPctOfTotal FLOAT,
    valMaxValue VARCHAR(255),
    valMaxCount INT,
    valMaxPctOfTotal FLOAT,
    valMinValue VARCHAR(255),
    valMinCount INT,
    valMinPctOfTotal FLOAT  
);
% for colName in colNames:
% if len(acceptColNames) == 0 or colName in acceptColNames:
<%
 attrs['maxVals'] = len(valueFreqs[colName]['frqValValAsc'])
%>
% for i in range(attrs['maxVals']):
insert into DqsValueFreqs (
	dataProvider,
	runDate,
	runBy,
	srcFileBaseName,
	srcColName,
	valValue,
	valCount,
	valValPctOfTotal,
	valMaxValue,
	valMaxCount,
	valMaxPctOfTotal,
	valMinValue,
	valMinCount,
	valMinPctOfTotal) values(
	'${str.replace(dataProvider,"'","''")}',
	'${str.replace(runDate,"'","''")}',
	'${str.replace(executorName,"'","''")}',
	'${str.replace(srcPathBaseName,"'","''")}',
	'${str.replace(colName,"'","''")}',
	'${str.replace(valueFreqs[colName]['frqValValAsc'][i][0],"'","''")}',
	${valueFreqs[colName]['frqValValAsc'][i][1]},
	% if inputRows > 0:
	${valueFreqs[colName]['frqValValAsc'][i][1]*100.0/inputRows*1.0},
	% else:
	0.00,
	% endif
	'${str.replace(valueFreqs[colName]['frqValFrqDsc'][i][0],"'","''")}',
	${valueFreqs[colName]['frqValFrqDsc'][i][1]},
	% if inputRows > 0:
	${valueFreqs[colName]['frqValFrqDsc'][i][1]*100.0/inputRows*1.0},
	% else:
	0.00,
	% endif
	'${str.replace(valueFreqs[colName]['frqValFrqAsc'][i][0],"'","''")}',
	${valueFreqs[colName]['frqValFrqAsc'][i][1]},
	% if inputRows > 0:
	${valueFreqs[colName]['frqValFrqAsc'][i][1]*100.0/inputRows*1.0});
	% else:
	0.00);
	% endif
% endfor
% endif
% endfor

% if jdbcDropTableIfExistsCompliant:
DROP TABLE IF EXISTS DqsWidthFreqs;
% else:
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DqsWidthFreqs') DROP TABLE DqsWidthFreqs;
% endif
CREATE TABLE DqsWidthFreqs ( 
    dataProvider VARCHAR(50),
    runDate VARCHAR(50),
    runBy VARCHAR(50),
    srcFileBaseName VARCHAR(255),
    srcColName VARCHAR(255),
    widthValue INT,
    widthCount INT,
    widthPctOfTotal FLOAT  
);
% for colName in colNames:
% if len(acceptColNames) == 0 or colName in acceptColNames:
% for frqWidth in frqWidthAscs[colName]:
insert into DqsWidthFreqs (
	dataProvider,
	runDate,
	runBy,
	srcFileBaseName,
	srcColName,
	widthValue,
	widthCount,
	widthPctOfTotal) values(
	'${str.replace(dataProvider,"'","''")}',
	'${str.replace(runDate,"'","''")}',
	'${str.replace(executorName,"'","''")}',
	'${str.replace(srcPathBaseName,"'","''")}',
	'${str.replace(colName,"'","''")}',
	${frqWidth[0]},
	${frqWidth[1]},
	% if inputRows > 0:
	${frqWidth[1]*100.0/inputRows*1.0});
    % else:
	0.00);
    % endif
% endfor
% endif
% endfor
