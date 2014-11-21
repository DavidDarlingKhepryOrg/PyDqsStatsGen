<%! import html %>
<html>
	<style>
		
		body {
			font-family: arial, courier, "Times New Roman";
		}
		h3 {
			font-family: inherit;
			text-align: center;
		}
		table,tr,th,td
		{
			border-style: solid;
			border-width: 1px;
			font-family: inherit;
			margin: auto;
		}
		.numeric
		{
			font-family: inherit;
			text-align: right;
		}
		.colStatsHdrLvl1
		{
			font-family: inherit;
			font-size: large;
			text-align: center;
		}
		.colStatsHdrLvl2
		{
			font-family: inherit;
			font-size: small;
			text-align: center;
		}
		.colStatsHdrLvl3
		{
			font-family: inherit;
			font-size: inherit;
			text-align: center;
		}
		.exeBlockCnt
		{
			background-color: wheat;
			font-family: inherit;
			text-align: center;
		}
		.leftBlockValCnt
		{
			background-color: wheat;
			font-family: inherit;
			text-align: center;
		}
		.leftBlockValLft
		{
			background-color: wheat;
			font-family: inherit;
		}
		.leftBlockValRgt
		{
			background-color: wheat;
			font-family: inherit;
			text-align: right;
		}
		.midlBlockValLft
		{
			background-color: lemonchiffon;
			font-family: inherit;
		}
		.midlBlockValRgt
		{
			background-color: lemonchiffon;
			font-family: inherit;
			text-align: right;
		}
		.onlyBlockVal
		{
			background-color: lemonchiffon;
			font-family: inherit;
		}
		.riteBlockValLft
		{
			background-color: lavender;
			font-family: inherit;
		}
		.riteBlockValRgt
		{
			background-color: lavender;
			font-family: inherit;
			text-align: right;
		}
		.onlyBlockLen
		{
			background-color: lavender;
			font-family: inherit;
		}
	</style>
	<body>

		<h3>
			DqsValidator ACHI APCD Data Quality Report (version 1.0)
		</h3>		
		<table style="border: none;">
			<tr>
				<th style="border: none;">Source:</th>
				<td style="border: none;">${html.escape(dataProvider)}</td>
			</tr>
			<tr>
				<th style="border: none;">Run by:</th>
				<td style="border: none;">${html.escape(executorName)}</td>
			</tr>
			<tr>
				<th style="border: none;">Run date:</th>
				<td style="border: none;">${html.escape(runDate)}</td>
			</tr>
		</table>
		<p />
		<table>
			<tr>
				<th colspan="2">Execution Parameters</th>
			</tr>
			<tr>
				<th>Source File</th>
				<td class="exeBlockCnt">${html.escape(srcPathExpanded)}</td>
			</tr>
			<tr>
				<th>Input Records</th>
				<td class="exeBlockCnt">${inputRows}</td>
			</tr>
			<tr>
				<th>Input Columns</th>
				<td class="exeBlockCnt">${inputCols}</td>
			</tr>
			<tr>
				<th>Source File Delimiter</th>
				<td class="exeBlockCnt">${html.escape(srcDelim)}</td>
			</tr>
			<tr>
				<th>Source Header Rows</th>
				<td class="exeBlockCnt">${srcHeaderRows}</td>
			</tr>
			<tr>
				<th>Target File Delimiter</th>
				<td class="exeBlockCnt">${html.escape(tgtDelim)}</td>
			</tr>
			<tr>
				<th>APCD_SOURCE_ID Format</th>
				<td class="exeBlockCnt">${html.escape(apcdSrcIdFmt)}</td>
			</tr>
			<tr>
				<th>Source ID Column Name</th>
				<td class="exeBlockCnt">${html.escape(srcIdColName)}</td>
			</tr>
			<tr>
				<th>APCD_SOURCE_ID Beginning Number</th>
				<td class="exeBlockCnt">${apcdSrcIdBgnNbr}</td>
			</tr>
			<tr>
				<th>APCD_SOURCE_ID Ending Number</th>
				<td class="exeBlockCnt">${apcdSrcIdEndNbr}</td>
			</tr>
			<tr>
				<th>Max Records</th>
				<td class="exeBlockCnt">${maxRows}</td>
			</tr>
			<tr>
				<th>Max HTML Count to Report</th>
				<td class="exeBlockCnt">${maxHtmlCount}</td>
			</tr>
			<tr>
				<th>Max JDBC Count to Report</th>
				<td class="exeBlockCnt">${maxJdbcCount}</td>
			</tr>
			<tr>
				<th>Max Width Count to Report</th>
				<td class="exeBlockCnt">${maxHtmlCount}</td>
			</tr>
			<tr>
				<th>DQS Report File</th>
				<td class="exeBlockCnt">${html.escape(tgtDqsFileNameExpanded)}</td>
			</tr>
		</table>

		<hr />
		<hr />

		<p />
		<table>
			<tr>
				<th colspan="3">Source File Column Coverage</th>
			</tr>
			<tr>
				<th>Column Name</th>
				<th>MinWidth</th>
				<th>MaxWidth</th>
				<th>AvgWidth</th>
				<th>Non-Blanks</th>
				<th>Coverage Percent</th>
			</tr>
			
			% for colName in colNames:
			
			<tr class="onlyBlockVal">
				<th>${html.escape(colName)}</th>
				<td class="numeric">${minWidths[colName]}</td>
				<td class="numeric">${maxWidths[colName]}</td>
				<td class="numeric">${'{0:.1f}'.format(avgWidths[colName])}</td>
				<td class="numeric">${nonBlanks[colName]}</td>
				<td class="numeric">${'{0:.2%}'.format(nonBlanks[colName]/inputRows)}</td>
			</tr>
			
			% endfor
			
		</table>

		<hr />
		<hr />
		
		<p />
		<table>
			<tr>
				<th colspan="9">Source File Column Value Frequencies</th>
			</tr>
			<tr>
				<th colspan="9">${html.escape(runDate)}</th>
			</tr>
		</table>
		
		% for colName in colNames:
		<p />
		<table>
			<tr class="colStatsHdrLvl1">
				<th colspan="9">${html.escape(colName)} Values by Freq (desc), Value (asc), Value (desc)</th>
			</tr>
			<tr class="colStatsHdrLvl2">
				<th colspan="3">Values by Value (asc)</th>
				<th colspan="3">Values by Freq (desc)</th>
				<th colspan="3">Values by Freq (asc)</th>
			</tr>
			<tr class="colStatsHdrLvl3">
				<th>Value</th>
				<th>Count</th>
				<th>% of Total</th>
				<th>Value</th>
				<th>Count</th>
				<th>% of Total</th>
				<th>Value</th>
				<th>Count</th>
				<th>% of Total</th>
			</tr>

			% if colName in uniqueColNames:
			<tr class="leftBlockValLft">
				<td colspan="9">No results were calculated, as column was specified as having all unique values</td>
			</tr>
			% else:

                  <%
                     attrs['maxVals'] = len(valueFreqs[colName]['frqValValAsc'])
                  %>
			
                  % for i in range(attrs['maxVals']):
			
	      		<tr>
    				<th class="leftBlockValLft">${html.escape(valueFreqs[colName]['frqValValAsc'][i][0])}</th>
    				<td class="leftBlockValRgt">${valueFreqs[colName]['frqValValAsc'][i][1]}</td>
                          % if nonBlanks[colName] > 0:
            				<td class="leftBlockValRgt">${'{0:.2%}'.format(valueFreqs[colName]['frqValValAsc'][i][1]/inputRows)}</td>
                          % else:
            				<td class="leftBlockValRgt">0.00%</td>
                          % endif
    				<th class="leftBlockValLft">${html.escape(valueFreqs[colName]['frqValFrqDsc'][i][0])}</th>
    				<td class="leftBlockValRgt">${valueFreqs[colName]['frqValFrqDsc'][i][1]}</td>
                          % if nonBlanks[colName] > 0:
           				<td class="leftBlockValRgt">${'{0:.2%}'.format(valueFreqs[colName]['frqValFrqDsc'][i][1]/inputRows)}</td>
                          % else:
            				<td class="leftBlockValRgt">0.00%</td>
                          % endif
       				<th class="leftBlockValLft">${html.escape(valueFreqs[colName]['frqValFrqAsc'][i][0])}</th>
    				<td class="leftBlockValRgt">${valueFreqs[colName]['frqValFrqAsc'][i][1]}</td>
                          % if nonBlanks[colName] > 0:
          				<td class="leftBlockValRgt">${'{0:.2%}'.format(valueFreqs[colName]['frqValFrqAsc'][i][1]/inputRows)}</td>
                          % else:
           				<td class="leftBlockValRgt">0.00%</td>
                          % endif
	    		</tr>
	      		% endfor
			
			% endif
		</table>
		% endfor

		<hr />
		<hr />
		
		% for colName in colNames:
		<p />
		<table>
			<tr>
				<th colspan="9">${html.escape(colName)} Width by Freq</th>
			</tr>
			<tr>
				<th>Width</th>
				<th>Count</th>
				<th>% of Total</th>
			</tr>
			% for frqWidth in frqWidthAscs[colName]:
			
			<tr class="onlyBlockLen">
				<th>${frqWidth[0]}</th>
				<td class="numeric">${frqWidth[1]}</td>
                        % if nonBlanks[colName] > 0:
				<td class="numeric">${'{0:.2%}'.format(frqWidth[1]/inputRows)}</td>
                        % else:
				<td class="numeric">0.00%</td>
                        % endif
			</tr>
			% endfor
		</table>
		% endfor
				
			
	</body>
</html>
