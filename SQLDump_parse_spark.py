#!/usr/bin/python
import fileinput
import csv
import sys
import os
import shutil

from pyspark import SparkConf, SparkContext

reload(sys)
sys.setdefaultencoding('utf-8')

# This prevents prematurely closed pipes from raising
# an exception in Python
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)


def get_values(line):
    """
    Returns the portion of an INSERT statement containing values
    """
    return line.partition('` VALUES ')[2]

def get_tablenames(line):
    """
    Returns the tablename of an INSERT statement
    """
    return line.partition('` VALUES ')[0].partition('INSERT INTO `')[2]



def values_sanity_check(values):
    """
    Ensures that values from the INSERT statement meet basic checks.
    """
    assert values
    assert values[0] == '('
    # Assertions have not been raised
    return True

def parse_values(values, table_name):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """
    latest_row = []
    outputdata= []
    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
    )
    for reader_row in reader:
        for column in reader_row:
            # If our current string is empty...
            if len(column) == 0 or column == 'NULL':
                latest_row.append('')
                continue
            # If our string starts with an open paren
            if column[0] == "(":
                # Assume that this column does not begin
                # a new row.
                new_row = False
                # If we've been filling out a row
                if len(latest_row) > 0:
                    # Check if the previous entry ended in
                    # a close paren. If so, the row we've
                    # been filling out has been COMPLETED
                    # as:
                    #    1) the previous entry ended in a )
                    #    2) the current entry starts with a (
                    if latest_row[-1][-1] == ")":
                        # Remove the close paren.
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                # If we've found a new row, write it out
                # and begin our new one
                if new_row:
                    #writer.writerow(latest_row)
                    latest_row_str=",".join(latest_row)
                    outputdata.append((table_name,latest_row_str))
                    latest_row = []
                # If we're beginning a new row, eliminate the
                # opening parentheses.
                if len(latest_row) == 0:
                    column = column[1:]
            # Add our column to the row we're working on.
            #latest_row.append(column.decode().replace('NULL','').replace('\x01','1').replace('\x00','0').replace(',','').replace('\'',''))
            latest_row.append(column)
        # At the end of an INSERT statement, we'll
        # have the semicolon.
        # Make sure to remove the semicolon and
        # the close paren.
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            #writer.writerow(latest_row)
            latest_row_str=",".join(latest_row)
            outputdata.append((table_name,latest_row_str))
    return outputdata

def parse(line):
       values = get_values(line)
       table_name=get_tablenames(line)
       if values_sanity_check(values):
           #values1=unicode(values.strip(codecs.BOM_UTF8), 'utf-8-sig')
           values1=values.encode('ascii', 'ignore').decode('utf-8')
           output=parse_values(values1,table_name)
           #output=(values,table_name)
       return output

if __name__ == "__main__":
  
  conf = SparkConf().setMaster("local").setAppName("My App")
  sc = SparkContext(conf = conf)

  #log_txt = sc.textFile("/apps/hdmi-technology/b_clsfd/tract/test.sql",use_unicode=False)
  log_txt = sc.textFile("/apps/hdmi-technology/b_clsfd/tract/2017_09_04_eu2_tract.sql")  ,use_unicode=False)
  data_txt= log_txt.filter(lambda x: x.startswith('INSERT INTO')).flatMap(parse).count()

  output_df = data_txt.toDF(["table_name", "backend_data"])
  output_df.write.partitionBy("table_name").text("/apps/hdmi-technology/b_clsfd/tract/dev/output")

  sc.stop()


Two issue meet:
	1. UnicodeEncodeError: 'ascii' codec can't encode characters in position 147-150: ordinal not in range(128)
	2. values1=values.encode('ascii', 'ignore').decode('utf-8') "Error: field larger than field limit (131072)"



	
