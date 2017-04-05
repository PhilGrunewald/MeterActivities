#!/usr/bin/env python
import getopt
import sys
import json
import MySQLdb.cursors
import db_ini as db     # reads the database and file path information
# override host to local
# db.Host='localhost'

# ========= #
#  GLOBALS  #
# ========= #


SQLquery ={
"PDF": "\
        SELECT {0},ROUND\
            (\
            COUNT(*)/\
                (\
                SELECT COUNT(*) as count \
                FROM Household\
                )\
            *100\
            ) as Percent \
        FROM {1} \
        GROUP BY {0};",
"Count": "\
    SELECT Count(*) as result\
    FROM {};"
}

with open('Definitions.json') as f:    
    Definitions = json.load(f)

# Definitions = {
# "Gender": { 
#         "0": "Skip",
#         "1": "Female",
#         "2": "Male",
#         "3": "Other"
#         }
# }
# ========= #
        # \
        # ORDER BY {0};"
# FUNCTIONS #
# ========= #

def connectDB():
    """ use db credentials for MySQLdb """
    dbConnection = MySQLdb.connect(
        host=db.Host,
        user=db.User,
        passwd=db.Pass,
        db=db.Name,
        cursorclass=MySQLdb.cursors.DictCursor)
    return dbConnection.cursor()

def getResults(_query,col):
    """ send sql query and return result as list """
    reverse = False
    resultStr = ''
    if (col in Definitions):
        resultStr += "\n\n{0}\n\n".format(Definitions[col]['q'])
        
    cursor.execute(_query)
    results = cursor.fetchall()
    ks = results[0].keys()
    if (ks[0] != col):
        # make sure the column comes first
        ks = reversed(ks)
        reverse = True
    resultStr += '\t|'.join("{}".format(str(e)) for e in ks)
    resultStr += "\n:--- |---:\n"
        
    for result in results:
        vs = result.values()
        a = 0
        b = 1
        if reverse:
            a = 1
            b = 0
        if (col in Definitions):
            resultStr += "{0}\t|{1}\n".format(Definitions[col][str(vs[int(a)])], vs[b])
        print resultStr
    return resultStr


def getCount(table):
    sqlq = SQLquery['Count'].format(table)
    cursor.execute(sqlq)
    results = cursor.fetchall()
    return results[0]['result']


def getCols(table):
    """ get list of Column names  """
    colNames = []
    sqlq = "SHOW Columns FROM {}".format(table)
    cursor.execute(sqlq)
    results = cursor.fetchall()
    for result in results:
        colNames.append(result['Field'])
    return colNames

def getColPDF(col,table):
    """ list all values and percentage of occurances """
    sqlq = SQLquery['PDF'].format(col,table)
    return getResults(sqlq,col)

def getTablePDFs(table):
    """ go through all cols and return PDF """
    PDF_Str = ""
    for col in getCols(table):
        PDF =  getColPDF(col,table)
        if (PDF.count('\n') < 18):
            # Formatting for markdown table: 2 col - align right
            # inserted after header (i.e after first \n)
            # PDF_table = PDF.replace("\n","\n---: |---:\n",1)
            PDF_Str += PDF
    return PDF_Str

def createCockpit(table,output):
    """ produce summary for table stats """
    if output:
        with open(output,'w') as f:
           f.write("% Table: {}\n".format(table))
           f.write("% Count: {}\n".format(getCount(table)))
           f.write(getTablePDFs(table))

def main(argv):
    """ Check for arguments """

    # Default values
    global width
    width = 0
    output = False
    # table = "Household"
    table = "Individual"

    # Optional arguments
    options = "hlt:w:o:"
    optionsLong = ["help","file"]

    # Help
    helpStr =  "sql.py {} \n\
                options \n\
                [-h,--help]\t\tthis help \n\
                ".format(options)
    result = helpStr

    # Evaluate arguments
    try:
        opts, args = getopt.getopt(argv,options,optionsLong)
    except getopt.GetoptError:
        print helpStr
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print helpStr
            sys.exit()
        elif opt in ("-l", "--localhost"):
            db.Host = 'localhost'
        elif opt in ("-w", "--width"):
            width = arg
        elif opt in ("-t", "--table"):
            table = arg
        elif opt in ("-o", "--output"):
            output = arg


    global cursor
    cursor = connectDB()
    createCockpit(table,output)
    print "Entry complete\n\n"

# ========= #
#  EXECUTE  #
# ========= #
if __name__ == "__main__":
    cursor = connectDB()
    main(sys.argv[1:])

