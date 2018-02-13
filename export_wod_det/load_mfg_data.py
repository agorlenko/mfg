import csv
import time
import pyodbc
import sys
from datetime import datetime

#print(time.time())

def convert_file(input_file, output_file):
    struct_1c = get_1c_struct()
    struct_mfg = get_mfg_struct()
    with open(input_file, newline='') as csvfile_source:
        with open(output_file, 'w', newline='', encoding = "utf-8") as csvfile_dest:
            reader = csv.reader(csvfile_source, dialect='unix', delimiter = ' ')
            writer = csv.writer(csvfile_dest, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            #i = 0
            for row in reader:
                #i += 1
                #if i >= 798011: 
                #    break
                #elif i >= 798007:
                    writer.writerow(normalize_row(row, struct_1c, struct_mfg))
            
def normalize_row(input_row, struct_1c, struct_mfg):            
    result = []
    dict_mfg = {}
    i = 0
    for cur_column in input_row:
        if i < len(struct_mfg) and struct_mfg[i]:
            dict_mfg[struct_mfg[i][0]] = cast_value(cur_column, struct_mfg[i][1])  
        i += 1
    for cur_column in struct_1c:
        if cur_column != "ОбластьДанныхОсновныеДанные":
            result.append(dict_mfg[cur_column])
        else:
            result.append("0")
    #print("input row:")
    #print(input_row)
    #print("result row:")
    #print(result)
    return result

def cast_value(value, type_info):
    if type_info == "Bit":
        if value == "no":
            return "00"
        else:
            return "01"
    #elif type_info == "DateTime":
    #    if value != "?":
    #        return "01/01/01"
        #else:
        #    return "20"+ "".join(x for x in value.split("/")[:: -1])
    elif value != "?":
        return value
    elif type_info == "DateTime":
        return "01/01/01"
    elif type_info in ("Double", "Long", "Short"):
        return "0"
    else:
        return value

#print(time.time())

def get_1c_struct():
    result = []
    with open("1c_struct", encoding = "utf-8") as struct_file:
        for row in struct_file:
            result.append(row.split("\t")[1].strip())
    return result

def get_mfg_struct():
    result = []
    with open("mfg_struct") as struct_file:
        for row in struct_file:
            column_decl = row.split("=")[1]
            delimiter_index = column_decl.find(" ")
            result.append((column_decl[:delimiter_index].strip(), column_decl[delimiter_index + 1:].strip()))
    return result

def bulk_insert(file_name, db_name, table_name):
    cnxn = pyodbc.connect(driver='{SQL Server}', server='(local)', database='{}'.format(db_name),               
               trusted_connection='yes')    
    crsr = cnxn.cursor()
    sql = "TRUNCATE TABLE {0}.[dbo].{1};".format(db_name, table_name)
    crsr.execute(sql)
    cnxn.commit()
    
    sql = """
    SET DATEFORMAT DMY;
    BULK INSERT {0}.[dbo].[{1}]
        FROM '""" + file_name + """'
        WITH (
        DATAFILETYPE = 'char',
        FIELDTERMINATOR = '\t',
        ROWTERMINATOR = '0x0A'
        );
    """.format(db_name, table_name)
    crsr.execute(sql)
    cnxn.commit()
    
    crsr.close()
    cnxn.close()

#print(get_1c_struct())
#print(get_mfg_struct())

if __name__ == "__main__":
    
    #input_file = r'D:\Work\1C\TCS\MFG\bulk\1C_edp_20170427200052_wod_det\1C_edp_20170427200052_wod_det'
    #output_file = r'D:\Work\1C\TCS\MFG\bulk\1C_edp_20170427200052_wod_det\1C_edp_20170427200052_wod_det_dest'
    db_name = sys.argv[1]
    table_name = sys.argv[2]
    input_file = sys.argv[3]
    output_file = input_file + '_dest'
    
    print(db_name, table_name, input_file, output_file)
    
    #print("begin conversion: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
    #convert_file(input_file, output_file)
    #print("end conversion: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
    #print("begin bulk insert: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
    #bulk_insert(output_file, db_name, table_name)
    #print("end bulk insert: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))

