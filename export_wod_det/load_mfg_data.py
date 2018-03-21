import csv
import time
import pyodbc
import sys
import os
from datetime import datetime

def convert_file(input_file, output_file, struct_1c_file, struct_mfg_file):
    struct_1c = get_1c_struct(struct_1c_file)
    struct_mfg = get_mfg_struct(struct_mfg_file)
    with open(input_file, newline='') as csvfile_source:
        with open(output_file, 'w', newline='', encoding = "utf-8") as csvfile_dest:
            reader = csv.reader(csvfile_source, dialect='unix', delimiter = ' ')
            writer = csv.writer(csvfile_dest, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            #i = 0
            for row in reader:
                #i += 1
                #if i >= 100: 
                #    break
                #elif i >= 798007:
                #else:
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
    elif type_info == "DateTime":
        if value == "?":
            return "01/01/01"
        else:
            return '/'.join([x for x in value.split("/")[:2]]) + '/40' + value.split("/")[2]  
    elif value != "?":
        return value
    elif type_info in ("Double", "Long", "Short"):
        return "0"
    else:
        return value

def get_1c_struct(file_name):
    result = []
    with open(file_name, encoding = "utf-8") as struct_file:
        for row in struct_file:
            result.append(row.split("\t")[1].strip())
    return result

def get_mfg_struct(struct_mfg_file):
    result = []
    with open(struct_mfg_file) as struct_file:
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
    """
    sql = sql.format(db_name, table_name)
    crsr.execute(sql)
    cnxn.commit()
    
    crsr.close()
    cnxn.close()

def check_tables():
    fields = []
    with open('1c_struct', encoding = "utf-8") as struct_file:
        for row in struct_file:
            fields.append('[_' + row.split("\t")[0].strip() + ']')
        
    key_fields = ['[_Fld172]', '[_Fld4499]', '[_Fld4500]', '[_Fld4501]']
    
    sql = 'SELECT' + '\n'
    
    for field in fields:
        sql += 't1.' + field + ', t2.' + field + ',\n'
        sql += 'CASE\n'
        sql += '\tWHEN t1.' + field + ' = t2.' + field + ' THEN 1\n\tELSE 0\nEND,\n'
    sql = sql[0:len(sql) - 2]
    sql += '\n'
    
    sql += 'FROM [MFGPROExchange].[dbo].[_InfoRg4498] AS t1\n'
    sql += 'INNER JOIN [MFGPROExchange].[dbo].[wod_det_tmp] AS t2\n'
    
    sql += 'ON\n t1.' + key_fields[0] + ' = t2.' + key_fields[0] + '\n'
    
    for key_field in key_fields[1:]:
        sql += 'AND t1.' + key_field + ' = t2.' + key_field + '\n'
        
    sql += "WHERE t1." + fields[0] + ' <> ' + 't2.' + fields[0] + '\n'
    
    for field in fields[1:]:
        sql += 'OR t1.' + field + ' <> t2.' + field + '\n'
        
    return sql 
    
if __name__ == "__main__":
    
    db_name = sys.argv[1]
    table_name = sys.argv[2]
    input_file = sys.argv[3]
    struct_1c_file = sys.argv[4]
    struct_mfg_file = sys.argv[5]
    output_file = input_file + '_dest'
    
    print("begin conversion: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
    convert_file(input_file, output_file, struct_1c_file, struct_mfg_file)
    print("end conversion: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
    print("begin bulk insert: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
    bulk_insert(output_file, db_name, table_name)
    print("end bulk insert: " + datetime.strftime(datetime.now(), "%Y.%m.%d %H:%M:%S"))
    os.remove(output_file)
    
