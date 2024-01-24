import arcpy 
import os
import path
import logging 

import BHS_Local_Check_Merge as local 
import BHS_SPI_Check_Merge as spi
import BHS_Tracker as tracker

logging.basicConfig(level=logging.DEBUG)

#variables
username= input("Enter BCGW user name: ")
password =getpass(prompt="Enter BCGW password: ")
host_nm=input("enter BCGW host name: ")
service_nm=input ("enter BCGW Service name: ")
consolidated_gdb_loc=r"\01_CE\BHS\BHS_Scratch\BHS_Test_3.gdb"
temp_gdb_loc=r"T:\BHS_Temp.gdb"


#input data, value to dictionary must be either Survey or Telemetry
inp_1= {r'T:\bhs_test\BHS_collar_shp\EVW_BHS_collar_locations_13Nov23.shp':'Telemetry'}
inp_2={r'T:\bhs_test\merge_test.gdb\EVW_BHS_Collar_Nov_23':'Telemetry'}
inp_3= {None:None}
inp_4={None:None}
#maybe just make the entire thing a dictionary and not a list 
new_fcs=[inp_1, inp_2,inp_3,inp_4]


#flag to run spi check merge
spi_flag=True

if spi_flag = True:
    logging.debug('starting SPI')
    spi.spi_check_merge(username, password, host_nm, service_nm, consolidated_gdb_loc, temp_gdb_loc)

if new_fcs is not None:
    logging.debug('start local FC')
    local.local_fc(consolidated_gdb_loc, temp_gdb_loc, new_fcs, username)