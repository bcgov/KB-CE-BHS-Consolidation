import arcpy 
import os
import logging 
from getpass import getpass

import BHS_Local_Check_Merge as local 
import BHS_SPI_Check_Merge as spi
import BHS_Tracker as tracker

logging.basicConfig(level=logging.DEBUG)

#variables
username= input("Enter BCGW user name: ")
password =getpass(prompt="Enter BCGW password: ")
host_nm= "bcgw.bcgov"  #=input("enter BCGW host name: ")
service_nm="idwprod1.bcgov"#=input ("enter BCGW Service name: ")
consolidated_gdb_loc=r"T:\bhs_test\BHS_Test_3.gdb"
temp_gdb_loc=r"T:\bhs_test\BHS_Temp.gdb"
tracker_location=r'T:\bhs_test\BHS_Consolidated_Dataset_Tracking.xlsx'


#input data, value to dictionary must be either Survey or Telemetry
inp_1= {r'T:\bhs_test\BHS_collar_shp\EVW_BHS_collar_locations_13Nov23.shp':'Telemetry'}
inp_2={r'T:\bhs_test\merge_test.gdb\EVW_BHS_Collar_Nov_23':'Telemetry'}
inp_3= {None:None}
inp_4={None:None}
#maybe just make the entire thing a dictionary and not a list 
new_fcs=[inp_1, inp_2,inp_3,inp_4]


#flag to run spi check merge
spi_flag=False

if spi_flag == True:
    logging.debug('starting SPI')
    spi_instance=spi.spi_check_merge(username, password, host_nm, service_nm, consolidated_gdb_loc, temp_gdb_loc, tracker_location)
    spi_instance.set_up_reqs(temp_gdb_loc)
    spi_instance.oracle_connect(username, password,host_nm, service_nm)
    spi_instance.excute_quries()
    spi_instance.update_fc_from_spi(consolidated_gdb_loc)
                                          
if new_fcs is not None:
    logging.debug('start local FC')
    local_instance = local.local_fc(consolidated_gdb_loc, temp_gdb_loc, new_fcs, username, tracker_location)
    local_instance.initialize_l()
    local_instance.check_merge(new_fcs)
