import arcpy 
import os
import logging 
from getpass import getpass

import BHS_Local_Check_Merge_ARC as local 
import BHS_SPI_Check_Merge as spi
import BHS_Tracker as tracker

logging.basicConfig(level=logging.DEBUG)

#variables
spi_flag=arcpy.GetParameterAsText(0)
username= arcpy.GetParameterAsText(1)
password =arcpy.GetParameterAsText(2)
host_nm= arcpy.GetParameterAsText(3)
service_nm=arcpy.GetParameterAsText(4)
consolidated_gdb_loc=arcpy.GetParameterAsText(5)
tracker_location=arcpy.GetParameterAsText(6)


#input data, value to dictionary must be either Survey or Telemetry
#may have to be split up as get aprameter text balh and then put into dicts --------will have to change loop below
#maybe even as a singel dict then have to re-format the loops but would get rid of one loop? 
inp_1= arcpy.GetParameterAsText(7)
type_1=arcpy.GetParameterAsText(8)
inp_2=arcpy.GetParameterAsText(9)
type_2=arcpy.GetParameterAsText(10)
inp_3=arcpy.GetParameterAsText(11)
type_3=arcpy.GetParameterAsText(12)
inp_4=arcpy.GetParameterAsText(13)
type_4=arcpy.GetParameterAsText(14)



new_fcs={inp_1:type_1, inp_2:type_2, inp_3:type_3, inp_4:type_4}


temp_gdb_loc=r"T:\bhs_test\BHS_Temp.gdb"


if spi_flag == True:
    logging.debug('starting SPI')
    arcpy.AddMessage('Checking SPI for new feature....')
    spi_instance=spi.spi_check_merge(username, password, host_nm, service_nm, consolidated_gdb_loc, temp_gdb_loc, tracker_location)
    spi_instance.set_up_reqs(temp_gdb_loc)
    spi_instance.oracle_connect(username, password,host_nm, service_nm)
    spi_instance.excute_quries()
    spi_instance.update_fc_from_spi(consolidated_gdb_loc)
                                          
if new_fcs is not None:
    logging.debug('start local FC')
    arcpy.AddMessage('Checking input feature calsses for new feature....')
    local_instance = local.local_fc(consolidated_gdb_loc, temp_gdb_loc, new_fcs, username, tracker_location)
    local_instance.initialize_l()
    local_instance.check_merge(new_fcs)
