
import logging
import sqlalchemy 
import pip
import sys
import subprocess
try:
    import cx_Oracle
except:
    # pip._internal.main(["install", "cx-oracle"])
    subprocess.check_call(["pip", "install", "cx-oracle"])

from getpass import getpass
import pandas
import arcpy
import os
from datetime import date

import BHS_Tracker


class spi_check_merge: 

    #=========================================================================================================================
    def __init__(self, bcgw_username, bcgw_password, bcgw_host_nm, bcgw_service_nm, consolidated_gdb, temp_gdb, tracker_sheet ):
        #get oracle username and password 
        self.bcgw_username=bcgw_username
        self.bcgw_password= bcgw_password
        self.bcgw_host_nm= bcgw_host_nm
        self.bcgw_service_nm=bcgw_service_nm
        self.consolidated_gdb= consolidated_gdb
        self.temp_gdb= temp_gdb
        logging.basicConfig(level=logging.DEBUG)

        tracker = BHS_Tracker.tracker(tracker_sheet)
        tracker.initialize(tracker_sheet)
    #=========================================================================================================================




    #=========================================================================================================================
    def set_up_reqs(self,temp_gdb):
        #create temp gdb if it does not exist
        if not arcpy.Exists(temp_gdb):
            arcpy.management.CreateFileGDB( os.path.dirname(temp_gdb), "BHS_Temp.gdb ")
        else: 
            logging.debug("gdb exists")
    #=========================================================================================================================




    #=========================================================================================================================
    def oracle_connect(self,username, password,hostnm, srvcnm ):
        #create oracle connection and engine
        dialect="oracle"
        sql_driver="cx_oracle"
        hostname=hostnm
        port=1521
        service_name=srvcnm

        #  Connection string
        oracle_connection_string_fmt = (
            f"oracle+cx_oracle://{username}:{password}@" +
            cx_Oracle.makedsn("{hostname}", "{port}", service_name="{service_name}")
        )
        url = oracle_connection_string_fmt.format(
            username=username, password=password, 
            hostname=hostname, port=port, 
            service_name=service_name,
        )

        engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(url, echo=True)
        self.conn = engine.connect()
        self.metadata=sqlalchemy.MetaData()
    #=========================================================================================================================





    #-----------------MOVE TO ANOTHER SCRIPT---------------------------
    #query observation data set in bounding box (Kootenay Boundary)
    spi_sql_obvs="""
    select 
    b.SURVEY_OBSERVATION_ID as Observation_ID,
    b.Species_Code,
    b.ANIMAL_ID,
    b.SEX,
    b.OBSERVED_NUMBER,
    b.OBSERVATION_DATETIME as Date_Time,
    b.OBSERVATION_YEAR as Year,
    b.OBSERVATION_MONTH as Month,
    b.OBSERVATION_DAY as Day,
    b.INVENTORY_METHOD,
    b.UTM_SOURCE,
    b.UTM_ZONE,
    b.UTM_EASTING,
    b.UTM_Northing,
    b.PROJECT_NAME,
    b.PROJECT_ID,
    b.SURVEY_NAME,
    b.SURVEY_ID,
    SDO_UTIL.TO_WKTGEOMETRY(b.GEOMETRY)AS WKT_GEOMETRY
    from
    WHSE_WILDLIFE_INVENTORY.SPI_SURVEY_OBS_ALL_SP b
    
    where b.species_code = 'M-OVCA'

    and 

    SDO_ANYINTERACT (GEOMETRY,
        SDO_GEOMETRY(2003, 3005, NULL,
            SDO_ELEM_INFO_ARRAY(1,1003,3),
            SDO_ORDINATE_ARRAY(1493647.6,466354.8,1889017.9,862609.6) 
        )
    ) = 'TRUE'
    
    """
    #query telemetry data set in bounding box (kootenay boundary)
    spi_sql_tele="""
    select 
    b.SURVEY_OBSERVATION_ID as Observation_ID,
    b.Species_Code,
    b.ANIMAL_ID,
    b.SEX,
    b.OBSERVED_NUMBER,
    b.OBSERVATION_DATETIME as Date_Time,
    b.OBSERVATION_YEAR as Year,
    b.OBSERVATION_MONTH as Month,
    b.OBSERVATION_DAY as Day,
    b.UTM_ZONE,
    b.UTM_EASTING,
    b.UTM_Northing,
    b.PROJECT_NAME,
    b.PROJECT_ID,
    b.SURVEY_NAME,
    b.SURVEY_ID,
    SDO_UTIL.TO_WKTGEOMETRY(b.GEOMETRY)AS WKT_GEOMETRY
    from
    WHSE_WILDLIFE_INVENTORY.SPI_TELEMETRY_OBS_ALL_SP b
    
    where b.species_code = 'M-OVCA'

    and 

    SDO_ANYINTERACT (GEOMETRY,
        SDO_GEOMETRY(2003, 3005, NULL,
            SDO_ELEM_INFO_ARRAY(1,1003,3),
            SDO_ORDINATE_ARRAY(1493647.6,466354.8,1889017.9,862609.6) 
        )
    ) = 'TRUE'
    
    """
    #-----------------END MOVE TO ANOTHER SCRIPT---------------------------




    #=========================================================================================================================
    def excute_quries(self):
        #execute queries
        obvs_df=pandas.read_sql_query(self.spi_sql_obvs,self.conn)
        tele_df=pandas.read_sql_query(self.spi_sql_tele, self.conn)

        #Merge results into one table 
        frames=[obvs_df,tele_df]
        prefix=[ "Observations ",  "Telemetry "]
        df_list=[]
        # result_df=pandas.concat(frames)
        for q, p in zip(frames, prefix):

            po_w= f"{p}_post_winter "
            
            po_m= f"{p}_post_movement "
            
            pr_w= f"{p}_pre_winter "
        
            pr_m= f"{p}_pre_movement "
            
            
        #split data frame into two, pre and post 1998
            post_1998 = q[q["year"] > 1998]
            pre_1998 = q[q["year"] <= 1998]

            #split pre and post 1998 data frames into pre and post, winter and movment based on dates
            po_w= post_1998.query("month <=3 or month =12" and "day >=15")
            logging.debug(len(po_w.index))
            df_list.append(po_w)
            po_m=post_1998.query("month = 11 or month =12" and "day <15 or month in (4,5)")
            logging.debug(len(po_m.index))
            df_list.append(po_m)
            pr_w= pre_1998.query("month <=3 or month =12" and "day >=15")
            logging.debug(len(pr_w.index))
            df_list.append(pr_w)
            pr_m= pre_1998.query("month = 11 or month =12" and "day <15 or month in (4,5)")
            logging.debug(len(pr_m.index))
            df_list.append(pr_m)

        arcpy.env.workspace=self.temp_gdb

        #names for temp lyrs 
        tmp_lyrs=["Observations_post_winter_Temp", "Observations_post_movement_Temp", "Observations_pre_winter_Temp", "Observations_pre_movement_Temp",
                "Telemetry_post_winter_Temp", "Telemetry_post_movement_Temp", "Telemetry_pre_winter_Temp", "Telemetry_pre_movement_Temp"]
        #create dictonary  of col name:type
        column_types = df_list[1].dtypes.apply(lambda x: x.name).to_dict()

        #create temp fc if they do not exist and add cols to it 
        for t,d in zip(tmp_lyrs,df_list):
            if not arcpy.Exists(t):
                arcpy.management.CreateFeatureclass(self.temp_gdb,
                    t,
                    "POINT",
                    spatial_reference=arcpy.SpatialReference(3005))
                logging.debug(f"{t} created")
            else: 
                logging.debug(f"{t} already exists")
            #add cols from df to fc
            for col in column_types:
                if column_types[col] == "int64" or column_types[col] == "float64":
                    arcpy.management.AddField(t, col, "DOUBLE")
                elif column_types[col] == "object":
                    arcpy.management.AddField(t, col, "TEXT")
                elif column_types[col] == "datetime64[ns]":
                    arcpy.management.AddField(t, col, "DATE")
            #add points from df to fc        
            with arcpy.da.InsertCursor(t, ["SHAPE@"] + list(d.columns)) as cursor:
                for index, row in d.iterrows():
                    # Create a point geometry from WKT
                    point = arcpy.FromWKT(row["wkt_geometry"], arcpy.SpatialReference(3005))
                    row_nm=row.tolist()
                    values = [point] + row_nm
                    cursor.insertRow(values)
        self.conn.close()


    #=========================================================================================================================



    #=========================================================================================================================
    def update_fc_from_spi(self,exsitng_gdb):
        #FCs
        arcpy.env.workspace = exsitng_gdb
        # fc=arcpy.ListFeatureClasses()
        # # logging.debug(fc)

        #eventually remove hard coding
        s_winter_pre=os.path.join(exsitng_gdb, "BHS_Winter_Pre_1998_Survey")
        t_winter_pre=os.path.join(exsitng_gdb, "BHS_Winter_Pre_1998_Telemetry")
        s_winter_post=os.path.join(exsitng_gdb, "BHS_Winter_Post_1998_Survey")
        t_winter_post =os.path.join(exsitng_gdb, "BHS_Winter_Post_1998_Telemetry")
        s_movement_pre =os.path.join(exsitng_gdb, "BHS_Movement_Pre_1998_Survey")
        t_movement_pre =os.path.join(exsitng_gdb, "BHS_Movement_Pre_1998_Telemetry")
        s_movement_post =os.path.join(exsitng_gdb, "BHS_Movement_Post_1998_Survey")
        t_movement_post =os.path.join(exsitng_gdb, "BHS_Movement_Post_1998_Telemetry")

        new_s_winter_post=os.path.join(self.temp_gdb,"Observations_post_winter_Temp")
        new_s_movement_post=os.path.join(self.temp_gdb,"Observations_post_movement_Temp")
        new_s_winter_pre=os.path.join(self.temp_gdb,"Observations_pre_winter_Temp")
        new_s_movement_pre=os.path.join(self.temp_gdb,"Observations_pre_movement_Temp")
        new_t_winter_post=os.path.join(self.temp_gdb,"Telemetry_post_winter_Temp")
        new_t_movement_post=os.path.join(self.temp_gdb,"Telemetry_post_movement_Temp")
        new_t_winter_pre=os.path.join(self.temp_gdb,"Telemetry_pre_winter_Temp")
        new_t_movement_pre=os.path.join(self.temp_gdb,"Telemetry_pre_movement_Temp")

        fc_dict={s_winter_pre:new_s_winter_pre,t_winter_pre:new_t_winter_pre, s_winter_post:new_s_winter_post, t_winter_post:new_t_winter_post,
                s_movement_pre:new_s_movement_pre, t_movement_pre:new_t_movement_pre, s_movement_post:new_s_movement_post,  t_movement_post:new_t_movement_post}

        
        for key in fc_dict:
            print(key)
            print(fc_dict[key])
            sel_feats=arcpy.management.SelectLayerByLocation(fc_dict[key], "INTERSECT", key)
            sel_count=int(arcpy.GetCount_management (sel_feats).getOutput (0))
            logging.debug(f"{sel_count} number of Features that intersect with exisitng layer")

            if sel_count >0:
                logging.debug(f"{sel_count} features overlap, removing")
                arcpy.management.DeleteFeatures(arcpy.management.SelectLayerByLocation(fc_dict[key], "INTERSECT", key))
                
            feats=int(arcpy.management.GetCount(fc_dict[key]).getOutput (0))
            logging.debug(f"{feats} remaining, adding to master...")
            if feats>0:
                fieldMappings = arcpy.FieldMappings()
                fieldMappings.addTable(key)
                fieldMappings.addTable(fc_dict[key])
                #Do we Merge or do we append, and do we keep the previous FC"s for like 3 versions past somewhere?
                #I think we merge then make the new one with the date and archive the old one keep 3 and delete the one with the oldest date?

                # arcpy.management.Merge([key,fc_dict[key]],os.path.join(temp_gdb,"merge_FM_test"),fieldMappings )
                
                arcpy.management.Append(fc_dict[key], key, schema_type= "NO_TEST ", field_mapping=fieldMappings)
                logging.debug(f"append master class {key}")
                fc_name=os.path.basename(key)
                tracker.append_tracker(fc_name,feats, "SPI", username)
                # how do we get the source? flag from  maybe add prefix to temp data name?
            elif feats == 0:
                logging.debug( "no new feats")

    #=========================================================================================================================
        



    # set_up_reqs()
    # oracle_connect(bcgw_username, bcgw_password, bcgw_host_nm, bcgw_service_nm)
    # excute_quries(spi_obvs_df,spi_tele_df)
    # update_fc_from_spi(consolidated_gdb)
   