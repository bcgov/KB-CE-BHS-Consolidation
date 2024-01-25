import arcpy
import pandas as pd
import os
import logging
import datetime
import pathlib

import BHS_Tracker


class local_fc:
    def __init__ (self, crnt_wrkspc, temp_output, new_fcs_l, user_nm) :
        
        self.crnt_wrkspc= crnt_wrkspc
        self.temp_output= temp_output
        self.new_fcs_l= new_fcs_l
        self.user_nm= user_nm
        

        current_wrkspc=self.crnt_wrkspc
        temp_out=self.temp_output
        new_fcs=self.new_fcs_l
        usr_nm= self.user_nm

        logging.basicConfig(level=logging.DEBUG)
        w_m_codeblock="""def cat (m, d):
            if m <=3:
                return 'Winter'
            elif m == 12 and d >=15:
                return 'Winter'
            elif m == 11:
                return 'Movement'
            elif m == 12 and d <15:
                return 'Movement'
            elif m ==4 or m ==5:
                return 'Movement'"""

        tracker = BHS_Tracker.tracker(tracker_sheet)
        tracker.initialize(tracker_sheet)

        #existing data to append to
        
        

        #identifies current consolidated layers
        e_winter_pre=fr"{current_wrkspc}/BHS_Winter_Pre_1998"
        e_winter_post=fr"{current_wrkspc}/BHS_Winter_Post_1998"
        e_movement_pre=fr"{current_wrkspc}/BHS_Movement_Pre_1998"
        e_movement_post=fr"{current_wrkspc}/BHS_Movement_Post_1998"



    def initialize():
        if arcpy.Exists(temp_out):
            logging.debug('gdb exists')
        else:
            arcpy.management.CreateFileGDB(r'T:\bhs_test','bhs_temp_data.gdb')
            logging.debug('gdb created')
        arcpy.env.workspace=temp_out

    def check_merge(feature_list):
        #sort features into categories 
        #Pre/post, winter/movement 
        for fc in feature_list:
            logging.debug(fc)
            for keys in fc:
                logging.debug(keys)
                if keys is not None:
                    key=keys
                    if key.endswith('.shp') or key.endswith('.lyr'):
                        f2f_name= pathlib.Path(key).stem
                        logging.debug(f" f2f name {f2f_name}")
                        arcpy.conversion.FeatureClassToGeodatabase(key, temp_out )
                        key=os.path.join(temp_out,f2f_name)
                        logging.debug(f" new key {key}")

                    spatial_ref = arcpy.Describe(key).spatialReference
                    logging.debug(spatial_ref.name)
                    #check sptial ref
                    fc_str=os.path.basename(key)
                    fc_str=fc_str.split('.')[0]+'_BC_Albers'
                    logging.debug(fc_str)
                    if spatial_ref.name != 'NAD_1983_BC_Environment_Albers' and not arcpy.Exists(fc_str):
                        arcpy.management.Project(key, fc_str, 3005)
                        key=fc_str
                    elif arcpy.Exists(fc_str):
                        logging.debug('albers fc exists')
                        key=fc_str

                    year_field=arcpy.ListFields(key, '*Year*')
                    month_field=arcpy.ListFields(key, '*Month*')
                    day_field= arcpy.ListFields(key, '*Day*')
                    date_field=arcpy.ListFields(key, '*Date*')
                    if date_field[0] is not None:
                        #check for date field and split to get year type could be text, or datetime?
                        logging.debug(f" date field name is {date_field[0].name}")
                        logging.debug(f" date field type is {date_field[0].type}")
                        date_field=date_field[0].name

                    if year_field[0] is not None:
                        logging.debug(f" year field name is {year_field[0].name}")
                        logging.debug(f"year field type is {year_field[0].type}")
                        year_field=year_field[0].name
                        before_1900=arcpy.management.SelectLayerByAttribute(key,'NEW_SELECTION',f"{year_field} < 1900")
                        if int(arcpy.GetCount_management (before_1900).getOutput (0)) >0:
                            logging.debug(f"{int(arcpy.GetCount_management (before_1900).getOutput (0)) >0} features with the year before 1900")                     
                            arcpy.management.CalculateField(key,'Year',f"Year($feature.{date_field})", "ARCADE" )
                        arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                        before_1998=arcpy.management.SelectLayerByAttribute(key,'NEW_SELECTION',f"{year_field} < 1998")
        

                    if month_field[0] is not None:
                        logging.debug(f" month field name is {month_field[0].name}")
                        logging.debug(f" month field type is {month_field[0].type}")
                        month_field=month_field[0].name

                    if year_field[0] is None and date_field[0] is not None:
                        year_field='Year'
                        arcpy.management.AddField(key, 'Year', 'DOUBLE')
                        arcpy.management.CalculateField(key,'Year',f"Year($feature.{date_field})", "ARCADE" )
                        before_1998=arcpy.management.SelectLayerByAttribute(key,'NEW_SELECTION',"Year < 1998")
                    
                    if day_field[0] is not None:
                        logging.debug(f" day field name is {day_field[0].name}")
                        logging.debug(f" day field type is {day_field[0].type}")
                        day_field=day_field[0].name
                    else:
                        day_field='Day'
                        arcpy.management.AddField(key, 'Day', 'DOUBLE')
                        arcpy.management.CalculateField(key,'Year',f"Day($feature.{date_field})", "ARCADE" )
                    
                    if year_field is None and date_field[0] is None:
                        #no year field, check data!
                        logging.debug('NO YEAR OR DATE FIELD CHECK DATA')
                        exit()

                    before_1998_count=int(arcpy.GetCount_management (before_1998).getOutput (0))
                    logging.debug(f"{before_1998_count} features found with a date before 1998")

                    winter_pre="Year < 1998 And Cat ='Winter'"
                    winter_pre_name=fr"{e_winter_pre}_{fc[keys]}"

                    winter_post="Year >= 1998 And Cat ='Winter'"
                    winter_post_name=fr"{e_winter_post}_{fc[keys]}"

                    movement_pre="Year < 1998 And Cat ='Movement'" 
                    movement_pre_name=fr"{e_movement_pre}_{fc[keys]}"

                    movement_post="Year < 1998 And Cat >='Movement'" 
                    movement_post_name=fr"{e_movement_post}_{fc[keys]}"          
                    
                

                    if before_1998_count >0:
                        logging.info('Looking for features before 1998 to append')
                        arcpy.management.AddField(key, 'Cat', 'TEXT')
                        arcpy.management.CalculateField(key, 'Cat', f"cat (!{month_field}!, !{day_field}!)",'PYTHON3',w_m_codeblock)
        

                        #condense with loop?
                        sel_1=arcpy.management.SelectLayerByAttribute(key, 'NEW_SELECTION',"Year <= 1998 And Cat = 'Winter'" )
                        logging.debug(f"{int(arcpy.GetCount_management (sel_1).getOutput (0))} features in first select")
                        sel_2=arcpy.management.SelectLayerByLocation(in_layer=sel_1, overlap_type='WITHIN', select_features=winter_pre_name, selection_type='REMOVE_FROM_SELECTION')
                        final_count=int(arcpy.GetCount_management (sel_2).getOutput(0))
                        logging.debug(f"{final_count} features in second select")
                        if final_count >0:                
                            fieldMappings = arcpy.FieldMappings()
                            fieldMappings.addTable(winter_pre_name)
                            fieldMappings.addTable(key)
                            arcpy.management.Append(key, winter_pre_name, schema_type= "NO_TEST ", field_mapping=fieldMappings, expression=winter_pre)
                            fieldMappings.removeAll()
                            tracker.append_tracker(winter_pre_name,int(final_count), str(fc[keys]), str(usr_nm))
                            logging.info('winter pre 1998 appended')
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                        else:
                            logging.info(f"No new features for {winter_pre_name}")
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")  

                        sel_1=arcpy.management.SelectLayerByAttribute(key, 'NEW_SELECTION',"Year >= 1998 And Cat = 'Winter'" )
                        logging.debug(f"{int(arcpy.GetCount_management (sel_1).getOutput (0))} features in first select")
                        sel_2=arcpy.management.SelectLayerByLocation(in_layer=sel_1, overlap_type='WITHIN', select_features=winter_post_name, selection_type='REMOVE_FROM_SELECTION')
                        final_count=int(arcpy.GetCount_management (sel_2).getOutput(0))
                        logging.debug(f"{final_count} features in second select")
                        
                        if final_count >0:                     
                            fieldMappings = arcpy.FieldMappings()
                            fieldMappings.addTable(winter_post_name)
                            fieldMappings.addTable(key)
                            arcpy.management.Append(key, winter_post_name, schema_type= "NO_TEST ", field_mapping=fieldMappings, expression=winter_post)
                            fieldMappings.removeAll()
                            tracker.append_tracker(winter_post_name,int(final_count), str(fc[keys]), str(usr_nm))
                            logging.info('winter post 1998 appended')
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                        else:
                            logging.info(f"No new features for {winter_post_name}")
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")      
                                
                        sel_1=arcpy.management.SelectLayerByAttribute(key, 'NEW_SELECTION',"Year <= 1998 And Cat = 'Movement'" )
                        logging.debug(f"{int(arcpy.GetCount_management (sel_1).getOutput (0))} features in first select")
                        sel_2=arcpy.management.SelectLayerByLocation(in_layer=sel_1, overlap_type='WITHIN', select_features=movement_pre_name, selection_type='REMOVE_FROM_SELECTION')
                        final_count=int(arcpy.GetCount_management (sel_2).getOutput(0))
                        logging.debug(f"{final_count} features in second select")
                        
                        if final_count >0: 
                            fieldMappings = arcpy.FieldMappings()
                            fieldMappings.addTable(movement_pre_name)
                            fieldMappings.addTable(key)
                            arcpy.management.Append(key, movement_pre_name, schema_type= "NO_TEST ", field_mapping=fieldMappings, expression=movement_pre)
                            fieldMappings.removeAll()
                            tracker.append_tracker(movement_pre_name,int(final_count), str(fc[keys]), str(usr_nm))
                            logging.info('Movement pre 1998 appended')
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                        else:
                            logging.info(f"No new features for {movement_pre_name}")
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")  

                        sel_1=arcpy.management.SelectLayerByAttribute(key, 'NEW_SELECTION',"Year >= 1998 And Cat = 'Movement'" )
                        logging.debug(f"{int(arcpy.GetCount_management (sel_1).getOutput (0))} features in first select")
                        sel_2=arcpy.management.SelectLayerByLocation(in_layer=sel_1, overlap_type='WITHIN', select_features=movement_post_name, selection_type='REMOVE_FROM_SELECTION')
                        final_count=int(arcpy.GetCount_management (sel_2).getOutput(0))
                        logging.debug(f"{final_count} features in second select")
                        
                        if final_count >0: 
                            fieldMappings = arcpy.FieldMappings()
                            fieldMappings.addTable(movement_post_name)
                            fieldMappings.addTable(key)
                            arcpy.management.Append(key, movement_post_name, schema_type= "NO_TEST ", field_mapping=fieldMappings, expression=movement_post)
                            fieldMappings.removeAll()
                            tracker.append_tracker(movement_post_name,int(final_count), str(fc[keys]), str(usr_nm))
                            logging.info('Movement post 1998 appended')
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                        else:
                            logging.info(f"No new features for {movement_post_name}")
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")  
                                                        
                    
                    else:
                        logging.info('Looking for features after 1998 to append')

                        
                        arcpy.management.AddField(key, 'Cat', 'TEXT')
                        arcpy.management.CalculateField(key, 'Cat', f"cat (!{month_field}!, !{day_field}!)",'PYTHON3',w_m_codeblock)
                        logging.debug('Winter/Movement calculated')
                        
                        #condense with loop
                        sel_1=arcpy.management.SelectLayerByAttribute(key, 'NEW_SELECTION',"Year >= 1998 And Cat = 'Winter'" )
                        logging.debug(f"{int(arcpy.GetCount_management (sel_1).getOutput (0))} features in first select")
                        sel_2=arcpy.management.SelectLayerByLocation(in_layer=sel_1, overlap_type='WITHIN', select_features=winter_post_name, selection_type='REMOVE_FROM_SELECTION')
                        final_count=int(arcpy.GetCount_management (sel_2).getOutput(0))
                        logging.debug(f"{final_count} features in second select")
                        if final_count >0:
                            fieldMappings = arcpy.FieldMappings()
                            fieldMappings.addTable(winter_post_name)
                            fieldMappings.addTable(key)
                            arcpy.management.Append(key, winter_post_name, schema_type= "NO_TEST", field_mapping=fieldMappings, expression=winter_post) 
                            fieldMappings.removeAll()
                            tracker.append_tracker(str(winter_pre_name),(final_count), str(fc[keys]), str(usr_nm))      
                            logging.info('post winter features appended')
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                        else:
                            logging.info(f"No new features for {winter_post_name}")
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                            


                        sel_1=arcpy.management.SelectLayerByAttribute(key, 'NEW_SELECTION',"Year <= 1998 And Cat = 'Movement'" )
                        logging.debug(f"features before for this test only 74 985")
                        logging.debug(f"{int(arcpy.GetCount_management (sel_1).getOutput (0))} features in first select")
                        sel_2=arcpy.management.SelectLayerByLocation(in_layer=sel_1, overlap_type='WITHIN', select_features=movement_post_name, selection_type='REMOVE_FROM_SELECTION')
                        final_count=int(arcpy.GetCount_management (sel_2).getOutput(0))
                        logging.debug(f"{final_count} features in second select")
                        if final_count >0:                    
                            fieldMappings = arcpy.FieldMappings()
                            fieldMappings.addTable(movement_post_name)
                            fieldMappings.addTable(key)
                            arcpy.management.Append(key, movement_post_name, schema_type= "NO_TEST", field_mapping=fieldMappings, expression=movement_post)
                            fieldMappings.removeAll()  
                            tracker.append_tracker(movement_post_name,int(final_count), str(fc[keys]), str(usr_nm))
                            logging.info('post movement features appended')
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                        else:
                            logging.info(f"No new features for {movement_post_name}")
                            arcpy.SelectLayerByAttribute_management(key, "CLEAR_SELECTION")
                           
            

    def xlsx_test(fc,feat, meth, usern):
        tracker.append_tracker(fc,feat, meth, usern)


local_fc.initialize()
local_fc.check_merge(new_fcs)
