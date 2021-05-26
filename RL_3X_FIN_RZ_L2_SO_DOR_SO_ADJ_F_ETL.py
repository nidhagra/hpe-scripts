
#
# RL_3X_FIN_RZ_L2_SO_DOR_SO_ADJ_F_ETL.py
#

import os
import json
import io
import sys
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Row, SQLContext
from pyspark import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.types as T
import pyspark.sql.functions as F

def fetch_schema_name(stmt_id, spark_session, schema_name):
    print("fetching schema name for {schema_name}".format(schema_name=schema_name))
    ret_val = None
    try:
        df = spark_session.sql("select param_value from edw_control.param_env where param_nm = '{schema_name}';".format(schema_name=schema_name))
        if df.count() > 0:
            df.show(10, False)
            data = df.collect()
            #print(data)
            ret_val_list = [ row.param_value for row in data ]
            #print(ret_val_list)
            ret_val = ret_val_list[0]
            print("param value for {schema_name} -- {ret_val}".format(schema_name=schema_name, ret_val=ret_val))
        else:
            ret_val = None
    except Exception as e:
        print("ERROR {}".format(str(e)))
        ret_val = None
    return ret_val

def fetch_param_value(stmt_id, spark_session, param_name):
    print("fetching value for {param_name}".format(param_name=param_name))
    ret_val = None
    try:
        query_string="""
        select concat('Select ', column_nm, ' as ', param_nm, ' from ', table_nm, ' ', clause, ';' )
        from edw_control.param_mapping
        where param_nm = '{param_name}';
        """.format(param_name=param_name)
        df = spark_session.sql(query_string)
        if df.count() > 0:
            df.show(10, False)
            data = df.collect()
            q_list = [ row[0] for row in data ]
            q = q_list[0]
            df2 = spark_session.sql(q)
            if df2.count() > 0:
                df2.show(10, False)
                data2 = df2.collect()
                r_list = [ row[0] for row in data2 ]
                ret_val = r_list[0]
                print("param value for {param_name} -- {ret_val}".format(param_name=param_name, ret_val=ret_val))
            else:
                ret_val = None
        else:
            ret_val = None
    except Exception as e:
        print("ERROR {}".format(str(e)))
        ret_val = None
    return ret_val

def execute_sql(stmt_id, spark_session, spark_context, sql_context, query_string):
    print("executing {stmt_id} - {query_string}".format(stmt_id=stmt_id, query_string=query_string))
    ret_val = False
    try:
        spark_session.sql(query_string)
        ret_val = True
    except Exception as e:
        print("ERROR {}".format(str(e)))
        ret_val = False
    return ret_val

def execute():
    spark_session = (SparkSession.builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .getOrCreate()
    )
    spark_context = spark_session.sparkContext
    sql_context = SQLContext(spark_context)

    PARAM_SCHEMA_ETL_SCHEMA_s = fetch_schema_name("1", spark_session, "ETL_SCHEMA")
    if PARAM_SCHEMA_ETL_SCHEMA_s is None:
        print("ERROR fetching value for parameter 'PARAM_SCHEMA_ETL_SCHEMA_s'")
        sys.exit(1)
    query_to_execute = """USE {PARAM_SCHEMA_ETL_SCHEMA_s};""".format(PARAM_SCHEMA_ETL_SCHEMA_s=PARAM_SCHEMA_ETL_SCHEMA_s)
    ret_val = execute_sql("1", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(1)

    query_to_execute = """DELETE FROM STG_FIN_RZ_SO_DOR_SO_ADJ_F_ETL;""" 
    ret_val = execute_sql("2", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(2)

    #query_to_execute = """INSERT INTO STG_FIN_RZ_SO_DOR_SO_ADJ_F_ETL SELECT SO_ID,SRC_SYS_KY,ORD_CRT_DT,EFF_FRM_GMT_TS,SO_LN_ITM_ID,BUS_AREA_CD,EFF_TO_GMT_TS,CROSS_BORD_FG,CONTRA_FG,QTA_PROD_LN_ID,FIN_CLOSE_DT,CUST_REQD_DLVR_DT,SLDT_CUST_ID,BILT_CUST_ID,SHPT_CUST_ID,END_CUST_ID,MGMT_GEO_CD,SO_DTL_STAT_CD,SO_TYPE_CD,OM_SRC_SYS_KY,PROD_ID,SRC_PROD_LN_ID,SLS_CHNL_CD,DOC_CRNCY_CD,INVN_CLS_CD,SHIP_QT,SO_DTL_QT,0 AS GRS_EXT_SO_AM,0 AS GRS_EXT_SO_USD_AM,NET_EXT_SO_AM - GRS_EXT_SO_AM AS NET_EXT_SO_AM,NET_EXT_SO_USD_AM - GRS_EXT_SO_USD_AM AS NET_EXT_SO_USD_AM,NET_EXT_SO_AM - GRS_EXT_SO_AM AS ORD_ADJ_SO_AM,NET_EXT_SO_USD_AM - GRS_EXT_SO_USD_AM AS ORD_ADJ_SO_USD_AM,LEG_SO_ID,LEG_SO_LN_ITM_ID,FUNC_AREA_CD,PRFT_CTR_CD,LEGL_CO_CD,SO_MTHD_CD,SRVC_NOTIF_ID,ORD_CRT_DT_YR_MTH_CD,DOC_EXCH_RT,DOC_MTH_3_EXCH_RT,DOC_MTH_12_EXCH_RT,LGCL_DEL_FG,FIN_RPTBL_FG,DSTRB_CHNL_CD,ORD_RSN_CD,VALU_DLVR_CHAIN_CD,ACCT_MGR_ID,CMRCL_CD,CUST_APP_CD,CUST_CNTRCT_ID,CUST_PO_ID,GOV_CNTRCT_ID,INDNT_FG,PRIM_AGNT_ID,PROMO_CD,SECND_AGNT_ID,SHPT_ISO_CTRY_CD,SLS_OFF_CD,SLS_ORG_CD,SLS_REP_CD,BIG_DEAL_ID,BUS_TYPE_CD,PARNT_SO_LN_ITM_ID,PAYM_TERM_CD,PLNT_CD,PROJ_ID,QTA_CR_SUB_ENT_CD,REC_EXPLN_CD,SLS_FRC_CD,SLS_SUB_ENT_CD,SO_SLS_QT,SUPLYG_DIV_SUB_ENT_CD,WBS_ID,BASE_UNIT_OF_MSR_CD,MATL_SLS_UNIT_OF_MSR_CD,PARNT_SO_ID,0 AS NET_CNSTNT_3_MTH_SO_USD_AM,0 AS NET_CNSTNT_12_MTH_SO_USD_AM,REC_EXPLN_PRCS_CD,PARNT_PROD_ID,UNRSTD_BUS_AREA_CD,TRSY_BUS_ORG_CD,CONV_TYPE_ID,CONV_RT_CD,CBN_ID,ORIG_PROD_ID,TEAM_CD_ID,'Y80A' AS SO_ADJ_CD,'N' AS QTA_PROD_LN_OVERR_FG,ORD_ADJ_SO_AM AS SO_ADJ_AM,ORD_ADJ_SO_USD_AM AS SO_ADJ_USD_AM,'?' AS GL_GRP_ACCT_ID,VOL_DRCT_CUST_NM,'?' AS MISC_CHRG_CD,'?' AS PROD_OPT_MCC_CD,'?' AS SO_ADJ_ALLOC_FG,ACK_CD,SO_CMPGN_TX,DLVR_MTHD_CD,EIFFEL_INVN_CLS_CD,SO_MKT_PGM_TX,SRVC_GDS_PROD_ID,SRVC_GDS_QT,USE_NM,VALU_VOL_NM,CONSOLIDTD_INV_FG,DRCT_CUST_IND_CD,REBATE_ORD_RSN_CD,FULLMT_MDL_DN ,NET_DEALER_PRC_USD_AM,PKG_PROD_ID,ENGMT_MDL_CD,0 AS SO_OPT_QT,'?' AS PRIM_SO_ADJ_CD,MSTR_CNTRCT_ID,CNTRCT_LN_ITM_NR_ID,SD_ITM_CATG_CD,RTE_CD,CUST_ACCT_GRP_CD,REJ_RSN_CD,CNTRCT_STRT_DT AS CNTRCT_STRT_DT,CNTRCT_END_DT AS CNTRCT_END_DT,SO_PREPAYM_TYPE_CD AS SO_PREPAYM_TYPE_CD,TOT_NET_EXT_SO_AM AS TOT_NET_EXT_SO_AM FROM STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL WHERE ((STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.net_ext_so_am - STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.grs_ext_so_am) <> 0 OR (STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.net_ext_so_usd_am - STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.grs_ext_so_usd_am) <> 0)  ;""" 
    query_to_execute = """INSERT INTO STG_FIN_RZ_SO_DOR_SO_ADJ_F_ETL SELECT SO_ID,'Y80A' AS SO_ADJ_CD, SRC_SYS_KY,SO_LN_ITM_ID,ORD_CRT_DT,EFF_FRM_GMT_TS,DOC_CRNCY_CD,BUS_AREA_CD,EFF_TO_GMT_TS,CROSS_BORD_FG,CONTRA_FG,QTA_PROD_LN_ID,FIN_CLOSE_DT,CUST_REQD_DLVR_DT,SLDT_CUST_ID,BILT_CUST_ID,SHPT_CUST_ID,END_CUST_ID,MGMT_GEO_CD,SO_DTL_STAT_CD,SO_TYPE_CD,OM_SRC_SYS_KY,PROD_ID,SRC_PROD_LN_ID,SLS_CHNL_CD,INVN_CLS_CD,SHIP_QT,SO_DTL_QT,0 AS GRS_EXT_SO_AM,0 AS GRS_EXT_SO_USD_AM,NET_EXT_SO_AM - GRS_EXT_SO_AM AS NET_EXT_SO_AM,NET_EXT_SO_USD_AM - GRS_EXT_SO_USD_AM AS NET_EXT_SO_USD_AM,NET_EXT_SO_AM - GRS_EXT_SO_AM AS ORD_ADJ_SO_AM,NET_EXT_SO_USD_AM - GRS_EXT_SO_USD_AM AS ORD_ADJ_SO_USD_AM,LEG_SO_ID,LEG_SO_LN_ITM_ID,FUNC_AREA_CD,PRFT_CTR_CD,LEGL_CO_CD,SO_MTHD_CD,SRVC_NOTIF_ID,ORD_CRT_DT_YR_MTH_CD,DOC_EXCH_RT,DOC_MTH_3_EXCH_RT,DOC_MTH_12_EXCH_RT,LGCL_DEL_FG,FIN_RPTBL_FG,DSTRB_CHNL_CD,ORD_RSN_CD,VALU_DLVR_CHAIN_CD,ACCT_MGR_ID,CMRCL_CD,CUST_APP_CD,CUST_CNTRCT_ID,CUST_PO_ID,GOV_CNTRCT_ID,INDNT_FG,PRIM_AGNT_ID,PROMO_CD,SECND_AGNT_ID,SHPT_ISO_CTRY_CD,SLS_OFF_CD,SLS_ORG_CD,SLS_REP_CD,BIG_DEAL_ID,BUS_TYPE_CD,PARNT_SO_LN_ITM_ID,PAYM_TERM_CD,PLNT_CD,PROJ_ID,QTA_CR_SUB_ENT_CD,REC_EXPLN_CD,SLS_FRC_CD,SLS_SUB_ENT_CD,SO_SLS_QT,SUPLYG_DIV_SUB_ENT_CD,WBS_ID,BASE_UNIT_OF_MSR_CD,MATL_SLS_UNIT_OF_MSR_CD,PARNT_SO_ID,0 AS NET_CNSTNT_3_MTH_SO_USD_AM,0 AS NET_CNSTNT_12_MTH_SO_USD_AM,REC_EXPLN_PRCS_CD,PARNT_PROD_ID,UNRSTD_BUS_AREA_CD,TRSY_BUS_ORG_CD,CONV_TYPE_ID,CONV_RT_CD,CBN_ID,ORIG_PROD_ID,TEAM_CD_ID,'N' AS QTA_PROD_LN_OVERR_FG,ORD_ADJ_SO_AM AS SO_ADJ_AM,ORD_ADJ_SO_USD_AM AS SO_ADJ_USD_AM,'?' AS GL_GRP_ACCT_ID,FULLMT_MDL_DN ,
    NET_DEALER_PRC_USD_AM,'?' AS PRIM_SO_ADJ_CD,
    VOL_DRCT_CUST_NM,'?' AS MISC_CHRG_CD,'?' AS PROD_OPT_MCC_CD,'?' AS SO_ADJ_ALLOC_FG,0 AS SO_OPT_QT,ACK_CD,SO_CMPGN_TX,DLVR_MTHD_CD,EIFFEL_INVN_CLS_CD,SO_MKT_PGM_TX,PKG_PROD_ID,SRVC_GDS_PROD_ID,SRVC_GDS_QT,USE_NM,VALU_VOL_NM,CONSOLIDTD_INV_FG,DRCT_CUST_IND_CD,ENGMT_MDL_CD,
	REBATE_ORD_RSN_CD,MSTR_CNTRCT_ID,CNTRCT_LN_ITM_NR_ID,SD_ITM_CATG_CD,RTE_CD,CUST_ACCT_GRP_CD,REJ_RSN_CD,CNTRCT_STRT_DT AS CNTRCT_STRT_DT,CNTRCT_END_DT AS CNTRCT_END_DT,SO_PREPAYM_TYPE_CD AS SO_PREPAYM_TYPE_CD,TOT_NET_EXT_SO_AM AS TOT_NET_EXT_SO_AM,TOT_NET_EXT_SO_USD_AM AS TOT_NET_EXT_SO_USD_AM  FROM STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL WHERE ((STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.net_ext_so_am - STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.grs_ext_so_am) <> 0 OR (STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.net_ext_so_usd_am - STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL.grs_ext_so_usd_am) <> 0)  ;""" 
    ret_val = execute_sql("3", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(3)

    #query_to_execute = """INSERT INTO STG_FIN_RZ_SO_DOR_SO_ADJ_F_ETL SELECT SO_ID,SRC_SYS_KY,ORD_CRT_DT,EFF_FRM_GMT_TS,SO_LN_ITM_ID,BUS_AREA_CD,EFF_TO_GMT_TS,CROSS_BORD_FG,CONTRA_FG,QTA_PROD_LN_ID,FIN_CLOSE_DT,CUST_REQD_DLVR_DT,SLDT_CUST_ID,BILT_CUST_ID,SHPT_CUST_ID,END_CUST_ID,MGMT_GEO_CD,SO_DTL_STAT_CD,SO_TYPE_CD,OM_SRC_SYS_KY,PROD_ID,SRC_PROD_LN_ID,SLS_CHNL_CD,DOC_CRNCY_CD,INVN_CLS_CD,SHIP_QT,SO_DTL_QT,GRS_EXT_SO_AM,GRS_EXT_SO_USD_AM,GRS_EXT_SO_AM AS NET_EXT_SO_AM,GRS_EXT_SO_USD_AM AS NET_EXT_SO_USD_AM,0 AS ORD_ADJ_SO_AM,0 AS ORD_ADJ_SO_USD_AM,LEG_SO_ID,LEG_SO_LN_ITM_ID,FUNC_AREA_CD,PRFT_CTR_CD,LEGL_CO_CD,SO_MTHD_CD,SRVC_NOTIF_ID,ORD_CRT_DT_YR_MTH_CD,DOC_EXCH_RT,DOC_MTH_3_EXCH_RT,DOC_MTH_12_EXCH_RT,LGCL_DEL_FG,FIN_RPTBL_FG,DSTRB_CHNL_CD,ORD_RSN_CD,VALU_DLVR_CHAIN_CD,ACCT_MGR_ID,CMRCL_CD,CUST_APP_CD,CUST_CNTRCT_ID,CUST_PO_ID,GOV_CNTRCT_ID,INDNT_FG,PRIM_AGNT_ID,PROMO_CD,SECND_AGNT_ID,SHPT_ISO_CTRY_CD,SLS_OFF_CD,SLS_ORG_CD,SLS_REP_CD,BIG_DEAL_ID,BUS_TYPE_CD,PARNT_SO_LN_ITM_ID,PAYM_TERM_CD,PLNT_CD,PROJ_ID,QTA_CR_SUB_ENT_CD,REC_EXPLN_CD,SLS_FRC_CD,SLS_SUB_ENT_CD,SO_SLS_QT,SUPLYG_DIV_SUB_ENT_CD,WBS_ID,BASE_UNIT_OF_MSR_CD,MATL_SLS_UNIT_OF_MSR_CD,PARNT_SO_ID,NET_CNSTNT_3_MTH_SO_USD_AM,NET_CNSTNT_12_MTH_SO_USD_AM,REC_EXPLN_PRCS_CD,PARNT_PROD_ID,UNRSTD_BUS_AREA_CD,TRSY_BUS_ORG_CD,CONV_TYPE_ID,CONV_RT_CD,CBN_ID,ORIG_PROD_ID,TEAM_CD_ID,'ZA01' AS SO_ADJ_CD,'N' AS QTA_PROD_LN_OVERR_FG,GRS_EXT_SO_AM AS SO_ADJ_AM,GRS_EXT_SO_USD_AM AS SO_ADJ_USD_AM,'?' AS GL_GRP_ACCT_ID,VOL_DRCT_CUST_NM,'?' AS MISC_CHRG_CD,'?' AS PROD_OPT_MCC_CD,'?' AS SO_ADJ_ALLOC_FG,ACK_CD,SO_CMPGN_TX,DLVR_MTHD_CD,EIFFEL_INVN_CLS_CD,SO_MKT_PGM_TX,SRVC_GDS_PROD_ID,SRVC_GDS_QT,USE_NM,VALU_VOL_NM,CONSOLIDTD_INV_FG,DRCT_CUST_IND_CD,REBATE_ORD_RSN_CD,FULLMT_MDL_DN ,NET_DEALER_PRC_USD_AM,PKG_PROD_ID,ENGMT_MDL_CD,0 AS SO_OPT_QT,PRIM_SO_ADJ_CD,MSTR_CNTRCT_ID,CNTRCT_LN_ITM_NR_ID,SD_ITM_CATG_CD,RTE_CD,CUST_ACCT_GRP_CD,REJ_RSN_CD,CNTRCT_STRT_DT AS CNTRCT_STRT_DT,CNTRCT_END_DT AS CNTRCT_END_DT,SO_PREPAYM_TYPE_CD AS SO_PREPAYM_TYPE_CD,TOT_NET_EXT_SO_AM AS TOT_NET_EXT_SO_AM FROM STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL  ;""" 
    query_to_execute = """INSERT INTO STG_FIN_RZ_SO_DOR_SO_ADJ_F_ETL SELECT SO_ID,'Y80A' AS SO_ADJ_CD, SRC_SYS_KY,SO_LN_ITM_ID,ORD_CRT_DT,EFF_FRM_GMT_TS,DOC_CRNCY_CD,BUS_AREA_CD,EFF_TO_GMT_TS,CROSS_BORD_FG,CONTRA_FG,QTA_PROD_LN_ID,FIN_CLOSE_DT,CUST_REQD_DLVR_DT,SLDT_CUST_ID,BILT_CUST_ID,SHPT_CUST_ID,END_CUST_ID,MGMT_GEO_CD,SO_DTL_STAT_CD,SO_TYPE_CD,OM_SRC_SYS_KY,PROD_ID,SRC_PROD_LN_ID,SLS_CHNL_CD,INVN_CLS_CD,SHIP_QT,SO_DTL_QT,0 AS GRS_EXT_SO_AM,0 AS GRS_EXT_SO_USD_AM,NET_EXT_SO_AM - GRS_EXT_SO_AM AS NET_EXT_SO_AM,NET_EXT_SO_USD_AM - GRS_EXT_SO_USD_AM AS NET_EXT_SO_USD_AM,NET_EXT_SO_AM - GRS_EXT_SO_AM AS ORD_ADJ_SO_AM,NET_EXT_SO_USD_AM - GRS_EXT_SO_USD_AM AS ORD_ADJ_SO_USD_AM,LEG_SO_ID,LEG_SO_LN_ITM_ID,FUNC_AREA_CD,PRFT_CTR_CD,LEGL_CO_CD,SO_MTHD_CD,SRVC_NOTIF_ID,ORD_CRT_DT_YR_MTH_CD,DOC_EXCH_RT,DOC_MTH_3_EXCH_RT,DOC_MTH_12_EXCH_RT,LGCL_DEL_FG,FIN_RPTBL_FG,DSTRB_CHNL_CD,ORD_RSN_CD,VALU_DLVR_CHAIN_CD,ACCT_MGR_ID,CMRCL_CD,CUST_APP_CD,CUST_CNTRCT_ID,CUST_PO_ID,GOV_CNTRCT_ID,INDNT_FG,PRIM_AGNT_ID,PROMO_CD,SECND_AGNT_ID,SHPT_ISO_CTRY_CD,SLS_OFF_CD,SLS_ORG_CD,SLS_REP_CD,BIG_DEAL_ID,BUS_TYPE_CD,PARNT_SO_LN_ITM_ID,PAYM_TERM_CD,PLNT_CD,PROJ_ID,QTA_CR_SUB_ENT_CD,REC_EXPLN_CD,SLS_FRC_CD,SLS_SUB_ENT_CD,SO_SLS_QT,SUPLYG_DIV_SUB_ENT_CD,WBS_ID,BASE_UNIT_OF_MSR_CD,MATL_SLS_UNIT_OF_MSR_CD,PARNT_SO_ID,0 AS NET_CNSTNT_3_MTH_SO_USD_AM,0 AS NET_CNSTNT_12_MTH_SO_USD_AM,REC_EXPLN_PRCS_CD,PARNT_PROD_ID,UNRSTD_BUS_AREA_CD,TRSY_BUS_ORG_CD,CONV_TYPE_ID,CONV_RT_CD,CBN_ID,ORIG_PROD_ID,TEAM_CD_ID,'N' AS QTA_PROD_LN_OVERR_FG,ORD_ADJ_SO_AM AS SO_ADJ_AM,ORD_ADJ_SO_USD_AM AS SO_ADJ_USD_AM,'?' AS GL_GRP_ACCT_ID,FULLMT_MDL_DN ,
    NET_DEALER_PRC_USD_AM,'?' AS PRIM_SO_ADJ_CD,
    VOL_DRCT_CUST_NM,'?' AS MISC_CHRG_CD,'?' AS PROD_OPT_MCC_CD,'?' AS SO_ADJ_ALLOC_FG,0 AS SO_OPT_QT,ACK_CD,SO_CMPGN_TX,DLVR_MTHD_CD,EIFFEL_INVN_CLS_CD,SO_MKT_PGM_TX,PKG_PROD_ID,SRVC_GDS_PROD_ID,SRVC_GDS_QT,USE_NM,VALU_VOL_NM,CONSOLIDTD_INV_FG,DRCT_CUST_IND_CD,ENGMT_MDL_CD,
	REBATE_ORD_RSN_CD,MSTR_CNTRCT_ID,CNTRCT_LN_ITM_NR_ID,SD_ITM_CATG_CD,RTE_CD,CUST_ACCT_GRP_CD,REJ_RSN_CD,CNTRCT_STRT_DT AS CNTRCT_STRT_DT,CNTRCT_END_DT AS CNTRCT_END_DT,SO_PREPAYM_TYPE_CD AS SO_PREPAYM_TYPE_CD,TOT_NET_EXT_SO_AM AS TOT_NET_EXT_SO_AM,TOT_NET_EXT_SO_USD_AM AS TOT_NET_EXT_SO_USD_AM FROM STG_FIN_RZ_SO_DOR_SO_DTL_F_ETL  ;"""
    
    ret_val = execute_sql("4", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(4)


    spark_session.stop()


if __name__ == "__main__":
    # datetime object containing current date and time
    now = datetime.now()
     
    # dd/mm/YY H:M:S
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
    file_name =  os.path.basename(sys.argv[0])
    print("{} | {} | starting execution".format(dt_string, file_name))
    
    execute()
    
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
    
    print("{} | {} | done".format(dt_string, file_name))

#
# end
#
