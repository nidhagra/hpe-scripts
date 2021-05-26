
#
# RL_3X_FIN_RZ_L2_SHIP_MCC_UNB_STG3_ETL.py
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

    query_to_execute = """DELETE   FROM STG_FIN_RZ_SHIP_DTL_CONTRA_DRV_FNL_ETL WHERE ORD_ADJ_SHIP_AM <> 0;""" 
    ret_val = execute_sql("2", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(2)

    query_to_execute = """INSERT INTO STG_FIN_RZ_SHIP_DTL_CONTRA_DRV_FNL_ETL SELECT SHIP_ID,SRC_SYS_KY,SHIP_LN_ITM_ID,SHIP_DT,SO_ADJ_CD,EFF_FRM_GMT_TS,BUS_AREA_CD,SO_ID,ORD_CRT_DT,SO_LN_ITM_ID,REC_EXPLN_CD,PROD_ID,CONTRA_FG,PRFT_CTR_CD,CBN_ID,SLDT_CUST_ID,SHPT_CUST_ID,BILT_CUST_ID,END_CUST_ID,MGMT_GEO_CD,SO_TYPE_CD,OM_SRC_SYS_KY,FIN_CLOSE_DT,SRC_PROD_LN_ID,FUNC_AREA_CD,CROSS_BORD_FG,SLS_CHNL_CD,DOC_CRNCY_CD,QTA_PROD_LN_ID,EFF_TO_GMT_TS,CUST_REQD_DLVR_DT,INVN_CLS_CD,QTA_PROD_LN_OVERR_FG,SLS_ORG_CD,SHIP_FRM_PLNT_CD,PARNT_SO_LN_ITM_ID,LEGL_CO_CD,SO_MTHD_CD,LEG_SO_ID,LEG_SO_LN_ITM_ID,CASE WHEN (MAX(SHIP_QT) > 0) THEN MAX(SHIP_QT) ELSE MIN(SHIP_QT) END AS SHIP_QT,CASE WHEN (MAX(SHIP_SLS_QT) > 0) THEN MAX(SHIP_SLS_QT) ELSE MIN(SHIP_SLS_QT) END AS SHIP_SLS_QT,AVG(UNIT_PRC_AM) AS UNIT_PRC_AM,SUM(GRS_EXT_SHIP_AM) AS GRS_EXT_SHIP_AM,SUM(GRS_EXT_SHIP_USD_AM) AS GRS_EXT_SHIP_USD_AM,SUM(ORD_ADJ_SHIP_AM) AS ORD_ADJ_SHIP_AM,SUM(ORD_ADJ_SHIP_USD_AM) AS ORD_ADJ_SHIP_USD_AM,SUM(NET_EXT_SHIP_AM) AS NET_EXT_SHIP_AM,SUM(NET_EXT_SHIP_USD_AM) AS NET_EXT_SHIP_USD_AM,INS_GMT_TS,UPD_GMT_TS,REC_ST_NR,LOAD_JOB_NR,LGCL_DEL_FG,DSTRB_CHNL_CD,ORD_RSN_CD,VALU_DLVR_CHAIN_CD,ACCT_MGR_ID,BASE_UNIT_OF_MSR_CD,BIG_DEAL_ID,BUS_TYPE_CD,CMRCL_CD,CUST_APP_CD,CUST_CNTRCT_ID,CUST_PO_ID,DLVR_MTHD_CD,GOV_CNTRCT_ID,INDNT_FG,LC_CD,MATL_SLS_UNIT_OF_MSR_CD,ORIG_PROD_ID,PAYM_TERM_CD,PRIM_AGNT_ID,PROJ_ID,PROMO_CD,SECND_AGNT_ID,SHPT_ISO_CTRY_CD,SLS_FRC_CD,SLS_OFF_CD,SLS_REP_CD,SLS_SUB_ENT_CD,SRVC_NOTIF_ID,SUPLYG_DIV_SUB_ENT_CD,WBS_ID,SUM(SHIP_ADJ_AM) AS SHIP_ADJ_AM,TEAM_CD_ID,FRST_TCHPT_ACTL_SHIP_TS,LAST_TCHPT_ACTL_SHIP_TS,SHIP_POST_TS,QTA_CR_SUB_ENT_CD,GL_GRP_ACCT_ID,DOC_EXCH_RT,DOC_MTH_3_EXCH_RT,DOC_MTH_12_EXCH_RT,SUM(SHIP_ADJ_USD_AM) AS SHIP_ADJ_USD_AM,SUM(NET_CNSTNT_3_MTH_SHIP_USD_AM) AS NET_CNSTNT_3_MTH_SHIP_USD_AM,SUM(NET_CNSTNT_12_MTH_SHIP_USD_AM) AS NET_CNSTNT_12_MTH_SHIP_USD_AM,CONV_TYPE_ID,CONV_RT_CD,TRSY_BUS_ORG_CD,ORD_CRT_DT_YR_MTH_CD,UNRSTD_BUS_AREA_CD,PARNT_SO_ID,REC_EXPLN_PRCS_CD,PARNT_PROD_ID,FDW_TRD_INTRA_CO_FG,LC_EXCH_RT,LC_MTH_3_EXCH_RT,LC_MTH_12_EXCH_RT,SUM(NET_EXT_SHIP_LC_AM) AS NET_EXT_SHIP_LC_AM,SUM(GRS_EXT_SHIP_LC_AM) AS GRS_EXT_SHIP_LC_AM,SUM(ORD_ADJ_SHIP_LC_AM) AS ORD_ADJ_SHIP_LC_AM,SUM(SHIP_ADJ_LC_AM) AS SHIP_ADJ_LC_AM,CASE WHEN (MAX(SHIP_EXT_QT) > 0) THEN MAX(SHIP_EXT_QT) ELSE MIN(SHIP_EXT_QT) END AS SHIP_EXT_QT,CASE WHEN (MAX(SHIP_UNIT_QT) > 0) THEN MAX(SHIP_UNIT_QT) ELSE MIN(SHIP_UNIT_QT) END AS SHIP_UNIT_QT,FUNC_AREA_LVL_7_CD,SUM(NET_DEALER_PRC_USD_AM) AS NET_DEALER_PRC_USD_AM,PRIM_SO_ADJ_CD,BILL_DOC_DT,BILL_DUE_DT,BILL_DOC_ID,MISC_CHRG_CD,PROD_OPT_MCC_CD,SO_ADJ_ALLOC_FG,CASE WHEN (QTA_PROD_LN_OVERR_FG = 'Y') THEN SUM(SHIP_OPT_QT) ELSE CASE WHEN (MAX(SHIP_OPT_QT) > 0) THEN MAX(SHIP_OPT_QT) ELSE MIN(SHIP_OPT_QT) END END AS SHIP_OPT_QT,ACK_CD,SO_CMPGN_TX,EIFFEL_INVN_CLS_CD,SO_MKT_PGM_TX,PKG_PROD_ID,SRVC_GDS_PROD_ID,CASE WHEN (MAX(SRVC_GDS_QT) > 0) THEN MAX(SRVC_GDS_QT) ELSE MIN(SRVC_GDS_QT) END AS SRVC_GDS_QT,USE_NM,VALU_VOL_NM,CONSOLIDTD_INV_FG,DRCT_CUST_IND_CD,ENGMT_MDL_CD,REBATE_ORD_RSN_CD,SD_ITM_CATG_CD FROM (SELECT T.ACCT_MGR_ID,T.BASE_UNIT_OF_MSR_CD,T.BIG_DEAL_ID,T.BILT_CUST_ID,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.BUS_AREA_CD ELSE T.BUS_AREA_CD END AS BUS_AREA_CD,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN 'B' ELSE t.BUS_TYPE_CD END AS BUS_TYPE_CD,T.CBN_ID,T.CMRCL_CD,T.CONTRA_FG,T.CONV_RT_CD,T.CONV_TYPE_ID,T.CROSS_BORD_FG,T.CUST_APP_CD,T.CUST_CNTRCT_ID,T.CUST_PO_ID,T.CUST_REQD_DLVR_DT,T.DLVR_MTHD_CD,T.DOC_CRNCY_CD,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.DOC_EXCH_RT ELSE T.DOC_EXCH_RT END AS DOC_EXCH_RT,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.DOC_MTH_12_EXCH_RT ELSE T.DOC_MTH_12_EXCH_RT END AS DOC_MTH_12_EXCH_RT,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.DOC_MTH_3_EXCH_RT ELSE T.DOC_MTH_3_EXCH_RT END AS DOC_MTH_3_EXCH_RT,T.DSTRB_CHNL_CD,T.EFF_FRM_GMT_TS,T.EFF_TO_GMT_TS,T.END_CUST_ID,T.FDW_TRD_INTRA_CO_FG,T.FIN_CLOSE_DT,T.FRST_TCHPT_ACTL_SHIP_TS,T.FUNC_AREA_CD,T.FUNC_AREA_LVL_7_CD,T.GL_GRP_ACCT_ID,T.GOV_CNTRCT_ID,NVL(CAST(T.GRS_EXT_SHIP_AM * T2.DISC_FAC_NR AS NUMERIC(18,6)),T.GRS_EXT_SHIP_AM) AS GRS_EXT_SHIP_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL((((T.GRS_EXT_SHIP_AM / T2.DOC_EXCH_RT) * T2.LC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.GRS_EXT_SHIP_LC_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END,T.GRS_EXT_SHIP_LC_AM) AS GRS_EXT_SHIP_LC_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL(((T.GRS_EXT_SHIP_AM / T2.DOC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.GRS_EXT_SHIP_USD_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END,T.GRS_EXT_SHIP_USD_AM) AS GRS_EXT_SHIP_USD_AM,T.INDNT_FG,T.INS_GMT_TS,T.INVN_CLS_CD,T.LAST_TCHPT_ACTL_SHIP_TS,T.LC_CD,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.LC_EXCH_RT ELSE T.LC_EXCH_RT END AS LC_EXCH_RT,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.LC_MTH_12_EXCH_RT ELSE T.LC_MTH_12_EXCH_RT END AS LC_MTH_12_EXCH_RT,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.LC_MTH_3_EXCH_RT ELSE T.LC_MTH_3_EXCH_RT END AS LC_MTH_3_EXCH_RT,T.LEG_SO_ID,T.LEG_SO_LN_ITM_ID,T.LEGL_CO_CD,T.LGCL_DEL_FG,T.LOAD_JOB_NR,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN '?' ELSE t.MATL_SLS_UNIT_OF_MSR_CD END AS MATL_SLS_UNIT_OF_MSR_CD,T.MGMT_GEO_CD,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_MTH_12_EXCH_RT = 0) THEN 0 ELSE CAST(NVL(((T.NET_EXT_SHIP_AM / T2.DOC_MTH_12_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.NET_CNSTNT_12_MTH_SHIP_USD_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END, T.NET_CNSTNT_12_MTH_SHIP_USD_AM) AS NET_CNSTNT_12_MTH_SHIP_USD_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_MTH_3_EXCH_RT = 0) THEN 0 ELSE CAST(NVL(((T.NET_EXT_SHIP_AM / T2.DOC_MTH_3_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.NET_CNSTNT_3_MTH_SHIP_USD_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END, T.NET_CNSTNT_3_MTH_SHIP_USD_AM) AS NET_CNSTNT_3_MTH_SHIP_USD_AM,NVL(CAST(T.NET_EXT_SHIP_AM * T2.DISC_FAC_NR AS NUMERIC(18,6)), T.NET_EXT_SHIP_AM) AS NET_EXT_SHIP_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL((((T.NET_EXT_SHIP_AM / T2.DOC_EXCH_RT) * T2.LC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.NET_EXT_SHIP_LC_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END,T.NET_EXT_SHIP_LC_AM) AS NET_EXT_SHIP_LC_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL(((T.NET_EXT_SHIP_AM / T2.DOC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.NET_EXT_SHIP_USD_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END, T.NET_EXT_SHIP_USD_AM) AS NET_EXT_SHIP_USD_AM,T.OM_SRC_SYS_KY,NVL(CAST(T.ORD_ADJ_SHIP_AM * T2.DISC_FAC_NR AS NUMERIC(18,6)),T.ORD_ADJ_SHIP_AM) AS ORD_ADJ_SHIP_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL((((T.ORD_ADJ_SHIP_AM / T2.DOC_EXCH_RT) * T2.LC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.ORD_ADJ_SHIP_LC_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END, T.ORD_ADJ_SHIP_LC_AM) AS ORD_ADJ_SHIP_LC_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL(((T.ORD_ADJ_SHIP_AM / T2.DOC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.ORD_ADJ_SHIP_USD_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END, T.ORD_ADJ_SHIP_USD_AM) AS ORD_ADJ_SHIP_USD_AM,T.ORD_CRT_DT,T.ORD_CRT_DT_YR_MTH_CD,T.ORD_RSN_CD,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.BUS_AREA_CD ELSE T.BUS_AREA_CD END AS UNRSTD_BUS_AREA_CD,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN 'M' || trim(ADJ_D.MISC_CHRG_CD) || '_' || trim(t2.QTA_PROD_LN_ID) ELSE t2.orig_prod_id END,T.ORIG_PROD_ID) AS ORIG_PROD_ID,T.PARNT_PROD_ID,T.PARNT_SO_ID,T.PARNT_SO_LN_ITM_ID,T.PAYM_TERM_CD,T.PRFT_CTR_CD,T.PRIM_AGNT_ID,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN 'M' || trim(ADJ_D.MISC_CHRG_CD) || '_' || trim(t2.QTA_PROD_LN_ID) ELSE t2.prod_id END, T.PROD_ID) AS PROD_ID,T.PROJ_ID,T.PROMO_CD,T.QTA_CR_SUB_ENT_CD,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T2.QTA_PROD_LN_ID ELSE T.QTA_PROD_LN_ID END AS QTA_PROD_LN_ID,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL THEN'Y' ELSE T.QTA_PROD_LN_OVERR_FG END AS QTA_PROD_LN_OVERR_FG,T.REC_EXPLN_CD,CASE WHEN TRIM(T.REC_EXPLN_PRCS_CD) = '?') THEN 'M' ELSE TRIM(T.REC_EXPLN_PRCS_CD) || 'M' END AS REC_EXPLN_PRCS_CD,T.REC_ST_NR,T.SECND_AGNT_ID,NVL(CAST(T.SHIP_ADJ_AM * T2.DISC_FAC_NR AS NUMERIC(18,6)), T.SHIP_ADJ_AM) AS SHIP_ADJ_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL((((T.SHIP_ADJ_AM / T2.DOC_EXCH_RT) * T2.LC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.SHIP_ADJ_LC_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END, T.SHIP_ADJ_LC_AM) AS SHIP_ADJ_LC_AM,NVL(CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN CASE WHEN (T2.DOC_EXCH_RT = 0) THEN 0 ELSE CAST(NVL(((T.SHIP_ADJ_AM / T2.DOC_EXCH_RT) * T2.DISC_FAC_NR),0) AS NUMERIC(18,4)) END ELSE CAST(T.SHIP_ADJ_USD_AM * T2.DISC_FAC_NR AS NUMERIC(18,4)) END, T.SHIP_ADJ_USD_AM) AS SHIP_ADJ_USD_AM,T.SHIP_DT,CASE WHEN ((STG_FIN_RZ_SHIP_PROD_D_ETL.MDL_UNITS_PER_SKU_QT > 1)) THEN NVL(T.SHIP_QT * STG_FIN_RZ_SHIP_PROD_D_ETL.MDL_UNITS_PER_SKU_QT,T.SHIP_QT) ELSE T.SHIP_QT END AS SHIP_EXT_QT,T.SHIP_FRM_PLNT_CD,T.SHIP_ID,T.SHIP_LN_ITM_ID,T.SHIP_POST_TS,T.SHIP_QT,T.SHIP_SLS_QT,CASE WHEN ((STG_FIN_RZ_SHIP_PROD_D_ETL.MDL_UNITS_PER_SKU_QT > 1) AND (STG_FIN_RZ_SHIP_PROD_D_ETL.PROD_TYPE_ID = 'UN' OR STG_FIN_RZ_SHIP_PROD_D_ETL.RECOND_TYPE_CD = 'Recon Unit')) THEN NVL(T.SHIP_QT * STG_FIN_RZ_SHIP_PROD_D_ETL.MDL_UNITS_PER_SKU_QT,T.SHIP_QT) WHEN ((STG_FIN_RZ_SHIP_PROD_D_ETL.PROD_TYPE_ID = 'UN' OR STG_FIN_RZ_SHIP_PROD_D_ETL.RECOND_TYPE_CD = 'Recon Unit')) THEN T.SHIP_QT ELSE 0 END AS SHIP_UNIT_QT,T.SHPT_CUST_ID,T.SHPT_ISO_CTRY_CD,T.SLDT_CUST_ID,T.SLS_CHNL_CD,T.SLS_FRC_CD,T.SLS_OFF_CD,T.SLS_ORG_CD,T.SLS_REP_CD,T.SLS_SUB_ENT_CD,T.SO_ADJ_CD,T.SO_ID,T.SO_LN_ITM_ID,T.SO_MTHD_CD,T.SO_TYPE_CD,T.SRC_PROD_LN_ID,T.SRC_SYS_KY,T.SRVC_NOTIF_ID,T.SUPLYG_DIV_SUB_ENT_CD,T.TEAM_CD_ID,T.TRSY_BUS_ORG_CD,T.UNIT_PRC_AM,T.UPD_GMT_TS,T.VALU_DLVR_CHAIN_CD,T.WBS_ID,T.PRIM_SO_ADJ_CD,T.BILL_DOC_DT,T.BILL_DUE_DT,T.BILL_DOC_ID,T.MISC_CHRG_CD,T.PROD_OPT_MCC_CD,T.SO_ADJ_ALLOC_FG,T.ACK_CD,T.SO_CMPGN_TX,T.EIFFEL_INVN_CLS_CD,T.SO_MKT_PGM_TX,T.PKG_PROD_ID,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL AND T.REC_EXPLN_CD = 'C' AND T.REC_EXPLN_PRCS_CD = 'I') THEN T.PARNT_PROD_ID WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T.ORIG_PROD_ID ELSE '?' END AS SRVC_GDS_PROD_ID,CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN T.SHIP_QT ELSE 0 END AS SRVC_GDS_QT,T.USE_NM,T.VALU_VOL_NM,T.CONSOLIDTD_INV_FG,T.DRCT_CUST_IND_CD,T.ENGMT_MDL_CD,T.REBATE_ORD_RSN_CD,T.NET_DEALER_PRC_USD_AM,T.SHIP_OPT_QT,T.SD_ITM_CATG_CD FROM STG_FIN_RZ_SHIP_MCC_UNB_DRV_ETL T LEFT JOIN (SELECT SHIP_ID,SHIP_LN_ITM_ID,SHIP_DT,SO_ID,SRC_SYS_KY,ORD_CRT_DT,EFF_FRM_GMT_TS,SO_LN_ITM_ID,BUS_AREA_CD,REC_EXPLN_CD,PROD_ID,QTA_PROD_LN_ID,ORIG_PROD_ID,SO_ADJ_CD,SUM(DISC_FAC_NR) AS DISC_FAC_NR,AVG(LC_EXCH_RT) AS LC_EXCH_RT,AVG(LC_MTH_3_EXCH_RT) AS LC_MTH_3_EXCH_RT,AVG(LC_MTH_12_EXCH_RT) AS LC_MTH_12_EXCH_RT,AVG(DOC_EXCH_RT) AS DOC_EXCH_RT,AVG(DOC_MTH_3_EXCH_RT) AS DOC_MTH_3_EXCH_RT,AVG(DOC_MTH_12_EXCH_RT) AS DOC_MTH_12_EXCH_RT FROM STG_FIN_RZ_SHIP_MCC_UNB_GROSS_PERCENT_ETL GROUP BY SHIP_ID,SHIP_LN_ITM_ID,SHIP_DT,SO_ID,SRC_SYS_KY,ORD_CRT_DT,EFF_FRM_GMT_TS,SO_LN_ITM_ID,BUS_AREA_CD,REC_EXPLN_CD,PROD_ID,QTA_PROD_LN_ID,ORIG_PROD_ID,SO_ADJ_CD) T2 ON T2.SHIP_ID = T.SHIP_ID AND T2.SHIP_LN_ITM_ID = T.SHIP_LN_ITM_ID AND T2.SHIP_DT = T.SHIP_DT AND T2.SO_ID = T.SO_ID AND T2.SRC_SYS_KY = T.SRC_SYS_KY AND T2.ORD_CRT_DT = T.ORD_CRT_DT AND T2.EFF_FRM_GMT_TS = T.EFF_FRM_GMT_TS AND T2.SO_LN_ITM_ID = T.SO_LN_ITM_ID AND T2.REC_EXPLN_CD = T.REC_EXPLN_CD AND T2.PROD_ID = T.PROD_ID INNER JOIN SRC_SYS_D SSD ON T.SRC_SYS_KY = SSD.SRC_SYS_KY INNER JOIN ADJ_D ADJ_D ON T2.SO_ADJ_CD = ADJ_D.ADJ_CD AND SSD.ROLLUP_DFLT_CO_ID = ADJ_D.SPRN_CO_ID LEFT JOIN FIN_SRVC_BUS_AREA_D T3 ON T2.BUS_AREA_CD = T3.BUS_AREA_CD AND SSD.ROLLUP_DFLT_CO_ID = T3.SPRN_CO_ID LEFT JOIN STG_FIN_RZ_SO_PROD_D_ETL AS STG_FIN_RZ_SHIP_PROD_D_ETL ON CASE WHEN (T.BUS_AREA_CD <> T2.BUS_AREA_CD AND T3.BUS_AREA_CD IS NOT NULL) THEN 'M' || trim(ADJ_D.MISC_CHRG_CD) || '_' || trim(t2.QTA_PROD_LN_ID) ELSE T.PROD_ID END = STG_FIN_RZ_SHIP_PROD_D_ETL.PROD_ID AND STG_FIN_RZ_SHIP_PROD_D_ETL.SPRN_CO_ID = SSD.ROLLUP_DFLT_CO_ID WHERE T.ORD_ADJ_SHIP_AM <> 0 AND (T2.DISC_FAC_NR <> 0 OR T2.DISC_FAC_NR IS NULL) ) STG_TABLE GROUP BY ACCT_MGR_ID,BASE_UNIT_OF_MSR_CD,BIG_DEAL_ID,BILT_CUST_ID,BUS_AREA_CD,BUS_TYPE_CD,CBN_ID,CMRCL_CD,CONTRA_FG,CONV_RT_CD,CONV_TYPE_ID,CROSS_BORD_FG,CUST_APP_CD,CUST_CNTRCT_ID,CUST_PO_ID,CUST_REQD_DLVR_DT,DLVR_MTHD_CD,DOC_CRNCY_CD,DOC_EXCH_RT,DOC_MTH_12_EXCH_RT,DOC_MTH_3_EXCH_RT,DSTRB_CHNL_CD,EFF_FRM_GMT_TS,EFF_TO_GMT_TS,END_CUST_ID,FDW_TRD_INTRA_CO_FG,FIN_CLOSE_DT,FRST_TCHPT_ACTL_SHIP_TS,FUNC_AREA_CD,FUNC_AREA_LVL_7_CD,GL_GRP_ACCT_ID,GOV_CNTRCT_ID,INDNT_FG,INS_GMT_TS,INVN_CLS_CD,LAST_TCHPT_ACTL_SHIP_TS,LC_CD,LC_EXCH_RT,LC_MTH_12_EXCH_RT,LC_MTH_3_EXCH_RT,LEG_SO_ID,LEG_SO_LN_ITM_ID,LEGL_CO_CD,LGCL_DEL_FG,LOAD_JOB_NR,MATL_SLS_UNIT_OF_MSR_CD,MGMT_GEO_CD,OM_SRC_SYS_KY,ORD_CRT_DT,ORD_CRT_DT_YR_MTH_CD,ORD_RSN_CD,UNRSTD_BUS_AREA_CD,ORIG_PROD_ID,PARNT_PROD_ID,PARNT_SO_ID,PARNT_SO_LN_ITM_ID,PAYM_TERM_CD,PRFT_CTR_CD,PRIM_AGNT_ID,PROD_ID,PROJ_ID,PROMO_CD,QTA_CR_SUB_ENT_CD,QTA_PROD_LN_ID,QTA_PROD_LN_OVERR_FG,REC_EXPLN_CD,REC_EXPLN_PRCS_CD,REC_ST_NR,SECND_AGNT_ID,SHIP_DT,SHIP_FRM_PLNT_CD,SHIP_ID,SHIP_LN_ITM_ID,SHIP_POST_TS,SHPT_CUST_ID,SHPT_ISO_CTRY_CD,SLDT_CUST_ID,SLS_CHNL_CD,SLS_FRC_CD,SLS_OFF_CD,SLS_ORG_CD,SLS_REP_CD,SLS_SUB_ENT_CD,SO_ADJ_CD,SO_ID,SO_LN_ITM_ID,SO_MTHD_CD,SO_TYPE_CD,SRC_PROD_LN_ID,SRC_SYS_KY,SRVC_NOTIF_ID,SUPLYG_DIV_SUB_ENT_CD,TEAM_CD_ID,TRSY_BUS_ORG_CD,UPD_GMT_TS,VALU_DLVR_CHAIN_CD,WBS_ID,PRIM_SO_ADJ_CD,BILL_DOC_DT,BILL_DUE_DT,BILL_DOC_ID,MISC_CHRG_CD,PROD_OPT_MCC_CD,SO_ADJ_ALLOC_FG,ACK_CD,SO_CMPGN_TX,EIFFEL_INVN_CLS_CD,SO_MKT_PGM_TX,PKG_PROD_ID,SRVC_GDS_PROD_ID,USE_NM,VALU_VOL_NM,CONSOLIDTD_INV_FG,DRCT_CUST_IND_CD,ENGMT_MDL_CD,REBATE_ORD_RSN_CD,SD_ITM_CATG_CD ;""" 
    ret_val = execute_sql("3", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(3)

    query_to_execute = """/* PARAM_FIN_RZ_L2_SHIP_MAINTAIN_TABLE_s STG_FIN_RZ_SHIP_DTL_CONTRA_DRV_FNL_ETL, UPDATE STATISTICS 'ON NECESSARY COLUMNS'; */ SELECT 1;""" 
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
