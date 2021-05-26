
#
# RL_SM_SLS_TTY_DMNSN.py
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

    query_to_execute = """UPDATE   SLS_TTY_PRNT SET DLT_IND = 'Y' WHERE (SLS_TTY_ID,SRC_SYS_KY,MNT_TS) IN (SELECT SLS_TTY_ID,SRC_SYS_KY, MIN(MNT_TS) AS MNT_TS FROM SLS_TTY_PRNT WHERE concat(SLS_TTY_ID,SRC_SYS_KY, PRNT_SLS_TTY_ID) IN (SELECT concat(SLS_TTY_ID,SRC_SYS_KY, PRNT_SLS_TTY_ID) FROM SLS_TTY_PRNT WHERE SLS_TTY_PRNT_EFF_TS <= CURRENT_TIMESTAMP AND (SLS_TTY_PRNT_XPRY_TS >= CURRENT_TIMESTAMP OR SLS_TTY_PRNT_XPRY_TS IS NULL OR SLS_TTY_PRNT_XPRY_TS = CAST('9999-12-31 00:00:00.000000' AS TIMESTAMP)) AND DLT_IND = 'N' AND HB_ST_CD = '1' GROUP BY 1,2,3 HAVING COUNT(*) > 1  ) AND SLS_TTY_PRNT_EFF_TS <= CURRENT_TIMESTAMP AND (SLS_TTY_PRNT_XPRY_TS >= CURRENT_TIMESTAMP OR SLS_TTY_PRNT_XPRY_TS IS NULL OR SLS_TTY_PRNT_XPRY_TS = CAST('9999-12-31 00:00:00.000000' AS TIMESTAMP)) AND DLT_IND = 'N' AND HB_ST_CD = '1' GROUP BY 1,2  );""" 
    ret_val = execute_sql("2", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(2)

    query_to_execute = """DELETE FROM SLS_TTY_DMNSN;""" 
    ret_val = execute_sql("3", spark_session, spark_context, sql_context, query_to_execute)
    if ret_val is False:
        print("ERROR executing query {query_to_execute}".format(query_to_execute=query_to_execute))
        sys.exit(3)

    query_to_execute = """/* INSERT INTO
  SLS_TTY_DMNSN
SELECT
  SSD.SPRN_CO_ID,
  SSD.SRC_SYS_KY,
  TTY.SLS_TTY_ID,
  TTY.OWN_SLS_ORG_ID,
  ORG.SLS_ORG_LG_NM,
  TTY.SLS_TTY_EST_CO_CNT_RNG_ID,
  RNG.SLS_TTY_EST_CO_CNT_RNG_NM,
  TTY.SLS_TTY_ASSC_RFNC_UID,
  TTY.SLS_TTY_TYP_CD,
  TYP.SLS_TTY_TYP_NM,
  TTY.SLS_TTY_ACCT_RFRSH_TYP_CD,
  RFRSH.SLS_TTY_ACCT_RFRSH_TYP_NM,
  TTY.SLS_TTY_STTS_CD,
  STTS.SLS_TTY_STTS_NM,
  TTY.SLS_TTY_CLNT_TYP_CD,
  CLNT.SLS_TTY_CLNT_TYP_NM,
  TTY.SLS_TTY_CRDT_TYP_CD,
  CRDT.SLS_TTY_CRDT_TYP_NM,
  TTY.SLS_TTY_ASSC_RFNC_UID_TYP_CD,
  UID.SLS_TTY_ASSC_RFNC_UID_TYP_NM,
  TTY.SLS_TTY_XTND_NM,
  TTY.SLS_TTY_SLS_LTR_XTND_NM,
  TTY.SLS_TTY_DN,
  TTY.SLS_TTY_MADO_RL_ACTV_IND,
  TTY.SLS_TTY_WRLD_WDE_IND,
  TTY.SLS_TTY_EFF_TS,
  TTY.SLS_TTY_XPRY_TS,
  NVL(
    STP.SLS_TTY_PRNT_EFF_TS,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  ) AS PRNT_SLS_TTY_EFF_TS,
  NVL(STP.PRNT_SLS_TTY_ID, 0) AS PRNT_SLS_TTY_ID,
  TTY.SLS_TTY_XTND_NM,
  NVL(
    STP.SLS_TTY_PRNT_XPRY_TS,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  ) AS PRNT_SLS_TTY_PRNT_XPRY_TS,
  NVL(STNL.CHR_SCRT_CD, '?') AS CHR_SCRT_CD,
  CS.CHR_SCRT_NM,
  STNL.SLS_TTY_NN_LTN_XTND_NM,
  STNL.SLS_TTY_NN_LTN_RPTG_NM,
  NVL(PFL.SLS_TTY_PFL_LG_SQN_NR, 0) AS SLS_TTY_PFL_LG_SQN_NR,
  NVL(
    PFL.SLS_TTY_PFL_EFF_TS,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  ) AS SLS_TTY_PFL_EFF_TS,
  NVL(
    PFL.SLS_TTY_PFL_XPRY_TS,
    CAST('1900-01-01 00:00:00' AS TIMESTAMP)
  ) AS SLS_TTY_PFL_XPRY_TS,
  NVL(PFL.LEAD_SLS_ROL_TYP_CD, '?') AS SLS_TTY_PFL_LEAD_SLS_ROL_TYP_CD,
  NVL(STG.SLS_TTY_LVL_1_ID, 0) AS SLS_TTY_LVL_1_ID,
  STG.SLS_TTY_LVL_1_XTND_NM,
  NVL(STG.SLS_TTY_LVL_2_ID, 0) AS SLS_TTY_LVL_2_ID,
  STG.SLS_TTY_LVL_2_XTND_NM,
  NVL(STG.SLS_TTY_LVL_3_ID, 0) AS SLS_TTY_LVL_3_ID,
  STG.SLS_TTY_LVL_3_XTND_NM,
  NVL(STG.SLS_TTY_LVL_4_ID, 0) AS SLS_TTY_LVL_4_ID,
  STG.SLS_TTY_LVL_4_XTND_NM,
  NVL(STG.SLS_TTY_LVL_5_ID, 0) AS SLS_TTY_LVL_5_ID,
  STG.SLS_TTY_LVL_5_XTND_NM,
  NVL(STG.SLS_TTY_LVL_6_ID, 0) AS SLS_TTY_LVL_6_ID,
  STG.SLS_TTY_LVL_6_XTND_NM,
  NVL(STG.SLS_TTY_LVL_7_ID, 0) AS SLS_TTY_LVL_7_ID,
  STG.SLS_TTY_LVL_7_XTND_NM,
  NVL(STG.SLS_TTY_LVL_8_ID, 0) AS SLS_TTY_LVL_8_ID,
  STG.SLS_TTY_LVL_8_XTND_NM,
  NVL(STG.SLS_TTY_LVL_9_ID, 0) AS SLS_TTY_LVL_9_ID,
  STG.SLS_TTY_LVL_9_XTND_NM,
  NVL(STG.SLS_TTY_LVL_10_ID, 0) AS SLS_TTY_LVL_10_ID,
  STG.SLS_TTY_LVL_10_XTND_NM,
  TTY.MNT_TS,
  TTY.MNT_SYS_USR_ID,
  CAST(
    TO_TIMESTAMP('PARAM_BATCH_GMT_START_TS_s') AS TIMESTAMP
  ) AS INS_GMT_TS,
  CAST(
    TO_TIMESTAMP('PARAM_BATCH_GMT_START_TS_s') AS TIMESTAMP
  ) AS UPD_GMT_TS,
  'PARAM_BATCH_ID_s' AS LOAD_JOB_NR,
  TTY.STBNG_IND
FROM
  SLS_TTY TTY
  INNER JOIN SRC_SYS_D SSD ON TTY.SRC_SYS_KY = SSD.SRC_SYS_KY
  LEFT OUTER JOIN SLS_TTY_EST_CO_CNT_RNG RNG ON TTY.SLS_TTY_EST_CO_CNT_RNG_ID = RNG.SLS_TTY_EST_CO_CNT_RNG_ID
  AND RNG.DLT_IND = 'N'
  AND RNG.HB_ST_CD = '1'
  AND TTY.SRC_SYS_KY = RNG.SRC_SYS_KY
  LEFT OUTER JOIN SLS_TTY_TYP TYP ON TTY.SLS_TTY_TYP_CD = TYP.SLS_TTY_TYP_CD
  AND TYP.DLT_IND = 'N'
  AND TYP.HB_ST_CD = '1'
  AND TTY.SRC_SYS_KY = TYP.SRC_SYS_KY
  LEFT OUTER JOIN SLS_TTY_ACCT_RFRSH_TYP RFRSH ON TTY.SLS_TTY_ACCT_RFRSH_TYP_CD = RFRSH.SLS_TTY_ACCT_RFRSH_TYP_CD
  AND RFRSH.DLT_IND = 'N'
  AND RFRSH.HB_ST_CD = '1'
  AND TTY.SRC_SYS_KY = RFRSH.SRC_SYS_KY
  LEFT OUTER JOIN SLS_TTY_STTS STTS ON TTY.SLS_TTY_STTS_CD = STTS.SLS_TTY_STTS_CD
  AND STTS.DLT_IND = 'N'
  AND STTS.HB_ST_CD = '1'
  AND TTY.SRC_SYS_KY = STTS.SRC_SYS_KY
  LEFT OUTER JOIN SLS_TTY_CLNT_TYP CLNT ON TTY.SLS_TTY_CLNT_TYP_CD = CLNT.SLS_TTY_CLNT_TYP_CD
  AND CLNT.DLT_IND = 'N'
  AND CLNT.HB_ST_CD = '1'
  AND TTY.SRC_SYS_KY = CLNT.SRC_SYS_KY
  LEFT OUTER JOIN SLS_TTY_CRDT_TYP CRDT ON TTY.SLS_TTY_CRDT_TYP_CD = CRDT.SLS_TTY_CRDT_TYP_CD
  AND CRDT.DLT_IND = 'N'
  AND CRDT.HB_ST_CD = '1'
  AND TTY.SRC_SYS_KY = CRDT.SRC_SYS_KY
  LEFT OUTER JOIN SLS_TTY_ASSC_RFNC_UID_TYP UID ON TTY.SLS_TTY_ASSC_RFNC_UID_TYP_CD = UID.SLS_TTY_ASSC_RFNC_UID_TYP_CD
  AND UID.DLT_IND = 'N'
  AND UID.HB_ST_CD = '1'
  AND TTY.SRC_SYS_KY = UID.SRC_SYS_KY
  LEFT OUTER JOIN SLS_ORG ORG ON TTY.OWN_SLS_ORG_ID = ORG.SLS_ORG_ID
  AND ORG.DLT_IND = 'N'
  AND ORG.HB_ST_CD = '1'
  LEFT OUTER JOIN (
    SELECT
      STP.SLS_TTY_ID,
      STP.PRNT_SLS_TTY_ID,
      STP.SLS_TTY_PRNT_EFF_TS,
      STP.SLS_TTY_PRNT_XPRY_TS,
      stp.SRC_SYS_KY FROM SLS_TTY_PRNT STP,
                        ( SELECT SLS_TTY_ID, MAX(PRNT_SLS_TTY_ID) AS PRNT_SLS_TTY_ID 
						 ,STTP.SRC_SYS_KY
                          FROM SLS_TTY_PRNT STTP
                          WHERE 
                             SLS_TTY_PRNT_EFF_TS <= CURRENT_TIMESTAMP
                          AND (SLS_TTY_PRNT_XPRY_TS >= CURRENT_TIMESTAMP
                              OR SLS_TTY_PRNT_XPRY_TS IS NULL
                              OR SLS_TTY_PRNT_XPRY_TS = TIMESTAMP '9999-12-31 00:00:00.000000')
                          AND DLT_IND = 'N' AND HB_ST_CD = '1' GROUP BY 1 ,STTP.SRC_SYS_KY --
                        )  STP_TEMP 
             WHERE STP.SLS_TTY_ID = STP_TEMP.SLS_TTY_ID 
			 and STP.SRC_SYS_KY =STP_TEMP.SRC_SYS_KY
               AND STP.PRNT_SLS_TTY_ID = STP_TEMP.PRNT_SLS_TTY_ID 
               AND STP.DLT_IND = 'N' AND STP.HB_ST_CD = '1' 
               AND STP.SLS_TTY_PRNT_EFF_TS <= CURRENT_TIMESTAMP   --added on 9/17/2014 avoid duplicate
               AND (STP.SLS_TTY_PRNT_XPRY_TS >= CURRENT_TIMESTAMP    
                              OR STP.SLS_TTY_PRNT_XPRY_TS IS NULL
                              OR STP.SLS_TTY_PRNT_XPRY_TS = TIMESTAMP '9999-12-31 00:00:00.000000')
               --
           ) STP 
     ON TTY.SLS_TTY_ID = STP.SLS_TTY_ID 
    and TTY.SRC_SYS_KY = STP.SRC_SYS_KY	 
LEFT OUTER JOIN (
           SELECT 
               STNL.SLS_TTY_ID
              ,STNL.CHR_SCRT_CD
              ,STNL.SLS_TTY_NN_LTN_XTND_NM
              ,STNL.SLS_TTY_NN_LTN_RPTG_NM
			  ,SRC_SYS_KY
           FROM
                 SLS_TTY_NN_LTN  STNL 
           WHERE (SLS_TTY_ID,SRC_SYS_KY,CHR_SCRT_CD)  IN 
                         (SELECT SLS_TTY_ID,SRC_SYS_KY,MAX(CHR_SCRT_CD) AS CHR_SCRT_CD 
                          FROM SLS_TTY_NN_LTN 
                          GROUP BY 1,2 --
                          )
                 AND STNL.DLT_IND = 'N' AND STNL.HB_ST_CD = '1'
          --
           ) STNL
   ON TTY.SLS_TTY_ID = STNL.SLS_TTY_ID
   and TTY.SRC_SYS_KY = STNL.SRC_SYS_KY
LEFT OUTER JOIN CHR_SCRT  CS ON STNL.CHR_SCRT_CD = CS.CHR_SCRT_CD AND CS.DLT_IND = 'N' AND CS.HB_ST_CD='1'
LEFT OUTER JOIN (
             SELECT 
                 SLS_TTY_ID
                ,SLS_TTY_PFL_LG_SQN_NR
                ,SLS_TTY_PFL_EFF_TS
                ,SLS_TTY_PFL_XPRY_TS
                ,LEAD_SLS_ROL_TYP_CD
				,SRC_SYS_KY
            FROM 
               SLS_TTY_PFL 
            WHERE (SLS_TTY_ID,SRC_SYS_KY,SLS_TTY_PFL_LG_SQN_NR) IN
                (
                SELECT  SLS_TTY_ID,SRC_SYS_KY,MAX(SLS_TTY_PFL_LG_SQN_NR) AS SLS_TTY_PFL_LG_SQN_NR
                FROM SLS_TTY_PFL 
                WHERE  DLT_IND = 'N' AND HB_ST_CD= '1' 
                GROUP BY 1,2 --
                 )
            AND DLT_IND = 'N' AND HB_ST_CD='1'
            --
                ) PFL                
               ON TTY.SLS_TTY_ID = PFL.SLS_TTY_ID
			   and TTY.SRC_SYS_KY = PFL.SRC_SYS_KY
LEFT OUTER JOIN STG_SLS_TTY_HRCHY_ETL STG ON TTY.SLS_TTY_ID = STG.SLS_TTY_ID AND TTY.SRC_SYS_KY = STG.SRS_SYS_KY WHERE TTY.DLT_IND = 'N' AND TTY.HB_ST_CD = '1'-- ;
-- 1 FOR TABLE SLS_TTY_DMNSN ON EVERY COLUMN SAMPLE; */ SELECT 1;""" 
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
