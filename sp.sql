create or replace PROCEDURE edp_consumer.dw_bld.CGI_PROC_CUST_GROUP_RQST() 
returns varchar
language sql
as
$$
/*
  ** ObjectName: DW_BLD.CGI_PROC_CUST_GROUP_RQST
  **
  ** Parameters: None
  **
  ** Returns:  None
  **
  ** Description: Apply customer grouping request in the order.
  **
  ** Revision History
  ** ----------------------------------------------------------------------------
  **  Date        Name        Description
  ** ----------------------------------------------------------------------------
  ** 04/16/14     RG         Initial Version
  ** 06/30/14     RG         Add abc_logger, Add Header, Add column name in insert statement
  ** 02/07/20     FJ         Updates for rule change:
  **                         Do not allow reassignment if Workout Accounts are found and IP has a Group ID assigned.
  */
  declare
V_INVOLVED_PARTY_ID_CGR VARCHAR(60);
V_BEG_EFCTV_DT DATE;
V_INVOLVED_PARTY_EXISTS VARCHAR(60);
V_GROUP_NAME_CG     VARCHAR(255);
V_GROUP_ID_CGR      NUMBER(10);
V_GROUP_ID_CGR_NEW  NUMBER(10);
V_GROUP_ID_CG       NUMBER(10);
v_CURRENT_IND_CG    VARCHAR(10);
V_CURRENT_BATCH_DATE DATE;
V_MAX_GROUP_ID NUMBER(10);
V_COUNT NUMBER(10) DEFAULT 0;
  -- ROW BY ROW PROCESSING DONE USING CURSOR
SCGR CURSOR for
SELECT SP.REQUEST_NUM,
SP.CIF_NUMBER,
SP.REQUEST_TYPE,
SP.COMMENTS,
SP.GROUP_ID,
SP.GROUP_WITH_CIF_NBR,
UPPER(SP.GROUP_NAME) GROUP_NAME,
SP.REQUESTOR_ID,
SP.REQUEST_TS,
SP.DW_INSERT_DTTM,
SP.DW_UPD_DTTM,
SP.REQUEST_ID,
SP.CUST_NAME,
SP.INVOLVED_PARTY_ID,
SP.GROUP_WITH_INVOLVED_PARTY_ID
from DM_CUST_GROUP.ISA_CGI_CUST_GROUP_RQST SP
LEFT OUTER JOIN DM_CUST_GROUP.CUST_GROUP_RQST CGR
ON SP.REQUEST_NUM      = CGR.REQUEST_NUM
WHERE CGR.REQUEST_NUM IS NULL
ORDER BY SP.REQUEST_TS;
BEGIN
  --dw_bld.abc_logger.start_timed_event('CGI - Grouping Request Process started ','TIMER1');
  SELECT CURRENT_DATE INTO :V_CURRENT_BATCH_DATE;
  FOR I IN SCGR
  LOOP
    V_CURRENT_BATCH_DATE:=V_CURRENT_BATCH_DATE+1/86400;
    V_COUNT:=V_COUNT+1;
    BEGIN
      BEGIN
        V_INVOLVED_PARTY_EXISTS:=NULL;
        SELECT INVOLVED_PARTY_ID
        INTO V_INVOLVED_PARTY_EXISTS
        FROM DM_CUST_GROUP.ISA_CGI_INVOLVED_PARTY
        WHERE INVOLVED_PARTY_ID=I.INVOLVED_PARTY_ID;
      EXCEPTION
      WHEN OTHER THEN
        NULL;
      END;
      -- Verify that the customer (CIF_Nbr) exists in the data warehouse; if not, set the fail reason on the request row (Grouping_Request.Fail_Reason) to ?CIF Number does not exist in the data warehouse?
      IF V_INVOLVED_PARTY_EXISTS IS NULL THEN
        BEGIN
          INSERT
          INTO DM_CUST_GROUP.CUST_GROUP_RQST( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
          VALUES
            (
              I.REQUEST_NUM,
              I.CIF_NUMBER,
              I.REQUEST_TYPE,
              I.COMMENTS,
              I.GROUP_ID,
              I.GROUP_WITH_CIF_NBR,
              I.GROUP_NAME,
              I.REQUESTOR_ID,
              I.REQUEST_TS,
              21,
              v_CURRENT_BATCH_DATE,
              v_CURRENT_BATCH_DATE,
              v_CURRENT_BATCH_DATE,
              I.REQUEST_ID
            );
        EXCEPTION
        WHEN OTHER THEN
          --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
          --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
        END;
      ELSE
      -- Verify that a request type (Request_Type) was specified; if not, set the fail reason to ?Request type not specified?
      IF I.REQUEST_TYPE IS NULL THEN
        BEGIN
          INSERT
          INTO DM_CUST_GROUP.CUST_GROUP_RQST( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
          VALUES
            (
              I.REQUEST_NUM,
              I.CIF_NUMBER,
              I.REQUEST_TYPE,
              I.COMMENTS,
              I.GROUP_ID,
              I.GROUP_WITH_CIF_NBR,
              I.GROUP_NAME,
              I.REQUESTOR_ID,
              I.REQUEST_TS,
              22,
              v_CURRENT_BATCH_DATE,
              v_CURRENT_BATCH_DATE,
              v_CURRENT_BATCH_DATE,
              I.REQUEST_ID
            );
        EXCEPTION
        WHEN OTHER THEN
          --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
          --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
        EXIT;
        END ;
      ELSE
      -- If the request type is ?1 Move customer to specified group?
      IF I.REQUEST_TYPE=1 THEN
        BEGIN
          -- Verify that the customer group ID (Group_ID) was specified; if not, set the fail reason to ?Customer Group ID must be specified for request?
          IF I.GROUP_ID IS NULL OR I.GROUP_ID = '' THEN
            BEGIN
              INSERT
              INTO DM_CUST_GROUP.CUST_GROUP_RQST( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
              VALUES
                (
                  I.REQUEST_NUM,
                  I.CIF_NUMBER,
                  I.REQUEST_TYPE,
                  I.COMMENTS,
                  I.GROUP_ID,
                  I.GROUP_WITH_CIF_NBR,
                  I.GROUP_NAME,
                  I.REQUESTOR_ID,
                  I.REQUEST_TS,
                  23,
                  v_CURRENT_BATCH_DATE,
                  v_CURRENT_BATCH_DATE,
                  v_CURRENT_BATCH_DATE,
                  I.REQUEST_ID
                );
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
              EXIT;
            END;
            --  If the customer group ID was specified
          ELSE
            --  Verify that the customer (CIF_Nbr) is not already assigned to the specified customer group; if so, set the fail reason to ?Customer already assigned to the requested  customer group?
            BEGIN
              v_GROUP_ID_CGR:=NULL;
              SELECT GROUP_ID
              INTO v_GROUP_ID_CGR
              FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
              WHERE INVOLVED_PARTY_ID   =I.INVOLVED_PARTY_ID
 AND CURRENT_IND='Y';
            EXCEPTION
            WHEN OTHER THEN
              NULL;
            END;
            IF TO_CHAR(V_GROUP_ID_CGR) =I.GROUP_ID THEN
              BEGIN
                INSERT
                INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
                VALUES
                  (
                    I.REQUEST_NUM,
                    I.CIF_NUMBER,
                    I.REQUEST_TYPE,
                    I.COMMENTS,
                    I.GROUP_ID,
                    I.GROUP_WITH_CIF_NBR,
                    I.GROUP_NAME,
                    I.REQUESTOR_ID,
                    I.REQUEST_TS,
                    24,
                    v_CURRENT_BATCH_DATE,
                    V_CURRENT_BATCH_DATE,
                    v_CURRENT_BATCH_DATE,
                    I.REQUEST_ID
                  ) ;
              EXCEPTION
              WHEN OTHER THEN
                --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                EXIT;
              END;
            ELSE
              -- Verify that the customer group ID exists on the Customer_Group table; if not, set the fail reason to ?Customer Group ID does not exist?
              BEGIN
                V_GROUP_ID_CG   :=NULL;
                V_CURRENT_IND_CG:=NULL;
                SELECT GROUP_ID,
                  CURRENT_IND
                INTO V_GROUP_ID_CG,
                  V_CURRENT_IND_CG
                FROM DM_CUST_GROUP.CUST_GROUP
                WHERE TO_CHAR(GROUP_ID)  =I.GROUP_ID
                AND BEG_EFCTV_DT=
                  (SELECT MAX(BEG_EFCTV_DT)
                  FROM DM_CUST_GROUP.CUST_GROUP
                  WHERE TO_CHAR(GROUP_ID)=I.GROUP_ID
                  ) ;
              EXCEPTION
              WHEN OTHER THEN
                NULL;
              END;
              IF V_GROUP_ID_CG IS NULL THEN
                BEGIN
                  INSERT
                  INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
                  VALUES
                    (
                      I.REQUEST_NUM,
                      I.CIF_NUMBER,
                      I.REQUEST_TYPE,
                      I.COMMENTS,
                      I.GROUP_ID,
                      I.GROUP_WITH_CIF_NBR,
                      I.GROUP_NAME,
                      I.REQUESTOR_ID,
                      I.REQUEST_TS,
                      25,
                      v_CURRENT_BATCH_DATE,
                      v_CURRENT_BATCH_DATE,
                      v_CURRENT_BATCH_DATE,
                      I.REQUEST_ID
                    );
                EXCEPTION
                WHEN OTHER THEN
                  --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                  --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                  EXIT;
                END ;
              ELSE
                -- Verify that the customer group ID is valid for assignment*; if not, set the fail reason to ?Customer can only be assigned to an inactive Customer Group ID if the customer was previously in the group?
                BEGIN
                  V_GROUP_ID_CGR:=NULL;
                  SELECT DISTINCT GROUP_ID
                  INTO V_GROUP_ID_CGR
                  FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                  WHERE INVOLVED_PARTY_ID =I.INVOLVED_PARTY_ID
                  AND TO_CHAR(GROUP_ID)    =I.GROUP_ID;
                EXCEPTION
                WHEN OTHER THEN
                  NULL;
                END;
                IF (V_CURRENT_IND_CG='N' AND V_GROUP_ID_CGR IS NULL) THEN
                  BEGIN
                    INSERT
                    INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
                    VALUES
                      (
                        I.REQUEST_NUM,
                        I.CIF_NUMBER,
                        I.REQUEST_TYPE,
                        I.COMMENTS,
                        I.GROUP_ID,
                        I.GROUP_WITH_CIF_NBR,
                        I.GROUP_NAME,
                        I.REQUESTOR_ID,
                        I.REQUEST_TS,
                        26,
                        v_CURRENT_BATCH_DATE,
                        v_CURRENT_BATCH_DATE,
                        v_CURRENT_BATCH_DATE,
                        I.REQUEST_ID
                      );
                  EXCEPTION
                  WHEN OTHER THEN
                    --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                    --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                    EXIT;
                  END ;
                ELSE
                  --  If the request is valid
                  BEGIN
                    V_INVOLVED_PARTY_ID_CGR:=NULL;
                    V_BEG_EFCTV_DT  :=NULL;
                    V_GROUP_ID_CGR  :=NULL;
                    SELECT INVOLVED_PARTY_ID,
                      BEG_EFCTV_DT,
                      GROUP_ID
                    INTO V_INVOLVED_PARTY_ID_CGR,
                      V_BEG_EFCTV_DT,
                      V_GROUP_ID_CGR
                    FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                    WHERE INVOLVED_PARTY_ID=I.INVOLVED_PARTY_ID
                    AND CURRENT_IND ='Y';
                  EXCEPTION
                  WHEN OTHER THEN
                    NULL;
                  END;
                  --  If the customer is already assigned to a customer group
                  IF V_GROUP_ID_CGR IS NOT NULL THEN
                    BEGIN
                      -- Terminate the existing customer group relationship
                      UPDATE DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                      SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                        CURRENT_IND    ='N',
                        DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                        REQUEST_NUM    =I.REQUEST_NUM
                      WHERE INVOLVED_PARTY_ID =V_INVOLVED_PARTY_ID_CGR
                      AND BEG_EFCTV_DT =V_BEG_EFCTV_DT;
                      BEGIN
                        V_GROUP_ID_CG:=NULL;
                        SELECT DISTINCT GROUP_ID
                        INTO V_GROUP_ID_CG
                        FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                        WHERE GROUP_ID =V_GROUP_ID_CGR
                        AND INVOLVED_PARTY_ID<>V_INVOLVED_PARTY_ID_CGR
                        AND CURRENT_IND='Y';
                      EXCEPTION
                      WHEN OTHER THEN
                        NULL;
                      END;
                      -- If no other customers are associated to the customer group, inactivate the customer group row
                      IF V_GROUP_ID_CG IS NULL THEN
                        UPDATE DM_CUST_GROUP.CUST_GROUP
                        SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                          CURRENT_IND    ='N',
                          DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                          REQUEST_NUM    =I.REQUEST_NUM
                        WHERE GROUP_ID   =V_GROUP_ID_CGR
                        AND CURRENT_IND  ='Y';
                      END IF;
                    EXCEPTION
                    WHEN OTHER THEN
                      --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                      --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert/update for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                      EXIT;
                    END;
                  END IF;
                  BEGIN
                    -- Insert a new customer group relationship row for the customer
                    INSERT
                    INTO DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP( INVOLVED_PARTY_ID,CIF_NUMBER,GROUP_ID,BEG_EFCTV_DT,END_EFCTV_DT,CURRENT_IND,REQUEST_NUM,DW_INSERT_DTTM,DW_UPD_DTTM)
                    VALUES
                      (
                        I.INVOLVED_PARTY_ID,
I.CIF_NUMBER,
                        I.GROUP_ID,
                        V_CURRENT_BATCH_DATE,
                        TO_DATE('31-12-9999','DD-MM-YYYY'),
                        'Y',
                        I.REQUEST_NUM,
                        V_CURRENT_BATCH_DATE,
                        V_CURRENT_BATCH_DATE
                      );
                    INSERT
                    INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
                    VALUES
                      (
                        I.REQUEST_NUM,
                        I.CIF_NUMBER,
                        I.REQUEST_TYPE,
                        I.COMMENTS,
                        I.GROUP_ID,
                        I.GROUP_WITH_CIF_NBR,
                        I.GROUP_NAME,
                        I.REQUESTOR_ID,
                        I.REQUEST_TS,
                        99,
                        V_CURRENT_BATCH_DATE,
                        V_CURRENT_BATCH_DATE,
                        V_CURRENT_BATCH_DATE,
                        I.REQUEST_ID
                      );
                  EXCEPTION
                  WHEN OTHER THEN
                    --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                    --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert/update for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                    EXIT;
                  END;
                  BEGIN
                    BEGIN
                      V_GROUP_ID_CG  :=NULL;
                      V_GROUP_NAME_CG:=NULL;
                      SELECT GROUP_ID,
                        GROUP_NAME
                      INTO V_GROUP_ID_CG,
                        V_GROUP_NAME_CG
                      FROM DM_CUST_GROUP.CUST_GROUP
                      WHERE TO_CHAR(GROUP_ID)  =I.GROUP_ID
                      AND CURRENT_IND ='N'
                      AND BEG_EFCTV_DT=
                        (SELECT MAX(BEG_EFCTV_DT)
                        FROM DM_CUST_GROUP.CUST_GROUP
                        WHERE TO_CHAR(GROUP_ID)=I.GROUP_ID
                        ) ;
                    EXCEPTION
                    WHEN OTHER THEN
                      NULL;
                    END;
                    IF V_GROUP_ID_CG IS NOT NULL THEN
                      -- If the newly associated customer group is currently inactive, re-activate the customer group row by inserting a new customer group row, setting the group ID and name to the values from the most recent inactive row which exists for the customer group.
                      INSERT
                      INTO DM_CUST_GROUP.CUST_GROUP( GROUP_ID,BEG_EFCTV_DT,END_EFCTV_DT,CURRENT_IND,REQUEST_NUM,GROUP_NAME,DW_INSERT_DTTM,DW_UPD_DTTM)
                      VALUES
                        (
                          I.GROUP_ID,
                          V_CURRENT_BATCH_DATE,
                          TO_DATE('31-12-9999','DD-MM-YYYY'),
                          'Y',
                          I.REQUEST_NUM,
                          V_GROUP_NAME_CG,
                          V_CURRENT_BATCH_DATE,
                          V_CURRENT_BATCH_DATE
                        );
                    END IF;
                  EXCEPTION
                  WHEN OTHER THEN
                    --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                    --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert/update for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                    EXIT;
                  END;
                END IF;
              END IF;
            END IF;
          END IF;
        END;
        -- If the request type is ?2 Group customer with specified customer?
      ELSIF I.REQUEST_TYPE=2 THEN
        --Verify that the ?group with? customer number (Group_With_CIF_Number) was specified; if not, set the fail reason to ?Group With CIF Number must be specified for request'
        IF I.GROUP_WITH_INVOLVED_PARTY_ID IS NULL THEN
          BEGIN
            INSERT
            INTO DM_CUST_GROUP.CUST_GROUP_RQST( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
            VALUES
              (
                I.REQUEST_NUM,
                I.CIF_NUMBER,
                I.REQUEST_TYPE,
                I.COMMENTS,
                I.GROUP_ID,
                I.GROUP_WITH_CIF_NBR,
                I.GROUP_NAME,
                I.REQUESTOR_ID,
                I.REQUEST_TS,
                28,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                I.REQUEST_ID
              );
          EXCEPTION
          WHEN OTHER THEN
            --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
            --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
            EXIT;
          END;
          --Verify that the ?group with? customer exists as a valid CIF_Nbr in the data warehouse; if not, set the fail reason to ?Group With CIF Number does not exist?
        ELSE
          BEGIN
            V_INVOLVED_PARTY_EXISTS:=NULL;
            SELECT INVOLVED_PARTY_ID
            INTO V_INVOLVED_PARTY_EXISTS
            FROM DM_CUST_GROUP.ISA_CGI_INVOLVED_PARTY
            WHERE INVOLVED_PARTY_ID=I.GROUP_WITH_INVOLVED_PARTY_ID;
          EXCEPTION
          WHEN OTHER THEN
            NULL ;
          END;
          IF V_INVOLVED_PARTY_EXISTS IS NULL THEN
            BEGIN
              INSERT
              INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
              VALUES
                (
                  I.REQUEST_NUM,
                  I.CIF_NUMBER,
                  I.REQUEST_TYPE,
                  I.COMMENTS,
                  I.GROUP_ID,
                  I.GROUP_WITH_CIF_NBR,
                  I.GROUP_NAME,
                  I.REQUESTOR_ID,
                  I.REQUEST_TS,
                  29,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  I.REQUEST_ID
                );
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
              EXIT;
            END;
            --Verify that the ?group with? customer is assigned to a customer group; if not, set the fail reason to ?Group With customer not assigned to a customer group?
          ELSE
            BEGIN
              V_GROUP_ID_CGR:=NULL;
              SELECT GROUP_ID
              INTO V_GROUP_ID_CGR
              FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
              WHERE INVOLVED_PARTY_ID=I.GROUP_WITH_INVOLVED_PARTY_ID
              AND CURRENT_IND ='Y';
            EXCEPTION
            WHEN OTHER THEN
              NULL ;
            END;
            IF V_GROUP_ID_CGR IS NULL THEN
              BEGIN
                INSERT
                INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
                VALUES
                  (
                    I.REQUEST_NUM,
                    I.CIF_NUMBER,
                    I.REQUEST_TYPE,
                    I.COMMENTS,
                    I.GROUP_ID,
                    I.GROUP_WITH_CIF_NBR,
                    I.GROUP_NAME,
                    I.REQUESTOR_ID,
                    I.REQUEST_TS,
                    30,
                    V_CURRENT_BATCH_DATE,
                    V_CURRENT_BATCH_DATE,
                    V_CURRENT_BATCH_DATE,
                    I.REQUEST_ID
                  );
              EXCEPTION
              WHEN OTHER THEN
                --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                EXIT;
              END;
              --If both the customer and the ?group with? customer are assigned to customer groups, ensure that the groups are different; if not, update the fail reason on the request row to ?Customer already in requested customer group?
            ELSE
              BEGIN
                V_GROUP_ID_CGR_NEW:=NULL;
                SELECT GROUP_ID
                INTO V_GROUP_ID_CGR_NEW
                FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                WHERE INVOLVED_PARTY_ID=I.INVOLVED_PARTY_ID
                AND CURRENT_IND ='Y';
              EXCEPTION
              WHEN OTHER THEN
                NULL ;
              END;
              IF V_GROUP_ID_CGR_NEW=V_GROUP_ID_CGR THEN
                BEGIN
                  INSERT
                  INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
                  VALUES
                    (
                      I.REQUEST_NUM,
                      I.CIF_NUMBER,
                      I.REQUEST_TYPE,
                      I.COMMENTS,
                      I.GROUP_ID,
                      I.GROUP_WITH_CIF_NBR,
                      I.GROUP_NAME,
                      I.REQUESTOR_ID,
                      I.REQUEST_TS,
                      31,
                      V_CURRENT_BATCH_DATE,
                      V_CURRENT_BATCH_DATE,
                      V_CURRENT_BATCH_DATE,
                      I.REQUEST_ID
                    );
                EXCEPTION
                WHEN OTHER THEN
                  --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                  --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
                  EXIT;
                END;
              ELSE
                --If the customer is already assigned to a customer group
                BEGIN
                  V_INVOLVED_PARTY_ID_CGR:=NULL;
                  V_BEG_EFCTV_DT  :=NULL;
                  V_GROUP_ID_CGR  :=NULL;
                  SELECT INVOLVED_PARTY_ID,
                    BEG_EFCTV_DT,
                    GROUP_ID
                  INTO V_INVOLVED_PARTY_ID_CGR,
                    V_BEG_EFCTV_DT,
                    V_GROUP_ID_CGR
                  FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                  WHERE INVOLVED_PARTY_ID=I.INVOLVED_PARTY_ID
                  AND CURRENT_IND ='Y';
                EXCEPTION
                WHEN OTHER THEN
                  NULL ;
                END;
                IF V_GROUP_ID_CGR IS NOT NULL THEN
                  BEGIN
                    --terminate the existing customer group relationship
                    UPDATE DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                    SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                      CURRENT_IND    ='N',
                      DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                      REQUEST_NUM    =I.REQUEST_NUM
                    WHERE INVOLVED_PARTY_ID =V_INVOLVED_PARTY_ID_CGR
                    AND CURRENT_IND  = 'Y';
                    --if no other customers are associated to the customer group, inactivate the customer group row
                    BEGIN
                      V_GROUP_ID_CG:=NULL;
                      SELECT DISTINCT GROUP_ID
                      INTO V_GROUP_ID_CG
                      FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                      WHERE GROUP_ID =V_GROUP_ID_CGR
                      AND INVOLVED_PARTY_ID<>V_INVOLVED_PARTY_ID_CGR
                      AND CURRENT_IND='Y';
                    EXCEPTION
                    WHEN OTHER THEN
                      NULL ;
                    END;
                    IF V_GROUP_ID_CG IS NULL THEN
                      UPDATE DM_CUST_GROUP.CUST_GROUP
                      SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                        CURRENT_IND    ='N',
                        DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                        REQUEST_NUM    =I.REQUEST_NUM
                      WHERE GROUP_ID   =V_GROUP_ID_CGR
                      AND CURRENT_IND  = 'Y';
                    END IF;
                  EXCEPTION
                  WHEN OTHER THEN
                    --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                    --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert/update for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
                    return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
EXIT;
                  END;
                END IF;
                BEGIN
                  BEGIN
                    V_GROUP_ID_CGR:=NULL;
                    SELECT GROUP_ID
                    INTO V_GROUP_ID_CGR
                    FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                    WHERE INVOLVED_PARTY_ID=I.GROUP_WITH_INVOLVED_PARTY_ID
                    AND CURRENT_IND ='Y';
                  EXCEPTION
                  WHEN OTHER THEN
                    NULL ;
                  END;
                  --Insert a new customer group relationship row for the customer
                  INSERT
                  INTO DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP( INVOLVED_PARTY_ID,CIF_NUMBER,GROUP_ID,BEG_EFCTV_DT,END_EFCTV_DT,CURRENT_IND,REQUEST_NUM,DW_INSERT_DTTM,DW_UPD_DTTM)
                  VALUES
                    (
                      I.INVOLVED_PARTY_ID,
 I.CIF_NUMBER,
                      V_GROUP_ID_CGR,
                      V_CURRENT_BATCH_DATE,
                      TO_DATE('31-12-9999','DD-MM-YYYY'),
                      'Y',
                      I.REQUEST_NUM,
                      V_CURRENT_BATCH_DATE,
                      V_CURRENT_BATCH_DATE
                    );
                  --Update the group ID on the request row (Grouping_Request.Group_ID) with the customer group ID associated to the ?group with? customer
                  INSERT
                  INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
                  VALUES
                    (
                      I.REQUEST_NUM,
                      I.CIF_NUMBER,
                      I.REQUEST_TYPE,
                      I.COMMENTS,
                      V_GROUP_ID_CGR,
                      I.GROUP_WITH_CIF_NBR,
                      I.GROUP_NAME,
                      I.REQUESTOR_ID,
                      I.REQUEST_TS,
                      99,
                      V_CURRENT_BATCH_DATE,
                      V_CURRENT_BATCH_DATE,
                      V_CURRENT_BATCH_DATE,
                      I.REQUEST_ID
                    );
                EXCEPTION
                WHEN OTHER THEN
                  --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
                  --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert/update for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
                END;
              END IF;
            END IF;
          END IF;
        END IF;
        --If the request type is ?3 Move customer to new group?
      ELSIF I.REQUEST_TYPE=3 THEN
        BEGIN
          --Identify If the customer is already assigned to a customer group
          BEGIN
            V_INVOLVED_PARTY_ID_CGR:=NULL;
            V_BEG_EFCTV_DT  :=NULL;
            V_GROUP_ID_CGR  :=NULL;
            SELECT INVOLVED_PARTY_ID,
              BEG_EFCTV_DT,
              GROUP_ID
            INTO V_INVOLVED_PARTY_ID_CGR,
              V_BEG_EFCTV_DT,
              V_GROUP_ID_CGR
            FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
            WHERE INVOLVED_PARTY_ID=I.INVOLVED_PARTY_ID
            AND CURRENT_IND ='Y';
          EXCEPTION
          WHEN OTHER THEN
            NULL ;
          END;
          --Idenfity If the customer is assigned to an active account which is classified as workout line of business
          BEGIN
            V_INVOLVED_PARTY_ID_CGR:=NULL;
            SELECT DISTINCT IP.INVOLVED_PARTY_ID
            INTO V_INVOLVED_PARTY_ID_CGR
            FROM DM_CUST_GROUP.ISA_CGI_IP_POP_COST_CENTER IP
            WHERE IP.INVOLVED_PARTY_ID = I.INVOLVED_PARTY_ID
            AND IP.CM3_03_ID ='CREDIT_LN';
           EXCEPTION
          WHEN OTHER THEN
            NULL ;
          END;
          --Do not allow reassignment if Workout Account are found and IP has a Group ID assigned.
          IF V_INVOLVED_PARTY_ID_CGR IS NOT NULL and V_GROUP_ID_CGR IS NOT NULL THEN
            BEGIN
              INSERT
              INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
              VALUES
                (
                  I.REQUEST_NUM,
                  I.CIF_NUMBER,
                  I.REQUEST_TYPE,
                  I.COMMENTS,
                  I.GROUP_ID,
                  I.GROUP_WITH_CIF_NBR,
                  I.GROUP_NAME,
                  I.REQUESTOR_ID,
                  I.REQUEST_TS,
                  27,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  I.REQUEST_ID
                );
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
            END;
          ELSE

          IF V_GROUP_ID_CGR IS NOT NULL THEN
            BEGIN
              --terminate the existing customer group relationship
              UPDATE DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
              SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                CURRENT_IND    ='N',
                DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                REQUEST_NUM    =I.REQUEST_NUM
              WHERE INVOLVED_PARTY_ID =I.INVOLVED_PARTY_ID
              AND CURRENT_IND  ='Y';
              --if no other customers are associated to the customer group, inactivate the customer group row
              BEGIN
                V_GROUP_ID_CG:=NULL;
                SELECT DISTINCT GROUP_ID
                INTO V_GROUP_ID_CG
                FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
                WHERE GROUP_ID =V_GROUP_ID_CGR
                AND INVOLVED_PARTY_ID<>I.INVOLVED_PARTY_ID
                AND CURRENT_IND='Y';
              EXCEPTION
              WHEN OTHER THEN
                NULL ;
              END;
              IF V_GROUP_ID_CG IS NULL THEN
                UPDATE DM_CUST_GROUP.CUST_GROUP
                SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                  CURRENT_IND    ='N',
                  DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                  REQUEST_NUM    =I.REQUEST_NUM
                WHERE GROUP_ID   =V_GROUP_ID_CGR
                AND CURRENT_IND  ='Y';
              END IF;
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert/update for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
            END;
          END IF;
          -- Insert a new customer group row: Set the group ID to the next available number
          BEGIN
            BEGIN
              V_MAX_GROUP_ID:=NULL;
              SELECT MAX(GROUP_ID) + 1
              INTO V_MAX_GROUP_ID
              FROM
                (SELECT MAX(GROUP_ID) AS GROUP_ID FROM DM_CUST_GROUP.CUST_GROUP
                UNION
                SELECT MAX(GROUP_ID) AS GROUP_ID FROM DM_CUST_GROUP.CUST_GROUP_EXCPTN
                );
            EXCEPTION
            WHEN OTHER THEN
              NULL;
            END;
            INSERT
            INTO DM_CUST_GROUP.CUST_GROUP( GROUP_ID,BEG_EFCTV_DT,END_EFCTV_DT,CURRENT_IND,REQUEST_NUM,GROUP_NAME,DW_INSERT_DTTM,DW_UPD_DTTM)
            VALUES
              (
                V_MAX_GROUP_ID,
                V_CURRENT_BATCH_DATE,
                TO_DATE('31-12-9999','DD-MM-YYYY'),
                'Y',
                I.REQUEST_NUM,
                CASE
                  WHEN I.GROUP_NAME IS NULL
                  THEN UPPER(I.CUST_NAME)
                  ELSE I.GROUP_NAME
                END ,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE
              );
            --Insert a new customer group relationship row for the customer
            INSERT
            INTO DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP( INVOLVED_PARTY_ID,CIF_NUMBER,GROUP_ID,BEG_EFCTV_DT,END_EFCTV_DT,CURRENT_IND,REQUEST_NUM,DW_INSERT_DTTM,DW_UPD_DTTM)
            VALUES
              (
                I.INVOLVED_PARTY_ID,
I.CIF_NUMBER,
                V_MAX_GROUP_ID,
                V_CURRENT_BATCH_DATE,
                TO_DATE('31-12-9999','DD-MM-YYYY'),
                'Y',
                I.REQUEST_NUM,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE
              );
            --Update the group ID on the request row (Grouping_Request.Group_ID) with the number of the new customer group
            INSERT
            INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
            VALUES
              (
                I.REQUEST_NUM,
                I.CIF_NUMBER,
                I.REQUEST_TYPE,
                I.COMMENTS,
                V_MAX_GROUP_ID,
                I.GROUP_WITH_CIF_NBR,
                I.GROUP_NAME,
                I.REQUESTOR_ID,
                I.REQUEST_TS,
                99,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                I.REQUEST_ID
              );
          EXCEPTION
          WHEN OTHER THEN
            --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
            --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
EXIT;
          END;
          END IF;
        END;
      ELSIF I.REQUEST_TYPE=4 THEN
        -- Verify that the new customer group name (Group_Name) was specified; if not, set the fail reason to ?Customer Group Name must be specified for request?
        IF I.GROUP_NAME IS NULL THEN
          BEGIN
            INSERT
            INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
            VALUES
              (
                I.REQUEST_NUM,
                I.CIF_NUMBER,
                I.REQUEST_TYPE,
                I.COMMENTS,
                V_MAX_GROUP_ID,
                I.GROUP_WITH_CIF_NBR,
                I.GROUP_NAME,
                I.REQUESTOR_ID,
                I.REQUEST_TS,
                32,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                I.REQUEST_ID
              );
          EXCEPTION
          WHEN OTHER THEN
            --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
            --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
EXIT;
          END;
        ELSE
          --Verify that the customer (CIF_Number) is assigned to a customer group; if not, set the fail reason to ?Customer not assigned to a customer group?
          BEGIN
            V_GROUP_ID_CGR:=NULL;
            SELECT GROUP_ID
            INTO V_GROUP_ID_CGR
            FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
            WHERE INVOLVED_PARTY_ID=I.INVOLVED_PARTY_ID
            AND CURRENT_IND ='Y';
          EXCEPTION
          WHEN OTHER THEN
            NULL;
          END;
          IF V_GROUP_ID_CGR IS NULL THEN
            BEGIN
              INSERT
              INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
              VALUES
                (
                  I.REQUEST_NUM,
                  I.CIF_NUMBER,
                  I.REQUEST_TYPE,
                  I.COMMENTS,
                  V_MAX_GROUP_ID,
                  I.GROUP_WITH_CIF_NBR,
                  I.GROUP_NAME,
                  I.REQUESTOR_ID,
                  I.REQUEST_TS,
                  33,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  I.REQUEST_ID
                );
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
            END;
            --If the request is valid
          ELSE
            --Update the group ID on the request row (Grouping_Request.Group_ID) with the customer group ID associated to the customer
            BEGIN
              INSERT
              INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
              VALUES
                (
                  I.REQUEST_NUM,
                  I.CIF_NUMBER,
                  I.REQUEST_TYPE,
                  I.COMMENTS,
                  V_GROUP_ID_CGR,
                  I.GROUP_WITH_CIF_NBR,
                  I.GROUP_NAME,
                  I.REQUESTOR_ID,
                  I.REQUEST_TS,
                  99,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  I.REQUEST_ID
                );
              --Inactivate the existing customer group row
              UPDATE DM_CUST_GROUP.CUST_GROUP
              SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                CURRENT_IND    ='N',
                DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                REQUEST_NUM    =I.REQUEST_NUM
              WHERE GROUP_ID   =V_GROUP_ID_CGR
              AND CURRENT_IND  = 'Y';
              --Insert a new customer group row, setting the group ID to the same number and setting the customer group name to the name specified on the request
              INSERT
              INTO DM_CUST_GROUP.CUST_GROUP( GROUP_ID,BEG_EFCTV_DT,END_EFCTV_DT,CURRENT_IND,REQUEST_NUM,GROUP_NAME,DW_INSERT_DTTM,DW_UPD_DTTM)
              VALUES
                (
                  V_GROUP_ID_CGR,
                  V_CURRENT_BATCH_DATE,
                  TO_DATE('31-12-9999','DD-MM-YYYY'),
                  'Y',
                  I.REQUEST_NUM,
                  I.GROUP_NAME,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE
                );
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
            END;
          END IF ;
        END IF;
        --If the request type is ?5 Remove customer from group?
      ELSIF I.REQUEST_TYPE=5 THEN
        --Verify that the customer (CIF_Nbr) is assigned to a customer group; if not, set the fail reason to ?Customer not assigned to a customer group?
        BEGIN
          V_GROUP_ID_CGR:=NULL;
          SELECT GROUP_ID
          INTO V_GROUP_ID_CGR
          FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
          WHERE INVOLVED_PARTY_ID=I.INVOLVED_PARTY_ID
          AND CURRENT_IND ='Y';
        EXCEPTION
        WHEN OTHER THEN
          NULL;
        END;
        IF V_GROUP_ID_CGR IS NULL THEN
          BEGIN
            INSERT
            INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
            VALUES
              (
                I.REQUEST_NUM,
                I.CIF_NUMBER,
                I.REQUEST_TYPE,
                I.COMMENTS,
                I.GROUP_ID,
                I.GROUP_WITH_CIF_NBR,
                I.GROUP_NAME,
                I.REQUESTOR_ID,
                I.REQUEST_TS,
                33,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                I.REQUEST_ID
              );
          EXCEPTION
          WHEN OTHER THEN
            --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
            --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
EXIT;
          END;

        ELSE

        --If the customer is assigned to an active account which is classified as workout line of business
          BEGIN
            V_INVOLVED_PARTY_ID_CGR := NULL;
             SELECT DISTINCT IP.INVOLVED_PARTY_ID
            INTO V_INVOLVED_PARTY_ID_CGR
            FROM DM_CUST_GROUP.ISA_CGI_IP_POP_COST_CENTER IP
            WHERE IP.INVOLVED_PARTY_ID = I.INVOLVED_PARTY_ID
            AND IP.CM3_03_ID ='CREDIT_LN';
           EXCEPTION
          WHEN OTHER THEN
            NULL ;
          END;
          IF V_INVOLVED_PARTY_ID_CGR IS NOT NULL THEN
            BEGIN
              INSERT
              INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
              VALUES
                (
                  I.REQUEST_NUM,
                  I.CIF_NUMBER,
                  I.REQUEST_TYPE,
                  I.COMMENTS,
                  I.GROUP_ID,
                  I.GROUP_WITH_CIF_NBR,
                  I.GROUP_NAME,
                  I.REQUESTOR_ID,
                  I.REQUEST_TS,
                  27,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  V_CURRENT_BATCH_DATE,
                  I.REQUEST_ID
                );
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
            END;
            --If the request is valid
            ELSE
          --Update the group ID on the request row (Grouping_Request.Group_ID) with the group ID associated to the customer
          BEGIN
            INSERT
            INTO DM_CUST_GROUP.CUST_GROUP_RQST ( REQUEST_NUM,CIF_NUMBER,REQUEST_TYPE,COMMENTS,GROUP_ID,GROUP_WITH_CIF_NBR,GROUP_NAME,REQUESTOR_ID,REQUEST_DTTM,PROCESS_STATUS,PROCESS_DTTM,DW_INSERT_DTTM,DW_UPD_DTTM,REQUEST_ID)
            VALUES
              (
                I.REQUEST_NUM,
                I.CIF_NUMBER,
                I.REQUEST_TYPE,
                I.COMMENTS,
                V_GROUP_ID_CGR,
                I.GROUP_WITH_CIF_NBR,
                I.GROUP_NAME,
                I.REQUESTOR_ID,
                I.REQUEST_TS,
                99,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                V_CURRENT_BATCH_DATE,
                I.REQUEST_ID
              );
            UPDATE DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
            SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
              CURRENT_IND    ='N',
              DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
              REQUEST_NUM    =I.REQUEST_NUM
            WHERE INVOLVED_PARTY_ID =I.INVOLVED_PARTY_ID
            AND CURRENT_IND  ='Y';
          EXCEPTION
          WHEN OTHER THEN
            --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
            --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert/update for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
EXIT;
          END;
          -- If no other customers are associated to the customer group, inactivate the customer group row
          BEGIN
            V_GROUP_ID_CG:=NULL;
            SELECT DISTINCT GROUP_ID
            INTO V_GROUP_ID_CG
            FROM DM_CUST_GROUP.CUST_GROUP_RELATIONSHIP
            WHERE GROUP_ID =V_GROUP_ID_CGR
            AND INVOLVED_PARTY_ID<>I.INVOLVED_PARTY_ID
            AND CURRENT_IND='Y';
          EXCEPTION
          WHEN OTHER THEN
            NULL;
          END;
          IF V_GROUP_ID_CG IS NULL THEN
            BEGIN
              UPDATE DM_CUST_GROUP.CUST_GROUP
              SET END_EFCTV_DT = date_trunc('day', V_CURRENT_BATCH_DATE)-1/86400,
                CURRENT_IND    ='N',
                DW_UPD_DTTM    =V_CURRENT_BATCH_DATE,
                REQUEST_NUM    =I.REQUEST_NUM
              WHERE GROUP_ID   =V_GROUP_ID_CGR
              AND CURRENT_IND  ='Y' ;
            EXCEPTION
            WHEN OTHER THEN
              --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
              --RAISE_APPLICATION_ERROR(-20001,'An error was encountered in CUST_GROUP_REQUEST insert for - '||I.REQUEST_NUM||' - '||SQLCODE||' -ERROR- '||SQLERRM);
 return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
 EXIT;
            END;
          END IF ;
          END IF;
        END IF ;
      END IF ;
      END IF ;
      END IF ;
    END;
  END LOOP ;
  COMMIT;
  --dw_bld.abc_logger.end_timed_event('CGI - Grouping Request Process completed','TIMER1',V_COUNT);

  EXCEPTION
  WHEN OTHER THEN
  --DW_BLD.ABC_LOGGER.SEVERE_MESSAGE(SQLERRM);
  --RAISE_APPLICATION_ERROR (-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
  return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
END;
$$
;
