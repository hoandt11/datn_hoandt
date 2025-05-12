--------------------------------------------------------
--  File created - Monday-May-12-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure ADD_PARTITION_DAY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HOANDT"."ADD_PARTITION_DAY" (
 
     tablespace_name_ VARCHAR2  DEFAULT NULL
 
    ) AS
 
    date_default VARCHAR2(30);

    total_date number;

    length number;

    total_table_partitons number;

    table_name VARCHAR2(500);

    sql_stmt VARCHAR2(1000);

    partition_name VARCHAR2(500);

BEGIN

        total_date:=720;

        date_default := TO_CHAR(TRUNC(SYSDATE)-7, 'YYYYMMDD');

        FOR table_rec IN (SELECT DISTINCT table_name FROM user_tab_partitions where table_name NOT like 'BIN$%' AND TABLESPACE_NAME = tablespace_name_) LOOP

        table_name := table_rec.table_name;   

            length := 0; -- ??t l?i bi?n ??m cho m?i b?ng

            WHILE length <= total_date LOOP

            partition_name := TO_CHAR(TO_DATE(date_default, 'YYYYMMDD')+length, 'YYYYMMDD') || '00';

            sql_stmt := 'ALTER TABLE ' || table_name || ' ADD PARTITION '|| '"'|| partition_name||'"' ||' VALUES LESS THAN ('||partition_name||') TABLESPACE'  || '"'|| tablespace_name_ ||'"';

             BEGIN

                EXECUTE IMMEDIATE sql_stmt;

                DBMS_OUTPUT.PUT_LINE(sql_stmt);

            EXCEPTION

                WHEN OTHERS THEN

                    DBMS_OUTPUT.PUT_LINE('Error adding partition ' || partition_name || ' for table ' || table_name);

            END;

            --DBMS_OUTPUT.PUT_LINE(sql_stmt);

            length := length + 1;

            END LOOP;

        END LOOP;

END ADD_PARTITION_DAY;

/
--------------------------------------------------------
--  DDL for Procedure ADD_PARTITION_DAY_CLONE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HOANDT"."ADD_PARTITION_DAY_CLONE" (
     tablespace_name_ VARCHAR2  DEFAULT NULL,
     facttable VARCHAR2  DEFAULT NULL
    ) AS
    date_default VARCHAR2(30); 
    total_date number;
    length number;
    total_table_partitons number;
    table_name VARCHAR2(500);
    sql_stmt VARCHAR2(1000);
    partition_name VARCHAR2(500);
BEGIN
        total_date:=3600;
        date_default := '20230101';

        FOR table_rec IN (SELECT DISTINCT table_name FROM user_tab_partitions where table_name = facttable AND TABLESPACE_NAME = tablespace_name_) LOOP
        table_name := table_rec.table_name;   
            length := 0; -- ??t l?i bi?n ??m cho m?i b?ng
            WHILE length <= total_date LOOP

            partition_name := TO_CHAR(TO_DATE(date_default, 'YYYYMMDD')+length, 'YYYYMMDD') || '00';
             sql_stmt := 'ALTER TABLE ' || '"'|| table_name ||'"' || ' ADD PARTITION '|| '"'|| partition_name||'"' ||' VALUES LESS THAN ('||partition_name||') TABLESPACE'  || '"'|| tablespace_name_ ||'"';
             DBMS_OUTPUT.PUT_LINE(sql_stmt);
             BEGIN
                EXECUTE IMMEDIATE sql_stmt;
                DBMS_OUTPUT.PUT_LINE(sql_stmt);
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error adding partition ' || partition_name || ' for table ' || table_name);
            END;

            --DBMS_OUTPUT.PUT_LINE(sql_stmt);
            length := length + 1;
            END LOOP;
        END LOOP;

END ADD_PARTITION_DAY_CLONE;

/
