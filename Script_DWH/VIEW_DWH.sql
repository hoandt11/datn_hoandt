--------------------------------------------------------
--  File created - Monday-May-12-2025   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_ACCESSIBILITY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_ACCESSIBILITY" ("DATETIME_ID", "CELL_NODE_ID", "pmRadioRaCbSuccMsg3", "pmRadioRaCbAttMsg2", "pmEndcSetupUeSucc", "pmEndcSetupUeAtt") AS 
  SELECT  "DATETIME_ID","CELL_NODE_ID"
      ,SUM(pmRadioRaCbSuccMsg3) "pmRadioRaCbSuccMsg3"
      ,SUM(pmRadioRaCbAttMsg2) "pmRadioRaCbAttMsg2"
      ,NULL "pmEndcSetupUeSucc"
      ,NULL "pmEndcSetupUeAtt" 
  FROM "ERS5G_NRCELL_ACCESSIBILITY1" 
  WHERE CELL_NODE_ID IS NOT NULL 
  GROUP BY "DATETIME_ID","CELL_NODE_ID"

  UNION ALL

  SELECT  "DATETIME_ID","CELL_NODE_ID"
      ,NULL "pmRadioRaCbSuccMsg3"
      ,NULL "pmRadioRaCbAttMsg2"
      ,SUM(pmEndcSetupUeSucc) "pmEndcSetupUeSucc"
      ,SUM(pmEndcSetupUeAtt) "pmEndcSetupUeAtt" 
  FROM "ERS5G_NRCELL_ACCESSIBILITY2" 
  WHERE CELL_NODE_ID IS NOT NULL 
  GROUP BY "DATETIME_ID","CELL_NODE_ID"
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_AVAILABILITY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_AVAILABILITY" ("DATETIME_ID", "CELL_NODE_ID", "RES", "PMCELLDOWNTIMEAUTO", "PMCELLDOWNTIMEMAN") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID,
       RES
      ,SUM(pmCellDowntimeAuto) pmCellDowntimeAuto
      ,SUM(pmCellDowntimeMan) pmCellDowntimeMan
  FROM ERS5G_NRCELL_AVAILABILITY 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID,RES
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_CONNECTED_RRC_USER_AVG
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_CONNECTED_RRC_USER_AVG" ("DATETIME_ID", "CELL_NODE_ID", "PMRRCCONNLEVELSUMENDC", "PMRRCCONNLEVELSAMP") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID
      ,SUM(pmRrcConnLevelSumEnDc) pmRrcConnLevelSumEnDc
	  ,SUM(pmRrcConnLevelSamp) pmRrcConnLevelSamp
      
  FROM ERS5G_NRCELL_CONNECTED_RRC_USER_AVG 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_CONNECTED_RRC_USER_MAX
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_CONNECTED_RRC_USER_MAX" ("DATETIME_ID", "CELL_NODE_ID", "PMRRCCONNLEVELMAXENDC") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID
      ,MAX(pmRrcConnLevelMaxEnDc) pmRrcConnLevelMaxEnDc
	 
      
  FROM ERS5G_NRCELL_CONNECTED_RRC_USER_MAX 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_LATENCY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_LATENCY" ("DATETIME_ID", "CELL_NODE_ID", "PMMACLATTIMEDLDRXSYNCQOS", "PMMACLATTIMEDLNODRXSYNCQOS", "PMMACLATTIMEDLDRXSYNCSAMPQOS", "PMMACLATTIMEDLNODRXSYNCSAMPQOS") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID
      ,SUM(pmMacLatTimeDlDrxSyncQos) pmMacLatTimeDlDrxSyncQos
	  ,SUM(pmMacLatTimeDlNoDrxSyncQos) pmMacLatTimeDlNoDrxSyncQos
	  ,SUM(pmMacLatTimeDlDrxSyncSampQoS) pmMacLatTimeDlDrxSyncSampQoS
	  ,SUM(pmMacLatTimeDlNoDrxSyncSampQoS) pmMacLatTimeDlNoDrxSyncSampQoS
      
  FROM ERS5G_NRCELL_LATENCY 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_MOBILITY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_MOBILITY" ("DATETIME_ID", "CELL_NODE_ID", "PMENDCPSCELLCHANGESUCCINTRASGNB", "PMENDCPSCELLCHANGEATTINTRASGNB", "PMENDCPSCELLCHANGESUCCINTERSGNB", "PMENDCPSCELLCHANGEATTINTERSGNB") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID
      ,SUM(pmEndcPSCellChangeSuccIntraSgnb) pmEndcPSCellChangeSuccIntraSgnb
	  ,SUM(pmEndcPSCellChangeAttIntraSgnb) pmEndcPSCellChangeAttIntraSgnb
	  ,SUM(pmEndcPSCellChangeSuccInterSgnb) pmEndcPSCellChangeSuccInterSgnb
	  ,SUM(pmEndcPSCellChangeAttInterSgnb) pmEndcPSCellChangeAttInterSgnb
      
  FROM ERS5G_NRCELL_MOBILITY 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_PACKET_LOSS_RATIO
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_PACKET_LOSS_RATIO" ("DATETIME_ID", "CELL_NODE_ID", "pmPdcpPktTransDlDiscQos", "pmPdcpPktTransDlDiscAqmQos", "pmPdcpPktTransDlQos") AS 
  SELECT   "DATETIME_ID" ,"CELL_NODE_ID"
      ,SUM(pmPdcpPktTransDlDiscQos) "pmPdcpPktTransDlDiscQos"
	  ,SUM(pmPdcpPktTransDlDiscAqmQos) "pmPdcpPktTransDlDiscAqmQos"
	  ,SUM(pmPdcpPktTransDlQos) "pmPdcpPktTransDlQos"
	 
      
  FROM "ERS5G_NRCELL_PACKET_LOSS_RATIO" 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY "DATETIME_ID","CELL_NODE_ID"
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_RAN
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_RAN" ("DATETIME_ID", "CELL_NODE_ID", "RANDOM_ACCESS_SUCCESS", "RANDOM_ACCESS_ATTEMPT", "SGNB_ADDITION_SUCC", "SGNB_ADDITION_ATTEMPT", "SGNBTRISGNBABNORMALRELEASE", "SGNBRELEASE", "INTRASGNBPSCELLCHANGESUCCESS", "INTRASGNBPSCELLCHANGEATTEMPT", "INTERSGNBPSCELLCHANGESUCCESS", "INTERSGNBPSCELLCHANGEATTEMPT", "CELLDLTRAFFICVOLUME", "CELLDLTRANSFERTIME", "CELLULTRAFFICVOLUME", "CELLULTRANSFERTIME", "USERDLRMVLASTSLOTTRAFFICVOLUME", "USERDLRMVLASTSLOTTRANSFERTIME", "USERULRMVLASTSLOTTRAFFICVOLUME", "USERULRMVSMALLPKTTRANSFERTIME", "DLTRAFFICVOLUME", "ULTRAFFICVOLUME", "RB_USEDDL", "RB_AVAILABLEDL", "RB_USEDUL", "RB_AVAILABLEUL", "AVAILABLE_TIME", "TOTAL_TIME", "TRANSFER_TIME", "NUMBER_OF_SAMPLES", "MISSED_PACKET", "TOTAL_PACKET", "CONNECTED_RRC_USER_AVERAGE", "CONNECTED_RRC_USER_MAX") AS 
  SELECT DATETIME_ID
      ,CELL_NODE_ID
	 
      ,"pmRadioRaCbSuccMsg3" AS Random_access_success ,"pmRadioRaCbAttMsg2" AS Random_access_attempt,"pmEndcSetupUeSucc" AS SgNB_addition_succ,"pmEndcSetupUeAtt" AS SgNB_addition_attempt
	  ,0 AS SgNBTriSgNBAbnormalRelease
	  ,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess
	  ,0 AS IntraSgNBPscellChangeAttempt
	  ,0 AS InterSgNBPscellChangeSuccess
	  ,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume
	  ,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL,0 AS RB_AvailableDL,0 AS RB_UsedUL,0 AS RB_AvailableUL,0 AS Available_time,0 AS Total_time 
	  ,0 AS Transfer_time,0 AS Number_of_samples,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average,0 AS Connected_RRC_user_Max
  FROM HR_ERS5G_NRCELL_ACCESSIBILITY
  UNION ALL 
  SELECT DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt
      ,(pmEndcRelUeAbnormalMenbAct + pmEndcRelUeAbnormalSgnbAct) AS SgNBTriSgNBAbnormalRelease
	  ,(pmEndcRelUeNormal + pmEndcRelUeAbnormalMenb + pmEndcRelUeAbnormalSgnb) AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess
	  ,0 AS IntraSgNBPscellChangeAttempt
	  ,0 AS InterSgNBPscellChangeSuccess
	  ,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume
	  ,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL,0 AS RB_AvailableDL,0 AS RB_UsedUL,0 AS RB_AvailableUL,0 AS Available_time,0 AS Total_time 
	  ,0 AS Transfer_time,0 AS Number_of_samples,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max
  FROM HR_ERS5G_NRCELL_RETAINABILITY
  UNION ALL
  SELECT DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
      ,pmEndcPSCellChangeSuccIntraSgnb	AS IntraSgNBPscellChangeSuccess
	  ,pmEndcPSCellChangeAttIntraSgnb	AS IntraSgNBPscellChangeAttempt
	  ,pmEndcPSCellChangeSuccInterSgnb	AS InterSgNBPscellChangeSuccess
	  ,pmEndcPSCellChangeAttInterSgnb	AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume
	  ,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL,0 AS RB_AvailableDL,0 AS RB_UsedUL,0 AS RB_AvailableUL,0 AS Available_time,0 AS Total_time 
	  ,0 AS Transfer_time,0 AS Number_of_samples,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max
  FROM HR_ERS5G_NRCELL_MOBILITY

  UNION ALL 

  
SELECT DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
      ,pmMacVolDl	AS CellDLTrafficVolume
	  ,pmPdschSchedActivity AS CellDLTransferTime
	  ,pmMacVolUl AS	CellULTrafficVolume
	  ,pmPuschSchedActivity AS CellULTransferTime
	  ,pmMacVolDlDrb AS UserDLRmvLastSlotTrafficVolume
	  ,pmMacTimeDlDrb AS UserDLRmvLastSlotTransferTime
	  ,pmMacVolUlResUe AS UseruLRmvLastSlotTrafficVolume
	  ,pmMacTimeUlResUe AS UserULRmvSmallPktTransferTime
	  ,pmMacVolDl AS DLTrafficVolume
      ,pmMacVolUl AS ULTrafficVolume
	  ,0 AS RB_UsedDL,0 AS RB_AvailableDL,0 AS RB_UsedUL,0 AS RB_AvailableUL,0 AS Available_time,0 AS Total_time 
	  ,0 AS Transfer_time,0 AS Number_of_samples,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max
  FROM HR_ERS5G_NRCELL_SERVICE_INTEGRITY
  UNION ALL
  SELECT DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime
      ,0 AS DLTrafficVolume
      ,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL,0 AS RB_AvailableDL,0 AS RB_UsedUL,0 AS RB_AvailableUL,0 AS Available_time,0 AS Total_time 
	  ,0 AS Transfer_time,0 AS Number_of_samples,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max

  FROM HR_ERS5G_NRCELL_TRAFFIC
   
   UNION ALL

   SELECT  DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume ,0 AS ULTrafficVolume
      ,pmMacRBSymUsedPdschTypeA+pmMacRBSymUsedPdcchTypeA+pmMacRBSymUsedPdcchTypeB+pmMacRBSymUsedPdschTypeABroadcasting+pmMacRBSymCsiRs	RB_UsedDL
	  ,pmMacRBSymAvailDl AS RB_AvailableDL
	  ,pmMacRBSymUsedPuschTypeA+pmMacRBSymUsedPuschTypeB AS RB_UsedUL
	  ,pmMacRBSymAvailUl AS RB_AvailableUL
	  ,0 AS Available_time,0 AS Total_time ,0 AS Transfer_time,0 AS Number_of_samples,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max

  FROM HR_ERS5G_NRCELL_UTILIZATION
  UNION ALL

  SELECT DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume ,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL ,0 AS RB_AvailableDL,0 AS RB_UsedUL ,0 AS RB_AvailableUL,0 AS Available_time, 0 AS Total_time
      ,0 AS Transfer_time
	  ,0 AS Number_of_samples
	  ,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average
      ,0 AS Connected_RRC_user_Max
  FROM HR_ERS5G_NRCELL_AVAILABILITY
  UNION ALL
  SELECT   DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume ,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL ,0 AS RB_AvailableDL,0 AS RB_UsedUL ,0 AS RB_AvailableUL,0 AS Available_time, 0 AS Total_time
      ,(pmMacLatTimeDlDrxSyncQos + pmMacLatTimeDlNoDrxSyncQos)	Transfer_time
	  ,(pmMacLatTimeDlDrxSyncSampQoS + pmMacLatTimeDlNoDrxSyncSampQoS)	Number_of_samples
	  ,0 AS Missed_packet ,0 AS Total_packet, 0 AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max

  FROM HR_ERS5G_NRCELL_LATENCY
  UNION ALL
  SELECT  DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume ,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL ,0 AS RB_AvailableDL,0 AS RB_UsedUL ,0 AS RB_AvailableUL,0 AS Available_time, 0 AS Total_time,0 AS Transfer_time,0 AS Number_of_samples
      ,("pmPdcpPktTransDlDiscQos" - "pmPdcpPktTransDlDiscAqmQos") AS 	Missed_packet
	  ,"pmPdcpPktTransDlQos" AS Total_packet, 0 AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max
  FROM HR_ERS5G_NRCELL_PACKET_LOSS_RATIO
  UNION ALL
  SELECT  DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume ,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL ,0 AS RB_AvailableDL,0 AS RB_UsedUL ,0 AS RB_AvailableUL,0 AS Available_time, 0 AS Total_time,0 AS Transfer_time,0 AS Number_of_samples
	  ,0 AS Missed_packet, 0 AS Total_packet
      ,(pmRrcConnLevelSumEnDc/pmRrcConnLevelSamp) AS Connected_RRC_user_average
	  ,0 AS Connected_RRC_user_Max
  FROM HR_ERS5G_NRCELL_CONNECTED_RRC_USER_AVG

  UNION ALL
  SELECT  DATETIME_ID
      ,CELL_NODE_ID
	  ,0 AS Random_access_success ,0 AS Random_access_attempt,0 AS SgNB_addition_succ,0 AS SgNB_addition_attempt,0 AS SgNBTriSgNBAbnormalRelease,0 AS SgNBRelease
	  ,0 AS IntraSgNBPscellChangeSuccess,0 AS IntraSgNBPscellChangeAttempt,0 AS InterSgNBPscellChangeSuccess,0 AS InterSgNBPscellChangeAttempt
	  ,0 AS CellDLTrafficVolume,0 AS CellDLTransferTime,0 AS	CellULTrafficVolume,0 AS CellULTransferTime,0 AS UserDLRmvLastSlotTrafficVolume,0 AS UserDLRmvLastSlotTransferTime
	  ,0 AS UseruLRmvLastSlotTrafficVolume,0 AS UserULRmvSmallPktTransferTime ,0 AS DLTrafficVolume ,0 AS ULTrafficVolume
	  ,0 AS RB_UsedDL ,0 AS RB_AvailableDL,0 AS RB_UsedUL ,0 AS RB_AvailableUL,0 AS Available_time, 0 AS Total_time,0 AS Transfer_time,0 AS Number_of_samples
	  ,0 AS Missed_packet, 0 AS Total_packet
      ,0 AS Connected_RRC_user_average
	  ,pmRrcConnLevelMaxEnDc AS Connected_RRC_user_Max

  FROM HR_ERS5G_NRCELL_CONNECTED_RRC_USER_MAX
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_RETAINABILITY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_RETAINABILITY" ("DATETIME_ID", "CELL_NODE_ID", "PMENDCRELUEABNORMALMENBACT", "PMENDCRELUEABNORMALSGNBACT", "PMENDCRELUENORMAL", "PMENDCRELUEABNORMALMENB", "PMENDCRELUEABNORMALSGNB") AS 
  SELECT   "DATETIME_ID" ,"CELL_NODE_ID"
      ,SUM(pmEndcRelUeAbnormalMenbAct) pmEndcRelUeAbnormalMenbAct
	  ,SUM(pmEndcRelUeAbnormalSgnbAct) pmEndcRelUeAbnormalSgnbAct
	  ,SUM(pmEndcRelUeNormal) pmEndcRelUeNormal
	  ,SUM(pmEndcRelUeAbnormalMenb) pmEndcRelUeAbnormalMenb
	  ,SUM(pmEndcRelUeAbnormalSgnb) pmEndcRelUeAbnormalSgnb
	 
      
  FROM ERS5G_NRCELL_RETAINABILITY 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_SERVICE_INTEGRITY
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_SERVICE_INTEGRITY" ("DATETIME_ID", "CELL_NODE_ID", "PMMACVOLDL", "PMMACVOLUL", "PMPDSCHSCHEDACTIVITY", "PMPUSCHSCHEDACTIVITY", "PMMACVOLDLDRB", "PMMACTIMEDLDRB", "PMMACVOLULRESUE", "PMMACTIMEULRESUE") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID
      ,SUM(pmMacVolDl) pmMacVolDl
      ,SUM(pmMacVolUl) pmMacVolUl
	  ,SUM(pmPdschSchedActivity) pmPdschSchedActivity
	  ,SUM(pmPuschSchedActivity) pmPuschSchedActivity

      ,NULL pmMacVolDlDrb
      ,NULL pmMacTimeDlDrb
      ,NULL pmMacVolUlResUe
      ,NULL pmMacTimeUlResUe
      
	 
      
  FROM ERS5G_NRCELL_SERVICE_INTEGRITY1 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
	UNION ALL

	SELECT   DATETIME_ID ,CELL_NODE_ID
      ,NULL pmMacVolDl
      ,NULL pmMacVolUl
	  ,NULL pmPdschSchedActivity
      ,NULL  pmPuschSchedActivity

      ,SUM(pmMacVolDlDrb) pmMacVolDlDrb
      ,SUM(pmMacTimeDlDrb) pmMacTimeDlDrb
      ,SUM(pmMacVolUlResUe) pmMacVolUlResUe
      ,SUM(pmMacTimeUlResUe) pmMacTimeUlResUe
      
	 
      
  FROM ERS5G_NRCELL_SERVICE_INTEGRITY2  
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_TRAFFIC
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_TRAFFIC" ("DATETIME_ID", "CELL_NODE_ID", "PMMACVOLDL", "PMMACVOLUL") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID
      ,SUM(pmMacVolDl) pmMacVolDl
      ,SUM(pmMacVolUl) pmMacVolUl
      
	 
      
  FROM ERS5G_NRCELL_TRAFFIC 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_ERS5G_NRCELL_UTILIZATION
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_ERS5G_NRCELL_UTILIZATION" ("DATETIME_ID", "CELL_NODE_ID", "PMMACRBSYMUSEDPDSCHTYPEA", "PMMACRBSYMUSEDPDCCHTYPEA", "PMMACRBSYMUSEDPDCCHTYPEB", "PMMACRBSYMUSEDPDSCHTYPEABROADCASTING", "PMMACRBSYMCSIRS", "PMMACRBSYMAVAILDL", "PMMACRBSYMUSEDPUSCHTYPEA", "PMMACRBSYMUSEDPUSCHTYPEB", "PMMACRBSYMAVAILUL") AS 
  SELECT   DATETIME_ID ,CELL_NODE_ID
      ,SUM(pmMacRBSymUsedPdschTypeA) pmMacRBSymUsedPdschTypeA
      ,SUM(pmMacRBSymUsedPdcchTypeA) pmMacRBSymUsedPdcchTypeA
      ,SUM(pmMacRBSymUsedPdcchTypeB) pmMacRBSymUsedPdcchTypeB
      ,SUM(pmMacRBSymUsedPdschTypeABroadcasting) pmMacRBSymUsedPdschTypeABroadcasting
      ,SUM(pmMacRBSymCsiRs) pmMacRBSymCsiRs
      ,SUM(pmMacRBSymAvailDl) pmMacRBSymAvailDl
      ,SUM(pmMacRBSymUsedPuschTypeA) pmMacRBSymUsedPuschTypeA
      ,SUM(pmMacRBSymUsedPuschTypeB) pmMacRBSymUsedPuschTypeB
      ,SUM(pmMacRBSymAvailUl) pmMacRBSymAvailUl
	 
      
  FROM ERS5G_NRCELL_UTILIZATION 
  WHERE CELL_NODE_ID IS NOT NULL 
    GROUP BY DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1542455305
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1542455305" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1542455911", "H1542455912") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1542455911) H1542455911
      ,SUM(H1542455912) H1542455912
  FROM HUA5G_RAN_1542455305 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816243
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816243" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816771") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1911816771) H1911816771
  
  FROM HUA5G_RAN_1911816243 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816244
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816244" ("DATETIME_ID", "CELL_NODE_ID", "H1911816803") AS 
  SELECT   DATETIME_ID,CELL_NODE_ID
      ,SUM(H1911816803) H1911816803
  FROM HUA5G_RAN_1911816244
  GROUP BY  DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816247
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816247" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816694", "H1911816695", "H1911816696", "H1911816697", "H1911816848", "H1911816850", "H1911816651", "H1911816653", "H1911816643", "H1911816645", "H1911816851", "H1911816853") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1911816694) H1911816694
      ,SUM(H1911816695) H1911816695
      ,SUM(H1911816696) H1911816696
      ,SUM(H1911816697) H1911816697
      ,SUM(H1911816848) H1911816848
      ,SUM(H1911816850) H1911816850
      ,SUM(H1911816651) H1911816651
      ,SUM(H1911816653) H1911816653
      ,SUM(H1911816643) H1911816643
      ,SUM(H1911816645) H1911816645
	  ,SUM(H1911816851) H1911816851
	  ,SUM(H1911816853) H1911816853
  
  FROM HUA5G_RAN_1911816247 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816249
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816249" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816540", "H1911816679", "H1911816541", "H1911816582") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1911816540) H1911816540
      ,SUM(H1911816679) H1911816679
      ,SUM(H1911816541) H1911816541
      ,SUM(H1911816582) H1911816582
  
  FROM HUA5G_RAN_1911816249 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816255
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816255" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911820710", "H1911816542") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1911820710) H1911820710
      ,SUM(H1911816542) H1911816542
  
  FROM HUA5G_RAN_1911816255 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816256
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816256" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816747", "H1911817843", "H1911816751", "H1911816750", "H1911816749", "H1911816748", "H1911816746", "H1911816752") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1911816747) H1911816747
      ,SUM(H1911817843) H1911817843
      ,SUM(H1911816751) H1911816751
      ,SUM(H1911816750) H1911816750
      ,SUM(H1911816749) H1911816749
      ,SUM(H1911816748) H1911816748
      ,SUM(H1911816746) H1911816746
	  ,SUM(H1911816752) H1911816752
  FROM HUA5G_RAN_1911816256 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816261
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816261" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911820624") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES)*60 RES
      ,SUM(H1911820624) H1911820624

  
  FROM HUA5G_RAN_1911816261 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816272
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816272" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816922", "H1911816923", "H1911816915", "H1911816919", "H1911816918", "H1911816913") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1911816922) H1911816922
      ,SUM(H1911816923) H1911816923
      ,SUM(H1911816915) H1911816915
	  ,SUM(H1911816919) H1911816919
      ,SUM(H1911816918) H1911816918
      ,SUM(H1911816913) H1911816913

  
  FROM HUA5G_RAN_1911816272 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816277
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816277" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816963", "H1911816962") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
       
      ,SUM(RES) RES
      ,SUM(H1911816963) H1911816963
      ,SUM(H1911816962) H1911816962
   

  
  FROM HUA5G_RAN_1911816277 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816286
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816286" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816942", "H1911816957", "H1911816948", "H1911816953", "H1911816943", "H1911817830", "H1911817829", "H1911817828", "H1911816947", "H1911816946", "H1911816958", "H1911816951", "H1911816955", "H1911817827", "H1911817826", "H1911817825", "H1911817093", "H1911817092") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
      ,SUM(RES) RES
      ,SUM(H1911816942) H1911816942
      ,SUM(H1911816957) H1911816957
      ,SUM(H1911816948) H1911816948
      ,SUM(H1911816953) H1911816953
      ,SUM(H1911816943) H1911816943
      ,SUM(H1911817830) H1911817830
      ,SUM(H1911817829) H1911817829
      ,SUM(H1911817828) H1911817828
      ,SUM(H1911816947) H1911816947
      ,SUM(H1911816946) H1911816946
      ,SUM(H1911816958) H1911816958
      ,SUM(H1911816951) H1911816951
      ,SUM(H1911816955) H1911816955
      ,SUM(H1911817827) H1911817827
      ,SUM(H1911817826) H1911817826
      ,SUM(H1911817825) H1911817825
      ,SUM(H1911817093) H1911817093
      ,SUM(H1911817092) H1911817092

  FROM HUA5G_RAN_1911816286 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816292
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816292" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911816967", "H1911816966") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
      ,SUM(RES) RES
      ,SUM(H1911816967) H1911816967
      ,SUM(H1911816966) H1911816966
      
  FROM HUA5G_RAN_1911816292 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_HUA5G_RAN_1911816296
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_HUA5G_RAN_1911816296" ("DATETIME_ID", "CELL_NODE_ID", "RES", "H1911817063", "H1911817061") AS 
  SELECT  DATETIME_ID,CELL_NODE_ID
      ,SUM(RES) RES
      ,SUM(H1911817063) H1911817063
      ,SUM(H1911817061) H1911817061
      
  FROM HUA5G_RAN_1911816296 
  group by DATETIME_ID,CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NCAV
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NCAV" ("DATETIME_ID", "CELL_NODE_ID", "DENOM_CELL_AVAIL", "SAMPLES_CELL_AVAIL") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID
	  ,SUM(M55601C00001) AS DENOM_CELL_AVAIL
      ,SUM(M55601C00002) AS SAMPLES_CELL_AVAIL
  FROM NSN5G_RAN_NCAV 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NCELA
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NCELA" ("DATETIME_ID", "CELL_NODE_ID", "DATA_SLOT_PDSCH", "DATA_SLOT_PUSCH", "DATA_SLOT_PDSCH_TIME", "DATA_SLOT_PUSCH_TIME", "ACC_UE_DL_DRB_DATA", "ACC_UE_UL_DRB_DATA", "PDSCH_OFDM_SYMBOLS_TIME", "PUSCH_OFDM_SYMBOLS_TIME", "PRB_USED_PDSCH", "PRB_USED_PUSCH", "PRB_AVAIL_PDSCH", "PRB_AVAIL_PUSCH") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID

      ,SUM(M55308C00005) AS DATA_SLOT_PDSCH
      ,SUM(M55308C00006) AS DATA_SLOT_PUSCH
      ,SUM(M55308C00008) AS DATA_SLOT_PDSCH_TIME
      ,SUM(M55308C00009) AS DATA_SLOT_PUSCH_TIME
      ,SUM(M55308C02000) AS ACC_UE_DL_DRB_DATA
      ,SUM(M55308C02002) AS ACC_UE_UL_DRB_DATA
      ,SUM(M55308C03005) AS PDSCH_OFDM_SYMBOLS_TIME
      ,SUM(M55308C03006) AS PUSCH_OFDM_SYMBOLS_TIME
      ,SUM(M55308C10001) AS PRB_USED_PDSCH
      ,SUM(M55308C10002) AS PRB_USED_PUSCH
      ,SUM(M55308C10003) AS PRB_AVAIL_PDSCH
      ,SUM(M55308C10004) AS PRB_AVAIL_PUSCH

  FROM NSN5G_RAN_NCELA 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NINFC
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NINFC" ("DATETIME_ID", "CELL_NODE_ID", "INTRA_FR_PSCEL_CH_ATTEMPT", "INTRA_FR_PSCEL_CH_FAI_TDC_EXP", "INTRA_FR_PSCEL_CH_FAI_MENB_REF", "INTRA_FR_PSCEL_CH_FAI_RES_ALLO", "INT_FR_PSCEL_CHA_ATT_SRB3") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID

      ,SUM(M55120C00001) AS INTRA_FR_PSCEL_CH_ATTEMPT
      ,SUM(M55120C00002) AS INTRA_FR_PSCEL_CH_FAI_TDC_EXP
      ,SUM(M55120C00003) AS INTRA_FR_PSCEL_CH_FAI_MENB_REF
      ,SUM(M55120C00004) AS INTRA_FR_PSCEL_CH_FAI_RES_ALLO
      ,SUM(M55120C00501) AS INT_FR_PSCEL_CHA_ATT_SRB3 

  FROM NSN5G_RAN_NINFC 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NMPDU
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NMPDU" ("DATETIME_ID", "CELL_NODE_ID", "PDSCH_INI_VOL_64TBL_MCS00", "PDSCH_INI_VOL_64TBL_MCS01", "PDSCH_INI_VOL_64TBL_MCS02", "PDSCH_INI_VOL_64TBL_MCS03", "PDSCH_INI_VOL_64TBL_MCS04", "PDSCH_INI_VOL_64TBL_MCS05", "PDSCH_INI_VOL_64TBL_MCS06", "PDSCH_INI_VOL_64TBL_MCS07", "PDSCH_INI_VOL_64TBL_MCS08", "PDSCH_INI_VOL_64TBL_MCS09", "PDSCH_INI_VOL_64TBL_MCS10", "PDSCH_INI_VOL_64TBL_MCS11", "PDSCH_INI_VOL_64TBL_MCS12", "PDSCH_INI_VOL_64TBL_MCS13", "PDSCH_INI_VOL_64TBL_MCS14", "PDSCH_INI_VOL_64TBL_MCS15", "PDSCH_INI_VOL_64TBL_MCS16", "PDSCH_INI_VOL_64TBL_MCS17", "PDSCH_INI_VOL_64TBL_MCS18", "PDSCH_INI_VOL_64TBL_MCS19", "PDSCH_INI_VOL_64TBL_MCS20", "PDSCH_INI_VOL_64TBL_MCS21", "PDSCH_INI_VOL_64TBL_MCS22", "PDSCH_INI_VOL_64TBL_MCS23", "PDSCH_INI_VOL_64TBL_MCS24", "PDSCH_INI_VOL_64TBL_MCS25", "PDSCH_INI_VOL_64TBL_MCS26", "PDSCH_INI_VOL_64TBL_MCS27", "PDSCH_INI_VOL_64TBL_MCS28", "PDSCH_INI_VOL_256TBL_MCS00", "PDSCH_INI_VOL_256TBL_MCS01", "PDSCH_INI_VOL_256TBL_MCS02", "PDSCH_INI_VOL_256TBL_MCS03", "PDSCH_INI_VOL_256TBL_MCS04", "PDSCH_INI_VOL_256TBL_MCS05", "PDSCH_INI_VOL_256TBL_MCS06", "PDSCH_INI_VOL_256TBL_MCS07", "PDSCH_INI_VOL_256TBL_MCS08", "PDSCH_INI_VOL_256TBL_MCS09", "PDSCH_INI_VOL_256TBL_MCS10", "PDSCH_INI_VOL_256TBL_MCS11", "PDSCH_INI_VOL_256TBL_MCS12", "PDSCH_INI_VOL_256TBL_MCS13", "PDSCH_INI_VOL_256TBL_MCS14", "PDSCH_INI_VOL_256TBL_MCS15", "PDSCH_INI_VOL_256TBL_MCS16", "PDSCH_INI_VOL_256TBL_MCS17", "PDSCH_INI_VOL_256TBL_MCS18", "PDSCH_INI_VOL_256TBL_MCS19", "PDSCH_INI_VOL_256TBL_MCS20", "PDSCH_INI_VOL_256TBL_MCS21", "PDSCH_INI_VOL_256TBL_MCS22", "PDSCH_INI_VOL_256TBL_MCS23", "PDSCH_INI_VOL_256TBL_MCS24", "PDSCH_INI_VOL_256TBL_MCS25", "PDSCH_INI_VOL_256TBL_MCS26", "PDSCH_INI_VOL_256TBL_MCS27", "PUSCH_CP_INI_VOL_64TBL_MCS00", "PUSCH_CP_INI_VOL_64TBL_MCS01", "PUSCH_CP_INI_VOL_64TBL_MCS02", "PUSCH_CP_INI_VOL_64TBL_MCS03", "PUSCH_CP_INI_VOL_64TBL_MCS04", "PUSCH_CP_INI_VOL_64TBL_MCS05", "PUSCH_CP_INI_VOL_64TBL_MCS06", "PUSCH_CP_INI_VOL_64TBL_MCS07", "PUSCH_CP_INI_VOL_64TBL_MCS08", "PUSCH_CP_INI_VOL_64TBL_MCS09", "PUSCH_CP_INI_VOL_64TBL_MCS10", "PUSCH_CP_INI_VOL_64TBL_MCS11", "PUSCH_CP_INI_VOL_64TBL_MCS12", "PUSCH_CP_INI_VOL_64TBL_MCS13", "PUSCH_CP_INI_VOL_64TBL_MCS14", "PUSCH_CP_INI_VOL_64TBL_MCS15", "PUSCH_CP_INI_VOL_64TBL_MCS16", "PUSCH_CP_INI_VOL_64TBL_MCS17", "PUSCH_CP_INI_VOL_64TBL_MCS18", "PUSCH_CP_INI_VOL_64TBL_MCS19", "PUSCH_CP_INI_VOL_64TBL_MCS20", "PUSCH_CP_INI_VOL_64TBL_MCS21", "PUSCH_CP_INI_VOL_64TBL_MCS22", "PUSCH_CP_INI_VOL_64TBL_MCS23", "PUSCH_CP_INI_VOL_64TBL_MCS24", "PUSCH_CP_INI_VOL_64TBL_MCS25", "PUSCH_CP_INI_VOL_64TBL_MCS26", "PUSCH_CP_INI_VOL_64TBL_MCS27", "PUSCH_CP_INI_VOL_64TBL_MCS28") AS 
  SELECT  DATETIME_ID, CELL_NODE_ID, 
		sum(M55304C01000) as PDSCH_INI_VOL_64TBL_MCS00,
		sum(M55304C01001) as PDSCH_INI_VOL_64TBL_MCS01,
		sum(M55304C01002) as PDSCH_INI_VOL_64TBL_MCS02,
		sum(M55304C01003) as PDSCH_INI_VOL_64TBL_MCS03,
		sum(M55304C01004) as PDSCH_INI_VOL_64TBL_MCS04,
		sum(M55304C01005) as PDSCH_INI_VOL_64TBL_MCS05,
		sum(M55304C01006) as PDSCH_INI_VOL_64TBL_MCS06,
		sum(M55304C01007) as PDSCH_INI_VOL_64TBL_MCS07,
		sum(M55304C01008) as PDSCH_INI_VOL_64TBL_MCS08,
		sum(M55304C01009) as PDSCH_INI_VOL_64TBL_MCS09,
		sum(M55304C01010) as PDSCH_INI_VOL_64TBL_MCS10,
		sum(M55304C01011) as PDSCH_INI_VOL_64TBL_MCS11,
		sum(M55304C01012) as PDSCH_INI_VOL_64TBL_MCS12,
		sum(M55304C01013) as PDSCH_INI_VOL_64TBL_MCS13,
		sum(M55304C01014) as PDSCH_INI_VOL_64TBL_MCS14,
		sum(M55304C01015) as PDSCH_INI_VOL_64TBL_MCS15,
		sum(M55304C01016) as PDSCH_INI_VOL_64TBL_MCS16,
		sum(M55304C01017) as PDSCH_INI_VOL_64TBL_MCS17,
		sum(M55304C01018) as PDSCH_INI_VOL_64TBL_MCS18,
		sum(M55304C01019) as PDSCH_INI_VOL_64TBL_MCS19,
		sum(M55304C01020) as PDSCH_INI_VOL_64TBL_MCS20,
		sum(M55304C01021) as PDSCH_INI_VOL_64TBL_MCS21,
		sum(M55304C01022) as PDSCH_INI_VOL_64TBL_MCS22,
		sum(M55304C01023) as PDSCH_INI_VOL_64TBL_MCS23,
		sum(M55304C01024) as PDSCH_INI_VOL_64TBL_MCS24,
		sum(M55304C01025) as PDSCH_INI_VOL_64TBL_MCS25,
		sum(M55304C01026) as PDSCH_INI_VOL_64TBL_MCS26,
		sum(M55304C01027) as PDSCH_INI_VOL_64TBL_MCS27,
		sum(M55304C01028) as PDSCH_INI_VOL_64TBL_MCS28,

		sum(M55304C05000) as PDSCH_INI_VOL_256TBL_MCS00,
		sum(M55304C05001) as PDSCH_INI_VOL_256TBL_MCS01,
		sum(M55304C05002) as PDSCH_INI_VOL_256TBL_MCS02,
		sum(M55304C05003) as PDSCH_INI_VOL_256TBL_MCS03,
		sum(M55304C05004) as PDSCH_INI_VOL_256TBL_MCS04,
		sum(M55304C05005) as PDSCH_INI_VOL_256TBL_MCS05,
		sum(M55304C05006) as PDSCH_INI_VOL_256TBL_MCS06,
		sum(M55304C05007) as PDSCH_INI_VOL_256TBL_MCS07,
		sum(M55304C05008) as PDSCH_INI_VOL_256TBL_MCS08,
		sum(M55304C05009) as PDSCH_INI_VOL_256TBL_MCS09,
		sum(M55304C05010) as PDSCH_INI_VOL_256TBL_MCS10,
		sum(M55304C05011) as PDSCH_INI_VOL_256TBL_MCS11,
		sum(M55304C05012) as PDSCH_INI_VOL_256TBL_MCS12,
		sum(M55304C05013) as PDSCH_INI_VOL_256TBL_MCS13,
		sum(M55304C05014) as PDSCH_INI_VOL_256TBL_MCS14,
		sum(M55304C05015) as PDSCH_INI_VOL_256TBL_MCS15,
		sum(M55304C05016) as PDSCH_INI_VOL_256TBL_MCS16,
		sum(M55304C05017) as PDSCH_INI_VOL_256TBL_MCS17,
		sum(M55304C05018) as PDSCH_INI_VOL_256TBL_MCS18,
		sum(M55304C05019) as PDSCH_INI_VOL_256TBL_MCS19,
		sum(M55304C05020) as PDSCH_INI_VOL_256TBL_MCS20,
		sum(M55304C05021) as PDSCH_INI_VOL_256TBL_MCS21,
		sum(M55304C05022) as PDSCH_INI_VOL_256TBL_MCS22,
		sum(M55304C05023) as PDSCH_INI_VOL_256TBL_MCS23,
		sum(M55304C05024) as PDSCH_INI_VOL_256TBL_MCS24,
		sum(M55304C05025) as PDSCH_INI_VOL_256TBL_MCS25,
		sum(M55304C05026) as PDSCH_INI_VOL_256TBL_MCS26,
		sum(M55304C05027) as PDSCH_INI_VOL_256TBL_MCS27,

		sum(M55304C03000) as PUSCH_CP_INI_VOL_64TBL_MCS00,
		sum(M55304C03001) as PUSCH_CP_INI_VOL_64TBL_MCS01,
		sum(M55304C03002) as PUSCH_CP_INI_VOL_64TBL_MCS02,
		sum(M55304C03003) as PUSCH_CP_INI_VOL_64TBL_MCS03,
		sum(M55304C03004) as PUSCH_CP_INI_VOL_64TBL_MCS04,
		sum(M55304C03005) as PUSCH_CP_INI_VOL_64TBL_MCS05,
		sum(M55304C03006) as PUSCH_CP_INI_VOL_64TBL_MCS06,
		sum(M55304C03007) as PUSCH_CP_INI_VOL_64TBL_MCS07,
		sum(M55304C03008) as PUSCH_CP_INI_VOL_64TBL_MCS08,
		sum(M55304C03009) as PUSCH_CP_INI_VOL_64TBL_MCS09,
		sum(M55304C03010) as PUSCH_CP_INI_VOL_64TBL_MCS10,
		sum(M55304C03011) as PUSCH_CP_INI_VOL_64TBL_MCS11,
		sum(M55304C03012) as PUSCH_CP_INI_VOL_64TBL_MCS12,
		sum(M55304C03013) as PUSCH_CP_INI_VOL_64TBL_MCS13,
		sum(M55304C03014) as PUSCH_CP_INI_VOL_64TBL_MCS14,
		sum(M55304C03015) as PUSCH_CP_INI_VOL_64TBL_MCS15,
		sum(M55304C03016) as PUSCH_CP_INI_VOL_64TBL_MCS16,
		sum(M55304C03017) as PUSCH_CP_INI_VOL_64TBL_MCS17,
		sum(M55304C03018) as PUSCH_CP_INI_VOL_64TBL_MCS18,
		sum(M55304C03019) as PUSCH_CP_INI_VOL_64TBL_MCS19,
		sum(M55304C03020) as PUSCH_CP_INI_VOL_64TBL_MCS20,
		sum(M55304C03021) as PUSCH_CP_INI_VOL_64TBL_MCS21,
		sum(M55304C03022) as PUSCH_CP_INI_VOL_64TBL_MCS22,
		sum(M55304C03023) as PUSCH_CP_INI_VOL_64TBL_MCS23,
		sum(M55304C03024) as PUSCH_CP_INI_VOL_64TBL_MCS24,
		sum(M55304C03025) as PUSCH_CP_INI_VOL_64TBL_MCS25,
		sum(M55304C03026) as PUSCH_CP_INI_VOL_64TBL_MCS26,
		sum(M55304C03027) as PUSCH_CP_INI_VOL_64TBL_MCS27,
		sum(M55304C03028) as PUSCH_CP_INI_VOL_64TBL_MCS28


FROM NSN5G_RAN_NMPDU
WHERE CELL_NODE_ID IS NOT NULL
GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NMSDU
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NMSDU" ("DATETIME_ID", "CELL_NODE_ID", "DL_MAC_SDU_VOL_DTCH", "UL_MAC_SDU_VOL_DTCH") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID

      ,SUM(M55309C01001) AS DL_MAC_SDU_VOL_DTCH
      ,SUM(M55309C02001) AS UL_MAC_SDU_VOL_DTCH


  FROM NSN5G_RAN_NMSDU 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NOK5G_NNSAUE
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NOK5G_NNSAUE" ("DATETIME_ID", "CELL_NODE_ID", "RLF_INITIATED_UE_RACH_FAIL", "RLF_INITIATED_UE_SCG_CHGE_FAIL") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID
	  ,SUM(M55116C00009) AS RLF_INITIATED_UE_RACH_FAIL
	  ,SUM(M55116C00012) AS RLF_INITIATED_UE_SCG_CHGE_FAIL
  FROM NSN5G_RAN_NNSAUE 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NOK5G_NRACH
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NOK5G_NRACH" ("DATETIME_ID", "CELL_NODE_ID", "RA_MSG3_RECEP") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID
	  ,SUM(M55306C06001) AS RA_MSG3_RECEP
  FROM NSN5G_RAN_NRACH 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NX2CB
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NX2CB" ("DATETIME_ID", "CELL_NODE_ID", "X2_UEREL_INCOMING_RESET", "X2_UEREL_OUTGOING_RESET", "X2_UEREL_INC_PARTIAL_RESET", "X2_UEREL_OUT_PARTIAL_RESET") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID

      ,SUM(M55110C00019) AS X2_UEREL_INCOMING_RESET
      ,SUM(M55110C00020) AS X2_UEREL_OUTGOING_RESET
      ,SUM(M55110C00027) AS X2_UEREL_INC_PARTIAL_RESET
      ,SUM(M55110C00028) AS X2_UEREL_OUT_PARTIAL_RESET


  FROM NSN5G_RAN_NX2CB 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_NX2CC
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_NX2CC" ("DATETIME_ID", "CELL_NODE_ID", "SGNB_RELEASE_REQ_UE_INACT", "NX2CC.X2_SGNB_ADD_REQ_ACK_SENT", "NX2CC.X2_SGNB_RECONF_ACK_RECEIVED", "X2_SGNB_REL_REQ_RECEIVED", "X2_SGNB_REL_REQUIRED_SENT", "NX2CC.X2_SGNB_ADD_REQ_REJ_SENT", "NX2CC.X2_SGNB_RECONF_T", "X2_SGNB_CHG_REQD_SENT", "X2_SGNB_CHG_CONF_RECEIVED", "SGNB_REL_SN_REQ_UE_LOST", "NX2CC_UE_RELEASE_ABNORMAL") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID

      ,SUM(M55112C02001) AS SGNB_RELEASE_REQ_UE_INACT
      ,SUM(M55112C00002) AS "NX2CC.X2_SGNB_ADD_REQ_ACK_SENT"
      ,SUM(M55112C00004) AS "NX2CC.X2_SGNB_RECONF_ACK_RECEIVED"
      ,SUM(M55112C00005) AS X2_SGNB_REL_REQ_RECEIVED
      ,SUM(M55112C00007) AS X2_SGNB_REL_REQUIRED_SENT
      ,SUM(M55112C00010) AS "NX2CC.X2_SGNB_ADD_REQ_REJ_SENT"
      ,SUM(M55112C00012) AS "NX2CC.X2_SGNB_RECONF_T"
      ,SUM(M55112C02501) AS X2_SGNB_CHG_REQD_SENT
      ,SUM(M55112C02502) AS X2_SGNB_CHG_CONF_RECEIVED
	  ,SUM(M55112C00502) AS SGNB_REL_SN_REQ_UE_LOST
	  ,SUM(M55112C01001) AS NX2CC_UE_RELEASE_ABNORMAL

  FROM NSN5G_RAN_NX2CC 
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View HR_RACCU
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."HR_RACCU" ("DATETIME_ID", "CELL_NODE_ID", "AVE_NUMBER_OF_NSA_USERS", "AVE_NUMBER_OF_NSA_USERS_DENOM", "PEAK_NUMBER_OF_NSA_USERS") AS 
  SELECT DATETIME_ID
	  ,CELL_NODE_ID

      ,SUM(M55114C00008) AS AVE_NUMBER_OF_NSA_USERS
      ,SUM(M55114C00009) AS AVE_NUMBER_OF_NSA_USERS_DENOM
	  ,MAX(M55114C00010) AS PEAK_NUMBER_OF_NSA_USERS

  FROM  NSN5G_RAN_RACCU
  WHERE CELL_NODE_ID IS NOT NULL
  GROUP BY DATETIME_ID, CELL_NODE_ID
;
--------------------------------------------------------
--  DDL for View _vDimRan5GCellCached
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "HOANDT"."_vDimRan5GCellCached" ("CELL_NODE_ID", "G_CELLID", "G_CELLNAME", "DISTRICT", "G_NODEID", "G_NODENAME", "VENDOR", "PROVINCE", "SERVICE_REGION", "NOC", "TELCOSUPPORTORG", "TELCOSUPPORTGROUP", "NW_REGION", "TYPE", "BRANCH", "NOC_ORDER", "NW_REGION_ORDER", "DISTRICTID", "TVTID") AS 
  SELECT A.CELL_NODE_ID,
       A.G_CELLID,
       A.G_CELLNAME,
       A.DISTRICT,
       A.G_NODEID,
       A.G_NODENAME,
       A.Vendor,
       A.PROVINCE,
       A.SERVICE_REGION,
       A.NOC,
       A.TELCOSUPPORTORG,
       A.TELCOSUPPORTGROUP,
       A.NW_REGION,
       A.Type,
       A.Branch,
       A.NOC_Order,
       A.NW_REGION_Order,
       B.DistrictId,
       C.TVTId
FROM dimran5gcellcached A
INNER JOIN DimProvinceDistrictDVTTVT B
  ON A.DISTRICT = B.District 
  AND A.PROVINCE = B.Province
INNER JOIN DimDvtTvt C
  ON B.TELCOSUPPORTGROUP = C.TELCOSUPPORTGROUP
  AND B.TELCOSUPPORTORG = C.TELCOSUPPORTORG
;
