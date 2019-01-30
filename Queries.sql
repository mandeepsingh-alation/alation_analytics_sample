-- ######################################################################################## --

-- Alation Analytics queries
-- Author: Mandeep Singh
  
-- User active/inactive
SELECT
    AU.user_name,
    MAX(DATE(AV.timestamp)) AS lastVisitDate,
    CURRENT_DATE - MAX(DATE(AV.timestamp)) AS daysSinceLastLogin,
    -- If a user has not logged in for more than 60 days, they can be marked as inactive
    CASE
        WHEN (CURRENT_DATE - MAX(DATE(AV.timestamp))) > 60 THEN 'Inactive'
        WHEN (CURRENT_DATE - MAX(DATE(AV.timestamp))) <= 60 THEN 'Active'
    END AS activityFlag
FROM
    public.alation_visits AS AV
JOIN
    public.alation_user AS AU
    ON
        AU.user_id = AV.user_id
WHERE
    AU.is_active = True
GROUP BY
    AU.user_name;

-- ######################################################################################## --

-- Total and distinct queries by day
SELECT
    date(executed_at_ts) AS Date,
    COUNT(query_statement_fp) AS number_of_queries,
    COUNT(DISTINCT query_statement_fp) AS number_of_unique_queries,
    COUNT(DISTINCT user_id) as number_of_active_users
FROM
    public.compose_query_log
GROUP BY
    date(executed_at_ts);

-- ######################################################################################## --

-- Alation Object Visit Time Series
SELECT
    COUNT(*) AS N,
    AOT.object_type_name,
    DATE(AV.timestamp)
FROM
    public.alation_visits AS AV
JOIN
    public.alation_object_type AS AOT
    ON
        AV.object_type_id = AOT.object_type_id
GROUP BY
    AOT.object_type_name,
    DATE(AV.timestamp);

-- ######################################################################################## --

-- Tableau Specific Object Visit Time Series
SELECT
    COUNT(*) AS N,
    AOT.object_type_name,
    DATE(AV.timestamp)
FROM
    public.alation_visits AS AV
JOIN
    public.alation_object_type AS AOT
    ON
        AV.object_type_id = AOT.object_type_id
WHERE
    AOT.object_type_id IN (SELECT object_type_id FROM public.alation_object_type WHERE object_type_name LIKE '%tableau%')
GROUP BY
    AOT.object_type_name,
    DATE(AV.timestamp);
  
-- ######################################################################################## --

-- article title tag information + all visits
SELECT
    DATE(AV.timestamp) AS Date,
    COUNT(*) AS numberOfVisits,
    OFV.object_uuid AS articleUUID,
    OFV.value_source,
    FV.text_value AS title
FROM
    public.object_field_value AS OFV
JOIN
    public.field_value AS FV
    ON
        OFV.value_fp = FV.value_fp
JOIN
    public.alation_visits AS AV
    ON
        OFV.object_uuid = AV.object_uuid
    AND
        OFV.object_type_id = AV.object_type_id
WHERE
    -- field ID = 3 is title
    OFV.field_id = 3
AND
    -- object ID = 0 is article
    OFV.object_uuid IN (SELECT object_uuid FROM public.alation_object WHERE object_type_id = 0)
AND
    -- remember, UUID = object UUID + type ID
    OFV.object_type_id = 0
GROUP BY
    DATE(AV.timestamp),
    OFV.object_uuid,
    OFV.value_source,
    FV.text_value;

-- ######################################################################################## --

-- The following query produces a list of unique alation objects which have stewards assigned to them
-- The last column, is_deleted, is True when the object has been deleted. Select the data you are
-- interested in.
SELECT DISTINCT
    AO.object_uuid,
    OT1.object_type_name AS objectType,
    AO.object_url,
    FV.object_type_uuid_value AS stewardUUID,
    AU.user_name AS stewardUsername,
    AU.email AS stewardEmail,
    AU.is_active AS stewardActiveFlag,
    AU.display_name AS stewardDisplayname,
    AU.is_admin,
    AU.user_type,
    FV3.boolean_value AS is_deleted
FROM
    public.alation_object AS AO
    -- Join on object UUID and object type ID to obtain value_fp associated with the catalog object
JOIN
    public.object_field_value AS OFV
    ON
        AO.object_uuid = OFV.object_uuid
        AND
        AO.object_type_id = OFV.object_type_id
-- Use value_fp to get the attached steward UUID and object type ID
JOIN
    public.field_value AS FV
    ON
        OFV.value_fp = FV.value_fp
-- Using object UUID and type ID, we get the value_fp associated to the stewards
JOIN
    public.object_field_value as OFV2
    ON
        FV.object_type_uuid_value = OFV2.object_uuid
    AND
        FV.object_type_id_value = OFV2.object_type_id
-- Now we get the value of the steward fields
JOIN
    public.field_value AS FV2 
    ON 
        OFV2.value_fp = FV2.value_fp
-- Get user information
JOIN
    public.alation_user AS AU 
    ON
        AU.email = FV2.text_value
-- Get object type information
JOIN
    public.alation_object_type AS OT1
    ON
        AO.object_type_id = OT1.object_type_id
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFV3
    ON
        AO.object_uuid = OFV3.object_uuid
    AND
    AO.object_type_id = OFV3.object_type_id
-- Get field value
JOIN
    public.field_value AS FV3
    ON
        OFV3.value_fp = FV3.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFV3.field_id = OBF.field_id
WHERE
    -- Requesting field_id for stewards only
    OFV.field_id IN (SELECT field_id FROM public.object_field WHERE field_name = 'steward')
AND
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted';

-- ######################################################################################## --

-- The following queries output information on which objects have stewards and which do not

-- We start with potential clean up
DROP TABLE IF EXISTS psCustomTempTable_ObjsWithSteward;
DROP TABLE IF EXISTS psCustomTempTable_allObjs;
DROP INDEX IF EXISTS idx_psMain1;
DROP INDEX IF EXISTS idx_psMain2;
-- Create a temporary table with all the objects with stewards
SELECT DISTINCT
    AO.object_uuid,
    OT1.object_type_name AS objectType,
    AO.object_url,
    FV.object_type_uuid_value AS stewardUUID,
    AU.user_name AS stewardUsername,
    AU.email AS stewardEmail,
    AU.is_active AS stewardActiveFlag,
    AU.display_name AS stewardDisplayname,
    AU.is_admin,
    AU.user_type
INTO
    TEMP TABLE psCustomTempTable_ObjsWithSteward
FROM
    public.alation_object AS AO
-- Join on object UUID and object type ID to obtain value_fp associated with the catalog object
JOIN
    public.object_field_value AS OFV
ON
    AO.object_uuid = OFV.object_uuid
AND
    AO.object_type_id = OFV.object_type_id
-- Use value_fp to get the attached steward UUID and object type ID
JOIN
    public.field_value AS FV
ON
    OFV.value_fp = FV.value_fp
-- Using object UUID and type ID, we get the value_fp associated to the stewards
JOIN
    public.object_field_value as OFV2
ON
    FV.object_type_uuid_value = OFV2.object_uuid
AND
    FV.object_type_id_value = OFV2.object_type_id
-- Now we get the value of the steward fields
JOIN
    public.field_value AS FV2 
ON 
    OFV2.value_fp = FV2.value_fp
-- Get user information
JOIN
    public.alation_user AS AU 
ON
    AU.email = FV2.text_value
-- Get object type information
JOIN
    public.alation_object_type AS OT1
ON
    AO.object_type_id = OT1.object_type_id
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFV3
    ON
        AO.object_uuid = OFV3.object_uuid
    AND
    AO.object_type_id = OFV3.object_type_id
-- Get field value
JOIN
    public.field_value AS FV3
    ON
        OFV3.value_fp = FV3.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFV3.field_id = OBF.field_id
-- Requesting field_id for stewards only
WHERE
    OFV.field_id IN (SELECT field_id FROM public.object_field WHERE field_name = 'steward')
AND
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted'
AND
    -- Get only objects which are not deleted
    FV3.boolean_value IS False;
-- Create an index for optimzed search
CREATE INDEX idx_psMain1 ON psCustomTempTable_ObjsWithSteward(object_uuid,objectType);

-- Create a temporary table with all the objects of types which allow stewards
SELECT DISTINCT
    AO.object_uuid,
    OT1.object_type_name AS objectType
INTO
TEMP TABLE psCustomTempTable_allObjs
FROM
    public.alation_object AS AO
-- Get object type information
JOIN
    public.alation_object_type AS OT1
ON
    AO.object_type_id = OT1.object_type_id
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFV
    ON
        AO.object_uuid = OFV.object_uuid
    AND
    AO.object_type_id = OFV.object_type_id
-- Get field value
JOIN
    public.field_value AS FV
    ON
        OFV.value_fp = FV.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFV.field_id = OBF.field_id
WHERE
    AO.object_type_id
IN 
(SELECT DISTINCT object_type_id FROM public.object_field_value WHERE field_id IN (SELECT field_id FROM public.object_field WHERE field_name = 'steward'))
AND
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted'
AND
    -- Get only objects which are not deleted
    FV.boolean_value IS False;
-- Create an index for optimized search
CREATE INDEX idx_psMain2 ON psCustomTempTable_allObjs(object_uuid,objectType);

-- Now time to get the information on which uuid+objecttype combinations have stewards
SELECT DISTINCT 
    A.*,
    B.object_url,
    stewardUUID,
    stewardUsername,
    stewardEmail,
    stewardActiveFlag,
    stewardDisplayname,
    is_admin,
    user_type,
    CASE
        WHEN stewardUUID IS NULL THEN 'False'
        WHEN stewardUUID IS NOT NULL THEN 'True'
    END AS stewardFlag
FROM
    psCustomTempTable_allObjs AS A
FULL OUTER JOIN
    psCustomTempTable_ObjsWithSteward AS B
ON
    A.object_uuid = B.object_uuid
AND
    A.objectType = B.objectType;
-- Clean up
DROP TABLE IF EXISTS psCustomTempTable_ObjsWithSteward;
DROP TABLE IF EXISTS psCustomTempTable_allObjs;
DROP INDEX IF EXISTS idx_psMain1;
DROP INDEX IF EXISTS idx_psMain2;

-- ######################################################################################## --

-- object flags information
DROP TABLE IF EXISTS psCustomTempTable_allStewards;
DROP TABLE IF EXISTS psCustomTempTable_allUndeletedObjects;
DROP INDEX IF EXISTS idx_psMain1;
DROP INDEX IF EXISTS idx_psMain2;

SELECT DISTINCT
    FV.object_type_uuid_value AS stewardUUID,
    AU.user_id,
    AU.user_name AS stewardUsername,
    AU.email AS stewardEmail,
    AU.is_active AS stewardActiveFlag,
    AU.display_name AS stewardDisplayname
INTO
    TEMP TABLE psCustomTempTable_allStewards
FROM
public.alation_object AS AO
-- Join on object UUID and object type ID to obtain value_fp associated with the catalog object
JOIN
    public.object_field_value AS OFV
    ON
        AO.object_uuid = OFV.object_uuid
    AND
        AO.object_type_id = OFV.object_type_id
-- Use value_fp to get the attached steward UUID and object type ID
JOIN
    public.field_value AS FV
    ON
        OFV.value_fp = FV.value_fp
-- Using object UUID and type ID, we get the value_fp associated to the stewards
JOIN
    public.object_field_value as OFV2
    ON
        FV.object_type_uuid_value = OFV2.object_uuid
    AND
        FV.object_type_id_value = OFV2.object_type_id
-- Now we get the value of the steward fields
JOIN
    public.field_value AS FV2 
    ON 
        OFV2.value_fp = FV2.value_fp
-- Get user information
JOIN
    public.alation_user AS AU 
    ON
        AU.email = FV2.text_value
-- Get object type information
JOIN
    public.alation_object_type AS OT1
    ON
        AO.object_type_id = OT1.object_type_id
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFV3
    ON
        AO.object_uuid = OFV3.object_uuid
    AND
        AO.object_type_id = OFV3.object_type_id
-- Get field value
JOIN
    public.field_value AS FV3
    ON
        OFV3.value_fp = FV3.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFV3.field_id = OBF.field_id
-- Requesting field_id for stewards only
WHERE
    OFV.field_id IN (SELECT field_id FROM public.object_field WHERE field_name = 'steward')
AND
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted'
AND
    -- Get only objects which are not deleted
    FV3.boolean_value IS False;

CREATE INDEX idx_psMain1 ON psCustomTempTable_allStewards(user_id);

-- Now we need all objects which are not deleted so that we can find their flags
SELECT DISTINCT
    AO.object_uuid,
    AO.object_type_id,
    AO.object_url
INTO
    TEMP TABLE psCustomTempTable_allUndeletedObjects
FROM
    public.alation_object AS AO
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFV
    ON
        AO.object_uuid = OFV.object_uuid
    AND
    AO.object_type_id = OFV.object_type_id
-- Get field value
JOIN
    public.field_value AS FV
    ON
        OFV.value_fp = FV.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFV.field_id = OBF.field_id
WHERE
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted'
AND
    -- Get only objects which are not deleted
    FV.boolean_value IS False;
CREATE INDEX idx_psMain2 ON psCustomTempTable_allUndeletedObjects(object_uuid,object_type_id);

-- Grab all the data where users are stewards
(
SELECT
    DATE(OFL.ts_created) AS Date,
    CASE OFL.flag_type
    -- Integer representing the type of the flag: ENDORSEMENT = 1, WARNING = 2, DEPRECATION = 3
    WHEN 1 THEN 'ENDORSEMENT'
    WHEN 2 THEN 'WARNING'
    WHEN 3 THEN 'DEPRECATION'
    END AS flag_type,
    tempTable.stewardUsername AS userName,
    tempTable.stewardActiveFlag AS activeFlag,
    tempTable.stewardDisplayName AS displayName,
    AOT.object_type_name AS objectType,
    OFL.is_propagated,
    'Steward' AS userType,
    COUNT(*) AS numberOfFlags
FROM
    public.object_flags AS OFL
-- keep only undeleted objects
JOIN
    psCustomTempTable_allUndeletedObjects AS AUO
    ON
        OFL.object_uuid = AUO.object_uuid
    AND
        OFL.object_type_id = AUO.object_type_id
JOIN
    psCustomTempTable_allStewards AS tempTable
    ON  
        OFL.user_id = tempTable.user_id
JOIN
    public.alation_object_type AS AOT
    ON
        OFL.object_type_id = AOT.object_type_id
GROUP BY
    DATE(OFL.ts_created),
    OFL.flag_type,
    tempTable.stewardUsername,
    tempTable.stewardActiveFlag,
    tempTable.stewardDisplayName,
    AOT.object_type_name,
    userType,
    OFL.is_propagated
)

UNION

-- Now for the data where users are not stewards
(
SELECT
    DATE(OFL.ts_created) AS Date,
    CASE OFL.flag_type
    -- Integer representing the type of the flag: ENDORSEMENT = 1, WARNING = 2, DEPRECATION = 3
    WHEN 1 THEN 'ENDORSEMENT'
    WHEN 2 THEN 'WARNING'
    WHEN 3 THEN 'DEPRECATION'
    END AS flag_type,
    AU.user_name AS userName,
    AU.is_active AS activeFlag,
    AU.display_name AS displayName,
    AOT.object_type_name AS objectType,
    OFL.is_propagated,
    'Not Steward' AS userType,
    COUNT(*) AS numberOfFlags
FROM
    public.object_flags AS OFL
-- keep only undeleted objects
JOIN
    psCustomTempTable_allUndeletedObjects AS AUO
    ON
        OFL.object_uuid = AUO.object_uuid
    AND
        OFL.object_type_id = AUO.object_type_id

JOIN
    public.alation_user AS AU
    ON
        OFL.user_id = AU.user_id
JOIN
    public.alation_object_type AS AOT
    ON
        OFL.object_type_id = AOT.object_type_id
WHERE
    OFL.user_id NOT IN (SELECT DISTINCT user_id FROM psCustomTempTable_allStewards)
GROUP BY
    DATE(OFL.ts_created),
    OFL.flag_type,
    AU.user_name,
    AU.is_active,
    AU.display_name,
    AOT.object_type_name,
    userType,
    OFL.is_propagated
);
-- clean up
DROP TABLE IF EXISTS psCustomTempTable_allStewards;
DROP TABLE IF EXISTS psCustomTempTable_allUndeletedObjects;
DROP INDEX IF EXISTS idx_psMain1;
DROP INDEX IF EXISTS idx_psMain2;

-- ######################################################################################## --

-- The following query produces a list of all tableau objects
SELECT DISTINCT
    AO.object_uuid,
    AOT.object_type_name,
    AO.object_url
FROM
    public.alation_object AS AO
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFVD
    ON
        AO.object_uuid = OFVD.object_uuid
    AND
        AO.object_type_id = OFVD.object_type_id
-- Get field value
JOIN
    public.field_value AS FVD
    ON
        OFVD.value_fp = FVD.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFVD.field_id = OBF.field_id
JOIN 
    public.alation_object_type AS AOT
ON 
    AOT.object_type_id = AO.object_type_id
WHERE
    AO.object_type_id IN (SELECT object_type_id FROM public.alation_object_type WHERE object_type_name LIKE '%tableau%')
AND
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted'
AND
    -- Get only objects which are not deleted
    FVD.boolean_value IS False;

-- ######################################################################################## --

-- The following query produces a list of all steward attached to non-deleted objects
SELECT DISTINCT
    FV.object_type_uuid_value AS stewardUUID,
    AU.display_name AS Steward,
    AU.email AS stewardEmail,
    AU.is_active AS stewardActiveFlag
FROM
    public.alation_object AS AO
-- Join on object UUID and object type ID to obtain value_fp associated with the catalog object
JOIN
    public.object_field_value AS OFV
    ON
        AO.object_uuid = OFV.object_uuid
    AND
        AO.object_type_id = OFV.object_type_id
-- Use value_fp to get the attached steward UUID and object type ID
JOIN
    public.field_value AS FV
    ON
        OFV.value_fp = FV.value_fp
-- Using object UUID and type ID, we get the value_fp associated to the stewards
JOIN
    public.object_field_value as OFV2
    ON
        FV.object_type_uuid_value = OFV2.object_uuid
    AND
        FV.object_type_id_value = OFV2.object_type_id
-- Now we get the value of the steward fields
JOIN
    public.field_value AS FV2 
    ON 
        OFV2.value_fp = FV2.value_fp
-- Get user information
JOIN
    public.alation_user AS AU 
    ON
        AU.email = FV2.text_value
-- Get object type information
JOIN
    public.alation_object_type AS OT1
    ON
        AO.object_type_id = OT1.object_type_id
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFV3
    ON
        AO.object_uuid = OFV3.object_uuid
    AND
        AO.object_type_id = OFV3.object_type_id
-- Get field value
JOIN
    public.field_value AS FV3
    ON
        OFV3.value_fp = FV3.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFV3.field_id = OBF.field_id
-- Requesting field_id for stewards only
WHERE
    OFV.field_id IN (SELECT field_id FROM public.object_field WHERE field_name = 'steward')
AND
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted'
AND
    -- Get only objects which are not deleted
    FV3.boolean_value IS False;
    
-- ######################################################################################## --

-- Get the following attributes for tableau workbooks:
/*
    1. object_url
    2. owner
    3. created timestamp
    4. last modified timestamp
    5. number of sheets
    6. number of views
    7. dashboard name
    8. last visited timestamp

*/
SELECT DISTINCT
    AO.object_uuid,
    AO.object_url,
    AOT.object_type_name,
    FV_owner.text_value AS "owner",
    FV_created_at.datetime_value AS "created_ts",
    FV_updated_at.datetime_value AS "updated_ts",
    FV_num_sheets.integer_value AS "number_of_sheets",
    FV_num_views.integer_value AS "number_of_views",
    FV_name.text_value AS "object_name",
    MAX(AV.timestamp) AS "last_visit"
FROM
    public.alation_object AS AO
-- Get deletion information
-- First grab the object UUID (uuid + type_id) and get the value_fp (value pointer)
JOIN
    public.object_field_value AS OFVD
    ON
        AO.object_uuid = OFVD.object_uuid
    AND
        AO.object_type_id = OFVD.object_type_id
-- Get field value
JOIN
    public.field_value AS FVD
    ON
        OFVD.value_fp = FVD.value_fp
-- Match on field id to get deletion flag
JOIN
    public.object_field AS OBF
    ON
        OFVD.field_id = OBF.field_id
JOIN
    public.alation_visits AS AV
    ON
        AO.object_uuid = AV.object_uuid
    AND
        AO.object_type_id = AV.object_type_id
JOIN
    public.alation_object_type AS AOT
    ON
        AV.object_type_id = AOT.object_type_id
-- Get owner
JOIN
    public.object_field_value AS OFV_owner
    ON
        AO.object_uuid = OFV_owner.object_uuid
    AND
        AO.object_type_id = OFV_owner.object_type_id
-- Get field value
JOIN
    public.field_value AS FV_owner
    ON
        OFV_owner.value_fp = FV_owner.value_fp
-- Match on field id
JOIN
    public.object_field AS OBF_owner
    ON
        OFV_owner.field_id = OBF_owner.field_id

-- Get created timestamp
JOIN
    public.object_field_value AS OFV_created_at
    ON
        AO.object_uuid = OFV_created_at.object_uuid
    AND
        AO.object_type_id = OFV_created_at.object_type_id
-- Get field value
JOIN
    public.field_value AS FV_created_at
    ON
        OFV_created_at.value_fp = FV_created_at.value_fp
-- Match on field id
JOIN
    public.object_field AS OBF_created_at
    ON
        OFV_created_at.field_id = OBF_created_at.field_id

-- Get updated_at, last updated ts
JOIN
    public.object_field_value AS OFV_updated_at
    ON
        AO.object_uuid = OFV_updated_at.object_uuid
    AND
        AO.object_type_id = OFV_updated_at.object_type_id
-- Get field value
JOIN
    public.field_value AS FV_updated_at
    ON
        OFV_updated_at.value_fp = FV_updated_at.value_fp
-- Match on field id
JOIN
    public.object_field AS OBF_updated_at
    ON
        OFV_updated_at.field_id = OBF_updated_at.field_id

-- Get tableau object name
JOIN
    public.object_field_value AS OFV_name
    ON
        AO.object_uuid = OFV_name.object_uuid
    AND
        AO.object_type_id = OFV_name.object_type_id
-- Get field value
JOIN
    public.field_value AS FV_name
    ON
        OFV_name.value_fp = FV_name.value_fp
-- Match on field id
JOIN
    public.object_field AS OBF_name
    ON
        OFV_name.field_id = OBF_name.field_id

-- Get num_sheets in the workbooks
JOIN
    public.object_field_value AS OFV_num_sheets
    ON
        AO.object_uuid = OFV_num_sheets.object_uuid
    AND
        AO.object_type_id = OFV_num_sheets.object_type_id
-- Get field value
JOIN
    public.field_value AS FV_num_sheets
    ON
        OFV_num_sheets.value_fp = FV_num_sheets.value_fp
-- Match on field id
JOIN
    public.object_field AS OBF_num_sheets
    ON
        OFV_num_sheets.field_id = OBF_num_sheets.field_id

-- Get num_views
JOIN
    public.object_field_value AS OFV_num_views
    ON
        AO.object_uuid = OFV_num_views.object_uuid
    AND
        AO.object_type_id = OFV_num_views.object_type_id
-- Get field value
JOIN
    public.field_value AS FV_num_views
    ON
        OFV_num_views.value_fp = FV_num_views.value_fp
-- Match on field id
JOIN
    public.object_field AS OBF_num_views
    ON
        OFV_num_views.field_id = OBF_num_views.field_id

WHERE
    -- get only workbooks
    AOT.object_type_id = 31
AND
    -- ensure only field_name = 'deleted' field values are extracted
    OBF.field_name = 'deleted'
AND
    -- Get only objects which are not deleted
    FVD.boolean_value IS False
AND
    -- get only field_id = '3234', owner
    OBF_owner.field_id = 3234
AND
    -- get only field_id = '3239', created_at timestamp
    OBF_created_at.field_id = 3039
AND
    -- get only field_id = 3361, updated_at
    OBF_updated_at.field_id = 3361
AND
    -- get only field_id = 3193, name
    OBF_name.field_id = 3193
AND
    -- get only field_id = 3216, num_sheets
    OBF_num_sheets.field_id = 3216
AND
    -- get only field_id = 3219, num_views
    OBF_num_views.field_id = 3219
GROUP BY
    AO.object_uuid,
    AO.object_url,
    AOT.object_type_name,
    FV_owner.text_value,
    FV_created_at.datetime_value,
    FV_updated_at.datetime_value,
    FV_num_sheets.integer_value,
    FV_num_views.integer_value,
    FV_name.text_value;