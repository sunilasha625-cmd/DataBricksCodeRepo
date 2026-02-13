CREATE OR REFRESH STREAMING TABLE lakehousecat.deltadb.drugstb1drugstbl_bronzetbl2
AS
SELECT *
FROM STREAM(lakehousecat.deltadb.drugstb1);