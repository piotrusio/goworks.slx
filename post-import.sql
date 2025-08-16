USE ERPXL_GO;
GO

-- enable change tracking to the databse
IF NOT EXISTS (
    SELECT 1 
    FROM sys.change_tracking_databases 
    WHERE database_id = DB_ID('ERPXL_GO')
)
BEGIN
    ALTER DATABASE ERPXL_GO
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 1 DAYS, AUTO_CLEANUP = ON);
END

IF OBJECT_ID('CDN.KntAdresy', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.KntAdresy')
)
BEGIN
  ALTER TABLE CDN.KntAdresy ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.KntKarty', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.KntKarty')
)
BEGIN
  ALTER TABLE CDN.KntKarty ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.KntOsoby', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.KntOsoby')
)
BEGIN
  ALTER TABLE CDN.KntOsoby ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.KntPromocje', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.KntPromocje')
)
BEGIN
  ALTER TABLE CDN.KntPromocje ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.PrmKarty', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.PrmKarty')
)
BEGIN
  ALTER TABLE CDN.PrmKarty ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.TwrCeny', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.TwrCeny')
)
BEGIN
  ALTER TABLE CDN.TwrCeny ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.TwrCenyNag', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.TwrCenyNag')
)
BEGIN
  ALTER TABLE CDN.TwrCenyNag ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.TwrKarty', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.TwrKarty')
)
BEGIN
  ALTER TABLE CDN.TwrKarty ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.TwrPromocje', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.TwrPromocje')
)
BEGIN
  ALTER TABLE CDN.TwrPromocje ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.ZamElem', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.ZamElem')
)
BEGIN
  ALTER TABLE CDN.ZamElem ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.ZamNag', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.ZamNag')
)
BEGIN
  ALTER TABLE CDN.ZamNag ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.TraElem', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.TraElem')
)
BEGIN
  ALTER TABLE CDN.TraElem ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.TraNag', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.TraNag')
)
BEGIN
  ALTER TABLE CDN.TraNag ENABLE CHANGE_TRACKING;
END

IF OBJECT_ID('CDN.TraPlat', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1 
  FROM sys.change_tracking_tables 
  WHERE object_id = OBJECT_ID('CDN.TraPlat')
)
BEGIN
  ALTER TABLE CDN.TraPlat ENABLE CHANGE_TRACKING;
END