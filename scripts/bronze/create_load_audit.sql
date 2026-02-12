/*
==============================================================
'Creating bronze load audit table' 
==============================================================
Purpose:
	This table stores execution metadata for the Bronze layer
loading procedure. It tracks start time, end time, duration,
execution status(SUCCESS / FAILED), and errormessages. 

It enables monitoring, troubleshouting, and validation of data
loading process to ensure reliability and traceability.
*/

CREATE TABLE IF NOT EXISTS bronze.load_audit(
  audit_id INT AUTO_INCREMENT PRIMARY KEY,
  procedure_name    VARCHAR(100),
  layer_name        VARCHAR(50),
  start_time        DATETIME,
  end_time          DATETIME,
  duration_seconds  INT,
  status            VARCHAR(20),
  error_message     TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
