WITH numbered AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (ORDER BY ctid) AS new_id
    FROM mock_data
)
UPDATE mock_data
SET id = numbered.new_id
FROM numbered
WHERE mock_data.ctid = numbered.ctid;

ALTER TABLE mock_data
ADD CONSTRAINT mock_data_pk PRIMARY KEY (id);