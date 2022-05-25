SELECT
  unfiltered.*
FROM `${PROJECT_ID}.${UNFILTERED_DATASET_ID}.${TABLE_ID}`
  unfiltered
JOIN (
  SELECT
    account_id
  FROM (
    SELECT
      user.account_ids
    FROM `${PROJECT_ID}.${UNFILTERED_DATASET_ID}.${PERMISSIONS_TABLE_ID}`
      user
    CROSS JOIN (
      SELECT
        SHA256(LOWER(SESSION_USER())) hashed_user_id
      FROM `${PROJECT_ID}.${UNFILTERED_DATASET_ID}.${PERMISSIONS_TABLE_ID}`
      WHERE
        (
          SELECT
            TIMESTAMP_MILLIS(last_modified_time)
          FROM `${PROJECT_ID}.${UNFILTERED_DATASET_ID}.__TABLES__`
          WHERE table_id = '${PERMISSIONS_TABLE_ID}'
        ) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${PERMISSIONS_TTL})
      LIMIT 1
    ) required
    WHERE
      required.hashed_user_id = user.hashed_user_id
    LIMIT 1
  ) user_data
  JOIN UNNEST(user_data.account_ids) account_id
) permissions
  ON unfiltered.account_id = permissions.account_id
