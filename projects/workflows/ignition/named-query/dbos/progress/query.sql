SELECT "offset", value FROM dbos.streams WHERE workflow_uuid= :uuid AND key='log' ORDER BY "offset"
