# This file stores queries to get latest snapshot for SFDC objects and destination tables
query = {}
destination = {}


objects_del = ["opportunity", "account", "campaign", "campaign member", "lead", "pricebook entry", "product2", "case", "contact", "contract", "event", "group",
           "lead history", "opportunity contact role", "opportunity field history", "opportunity history", "opportunity line item"]

objects_no_del = ["user", "user role", "opportunity stage", "period"]

snapshot_query = """
      SELECT * EXCEPT (ROW_NUMBER)
        FROM (SELECT *, 
                     ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sdc_batched_at DESC) ROW_NUMBER
                FROM stitch_sfdc.{}
               WHERE isdeleted = FALSE)
       WHERE ROW_NUMBER = 1;
"""

snapshot_query_no_del = """
      SELECT * EXCEPT (ROW_NUMBER)
        FROM (SELECT *, 
                     ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sdc_batched_at DESC) ROW_NUMBER
                FROM stitch_sfdc.{})
       WHERE ROW_NUMBER = 1;
"""

for obj in objects_del:
    if obj not in query.keys():
        query[obj] = snapshot_query.format(obj.title().replace(" ", ""))
    if obj not in destination.keys():
        destination[obj] = "_".join(obj.split())

for obj in objects_no_del:
    if obj not in query.keys():
        query[obj] = snapshot_query_no_del.format(obj.title().replace(" ", ""))
    if obj not in destination.keys():
        destination[obj] = "_".join(obj.split())