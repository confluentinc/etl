# This file stores queries to get latest snapshot for SFDC objects and destination tables
query = {}
destination = {}


objects = ["opportunity", "account", "campaign", "lead", "pricebook entry", "product2", "user", "user role", "case", "contact", "contract", "event", "group",
           "lead history", "opportunity contact role", "opportunity field history", "opportunity history", "opportunity line item", "opportunity stage", "period"]

sfdc_snapshot_query = """
      SELECT * EXCEPT (ROW_NUMBER)
        FROM (SELECT *, 
                     ROW_NUMBER() OVER (PARTITION BY id ORDER BY _sdc_batched_at DESC) ROW_NUMBER
                FROM stitch_sfdc.{})
       WHERE ROW_NUMBER = 1;
"""
for obj in objects:
    query[obj] = sfdc_snapshot_query.format(obj.title().replace(" ", ""))
    if obj not in destination.keys():
        destination[obj] = "_".join(obj.split())