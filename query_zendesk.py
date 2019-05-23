sql = {}

sql['audit'] = """
SELECT DISTINCT 
       id,
       ticket_id,
       created_at,
       author_id
  FROM zendesk.ticket_audits
 WHERE created_at > (SELECT MAX(created_at) FROM zendesk_v.audit)
"""

sql['change_event'] = """
SELECT DISTINCT a.id AS audit_id,
       e.id AS event_id,
       e.field_name,
       e.value,
       e.previous_value
 FROM zendesk.ticket_audits a, unnest(events) AS e
 WHERE a.created_at > (SELECT MAX(created_at) FROM zendesk_v.audit)
"""

sql["ticket_data"] = """
SELECT
  *
FROM (
  SELECT
    DISTINCT t.id AS ticket_id,
    cust.id AS field_id,
    cust.value AS value,
    cust.value__bo AS value_binary,
    updated_at,
    RANK() OVER(PARTITION BY t.id ORDER BY _sdc_batched_at DESC) AS rn
  FROM
    zendesk.tickets t, UNNEST(custom_fields) AS cust)
WHERE
  rn = 1 -- get latest update
"""
sql["user"] = """
SELECT
  *
FROM (
  SELECT DISTINCT
    id,
    name,
    created_at,
    updated_at,
    email,
    role,
    organization_id,
    default_group_id as group_id,
    time_zone,
    suspended,
    RANK() OVER(PARTITION BY id ORDER BY _sdc_batched_at DESC) AS rn
  FROM
    zendesk.users)
WHERE
  rn = 1 -- get latest version 
"""

sql["ticket_metric"] = """
SELECT
  *
FROM (
  SELECT DISTINCT
    ticket_id,
    created_at,
    solved_at,
    reply_time_in_minutes.calendar AS ttfr,
    full_resolution_time_in_minutes.calendar AS ttr,
    reopens,
    replies,
    requester_wait_time_in_minutes.calendar AS requester_wait_time,
    agent_wait_time_in_minutes.calendar AS agent_wait_time,
    RANK() OVER(PARTITION BY ticket_id ORDER BY _sdc_batched_at DESC) AS rn
  FROM
    zendesk.ticket_metrics
     )
WHERE
  rn = 1 -- get latest version 
"""


sql['ticket_priority'] = """
 SELECT DISTINCT ticket_id,
        value AS priority
  FROM zendesk_v.ticket_data
 WHERE field_id = 33471847
  AND value IS NOT NULL
"""

sql['ticket_time_spent'] = """
SELECT a.ticket_id,
       MAX(c.value) AS time_spent
  FROM zendesk_v.change_event c
  JOIN zendesk_v.audit a
    ON c.audit_id = a.id
 WHERE c.field_name = '34347708'
 GROUP BY 1
 """

sql['ticket_component'] = """
SELECT DISTINCT ticket_id,
       value AS component
  FROM zendesk_v.ticket_data
 WHERE field_id = 33020448
   AND value IS NOT NULL
"""

sql["ticket_cause"] = """
SELECT DISTINCT ticket_id,
       value AS cause
  FROM zendesk_v.ticket_data
 WHERE field_id = 47641647
  AND value IS NOT NULL
"""

sql['ticket_kafka_version'] = """
SELECT DISTINCT ticket_id,
       value AS kafka_version
  FROM zendesk_v.ticket_data
 WHERE field_id = 24843497
  AND value IS NOT NULL
"""

sql['ticket_java_version'] = """
SELECT DISTINCT ticket_id,
       value AS java_version
  FROM zendesk_v.ticket_data
 WHERE field_id = 26235617
  AND value IS NOT NULL
"""

sql['ticket_operating_system'] = """
SELECT DISTINCT ticket_id,
       value AS operating_system
  FROM zendesk_v.ticket_data
 WHERE field_id = 26235607
  AND value IS NOT NULL
"""

sql['bundle_usage'] = """
SELECT DISTINCT ticket_id,
       value_binary AS bundle_usage
  FROM zendesk_v.ticket_data
 WHERE field_id = 360000036206
  AND value IS NOT NULL
"""

sql['ticket_initial_priority'] = """
SELECT DISTINCT first.ticket_id,
       e.previous_value AS initial_priority
  FROM zendesk_v.change_event e
  JOIN 
	   (SELECT a.ticket_id,
		       MIN(event_id) AS first_changed
		  FROM zendesk_v.change_event e
		  JOIN zendesk_v.audit a
		    ON e.audit_id = a.id
		 WHERE e.field_name = '33471847'
		   AND e.previous_value > ""
		 GROUP BY 1) first
    ON first.first_changed = e.event_id
"""

sql['ticket'] = """
SELECT
  DISTINCT tickets.id,
  organizations.name AS organization,
  tickets.organization_id,
  user.name AS assignee,
  tickets.created_at,
  tickets.updated_at,
  CASE tickets.recipient
    WHEN 'support-escalations@confluent.zendesk.com' THEN "escalation"
    WHEN 'escalations@confluent.io' THEN "escalation"
    WHEN 'se-support@confluent.io' THEN "se-support"
    WHEN 'se-support@confluent.zendesk.com' THEN "se-support"
    WHEN 'support@confluent.io' THEN "enterprise"
    WHEN 'support@confluent.zendesk.com' THEN "enterprise"
    WHEN 'cloud-support@confluent.zendesk.com' THEN "cloud-professional"
    WHEN 'cloud-support@confluent.io' THEN "cloud-professional"
    ELSE "enterprise"
  END AS type,
  tickets.status,
  tickets.subject,
  priority.priority,
  ifnull(ip.initial_priority, priority.priority) initial_priority,
  component.component,
  CASE WHEN component.component LIKE '%broker%' THEN 'broker'
       WHEN component.component LIKE '%c3%' THEN 'c3'
       WHEN component.component LIKE '%client%' THEN 'client'
       WHEN component.component LIKE '%kafka_connect%' THEN 'connect'
       WHEN component.component LIKE '%librd%' THEN 'librd'
  ELSE component.component END AS component_group,
  time.time_spent,
  cause.cause,
  kversion.kafka_version,
  bundle.bundle_usage,
  metric.ttfr,
  metric.ttfr/60 AS ttfr_hours,
  metric.ttr/60 AS ttr_hours,
  metric.ttr,
  metric.solved_at,
  metric.agent_wait_time,
  metric.requester_wait_time
FROM
   (SELECT * FROM (
	   SELECT *,
	   		  RANK() OVER(PARTITION BY id ORDER BY _sdc_batched_at DESC) AS rn
	     FROM zendesk.tickets)
	    WHERE rn = 1) AS tickets
LEFT JOIN
  zendesk.organizations
ON
  tickets.organization_id = organizations.id
LEFT JOIN
  zendesk_v.user
ON
  tickets.assignee_id = user.id
LEFT JOIN
  zendesk_v.ticket_priority priority
ON
  tickets.id = priority.ticket_id
LEFT JOIN
  zendesk_v.ticket_initial_priority ip
ON
  tickets.id = ip.ticket_id
LEFT JOIN
  zendesk_v.ticket_component component
ON
  tickets.id = component.ticket_id
LEFT JOIN
  zendesk_v.ticket_cause cause
ON
  tickets.id = cause.ticket_id
LEFT JOIN
  zendesk_v.ticket_kafka_version kversion
ON
  tickets.id = kversion.ticket_id
LEFT JOIN
  zendesk_v.bundle_usage bundle
ON
  tickets.id = bundle.ticket_id
LEFT JOIN
  zendesk_v.ticket_metric metric
ON
  tickets.id = metric.ticket_id
LEFT JOIN
  zendesk_v.ticket_time_spent time
ON
  tickets.id = time.ticket_id
"""

sql["satisfaction_rating"] = """
SELECT
  *
FROM (
  SELECT DISTINCT
    id,
    assignee_id,
    group_id,
	requester_id,
	ticket_id,
    score,
    reason,
    comment,
    created_at,
    updated_at,
    RANK() OVER(PARTITION BY id ORDER BY _sdc_batched_at DESC) AS rn
  FROM
    zendesk.satisfaction_ratings)
WHERE
  rn = 1 -- get latest version 
"""

sql["ticket_csat"] = """
SELECT t.id,
       score,
       CASE WHEN score = 'bad' THEN 1 ELSE 0 END AS bad,
       CASE WHEN score = 'good' THEN 1 ELSE 0 END AS good,
       CASE WHEN score = 'offered' THEN 1 ELSE 0 END AS offered,
       t.updated_at,
       t.created_at
FROM zendesk_v.satisfaction_rating s
JOIN zendesk_v.ticket t ON s.ticket_id = t.id
"""
# select bundle_usage, count(*) as cnt
# from (
# SELECT ticket_id,
#        value AS bundle_usage
#   FROM zendesk_v.ticket_data
#  WHERE field_id = 360000036206
# )
# group by 1
# order by 2 desc

# SELECT
#   *
# FROM (
#   SELECT
#     DISTINCT t.id AS ticket_id,
#     cust.id AS field_id,
#     cust.value AS value,
#     cust.value__bo AS value_binary,
#     updated_at,
#     RANK() OVER(PARTITION BY t.id ORDER BY updated_at DESC) AS rn
#   FROM
#     zendesk.tickets t, UNNEST(custom_fields) AS cust)a
# WHERE
#   rn = 1
#   AND ticket_id IN (13371)
#     order by 1,2