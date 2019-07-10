sql = {}

sql['change_event'] = """
SELECT DISTINCT a.id AS audit_id,
       e.id AS event_id,
       e.field_name,
       e.value,
       e.previous_value
 FROM zendesk.ticket_audits a, unnest(events) AS e
 WHERE a.created_at > (SELECT MAX(created_at) FROM zendesk_v.audit)
"""

sql['audit'] = """
SELECT DISTINCT 
       id,
       ticket_id,
       created_at,
       author_id
  FROM zendesk.ticket_audits
 WHERE created_at > (SELECT MAX(created_at) FROM zendesk_v.audit)
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
    RANK() OVER(PARTITION BY t.id ORDER BY _sdc_sequence DESC) AS rn
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
    RANK() OVER(PARTITION BY id ORDER BY _sdc_sequence DESC) AS rn
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
    RANK() OVER(PARTITION BY ticket_id ORDER BY _sdc_sequence DESC) AS rn
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
sql["organization"] = """
SELECT
  *
FROM (
  SELECT
    DISTINCT id AS id,
    name,
    organization_fields.subscription_type AS subscription_type,
    organization_fields.organization_type AS organization_type,     
    organization_fields.technical_account_manager AS tam,
    organization_fields.timezone AS timezone,
    organization_fields.systems_engineer AS systems_engineer,
    organization_fields.region AS region,
    organization_fields.customer_id AS customer_id,
    organization_fields.critical_customer AS critical_customer,
    CAST(SUBSTR(organization_fields.effective_date, 0, 10) AS DATE) AS effective_date,
    CAST(SUBSTR(organization_fields.renewal_date, 0, 10) AS DATE) AS renewal_date,
    created_at,
    deleted_at,
    details,
    notes,
    RANK() OVER(PARTITION BY id ORDER BY _sdc_sequence DESC) AS rn
  FROM
    zendesk.organizations)
WHERE
  rn = 1 -- get latest update
"""

sql['ticket'] = """
SELECT
  DISTINCT tickets.id,
  tickets.id AS id2,
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
	   		  RANK() OVER(PARTITION BY id ORDER BY _sdc_sequence DESC) AS rn
	     FROM zendesk.tickets)
	    WHERE rn = 1) AS tickets
LEFT JOIN
  zendesk_v.organization organizations
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
    RANK() OVER(PARTITION BY ticket_id ORDER BY _sdc_sequence DESC) AS rn
  FROM
    zendesk.satisfaction_ratings)
WHERE
  rn = 1 -- get latest version 
"""

sql["ticket_csat"] = """
SELECT t.id,
       t.type,
       t.organization,
       t.organization_id,
       score,
       CASE WHEN score = 'bad' THEN 1 ELSE 0 END AS bad,
       CASE WHEN score = 'good' THEN 1 ELSE 0 END AS good,
       CASE WHEN score = 'offered' THEN 1 ELSE 0 END AS offered,
       s.comment,
       s.reason,
       s.updated_at,
       s.created_at
  FROM zendesk_v.satisfaction_rating s
  JOIN zendesk_v.ticket t 
    ON s.ticket_id = t.id
 WHERE type <> 'cloud-professional'
   AND organization_id <> 360023063623 -- cloud professional org
"""

sql["csat_trend"] = """
WITH csat_l90d AS (
	SELECT date,
	       SUM(n.good) AS good_l90d,
	       SUM(n.bad) AS bad_l90d,
	       100*SUM(n.good)/(SUM(n.good) + SUM(n.bad)) AS csat_l90d
	  FROM UNNEST(GENERATE_DATE_ARRAY('2018-01-01', DATE_ADD(CURRENT_DATE, INTERVAL -1 DAY))) AS date
	 CROSS JOIN zendesk_v.ticket_csat n
	 WHERE CAST(n.created_at AS DATE) BETWEEN DATE_ADD(date, INTERVAL -89 DAY) AND date
	 GROUP BY 1)
,
csat_l30d AS (
  SELECT date,
         SUM(t.good) AS good_l30d,
         SUM(t.bad) AS bad_l30d,
         100* SUM(t.good)/(SUM(t.good) + SUM(t.bad)) AS csat_l30d
    FROM UNNEST(GENERATE_DATE_ARRAY('2018-01-01', DATE_ADD(CURRENT_DATE, INTERVAL -1 DAY))) AS date
   CROSS JOIN zendesk_v.ticket_csat t
   WHERE CAST(t.created_at AS DATE) BETWEEN DATE_ADD(date, INTERVAL -29 DAY) AND date
   GROUP BY 1)

SELECT n.date,
       good_l90d,
       bad_l90d,
       csat_l90d,
       good_l30d,
       bad_l30d,
       csat_l30d
  FROM csat_l90d n
  JOIN csat_l30d t
    ON n.date = t.date
 """

sql["organization_metrics"] = """
  SELECT 
       o.name AS organization,
       o.id AS organization_id,
       o.effective_date,
       o.organization_type,
       o.subscription_type,
       o.renewal_date,
       a.id AS sfdc_account_id,
       a.technical_account_manager__c AS tam,
       a.renewal_risk_status__c AS renewal_risk,
       a.uses_our_ip__c AS use_ip,
       a.subscription_tier__c AS subscription_tier, 
       usr.name AS SE,
       usr2.name AS CAM,
       COUNT(DISTINCT CASE WHEN CAST(t.created_at AS DATE) >= DATE_ADD(CURRENT_DATE,  INTERVAL -90 DAY) THEN t.id ELSE NULL END) AS tickets_l90d,
       COUNT(DISTINCT CASE WHEN CAST(csat.created_at AS DATE) >= DATE_ADD(CURRENT_DATE,  INTERVAL -90 DAY) AND bad = 1 THEN csat.id ELSE NULL END) AS bad_csat_l90d,
       CAST(MAX(t.created_at) AS DATE) AS last_ticket_submitted
  FROM zendesk_v.organization o
  LEFT JOIN zendesk_v.ticket t
    ON o.id = t.organization_id
  LEFT JOIN zendesk_v.ticket_csat csat 
    ON csat.id = t.id
  LEFT JOIN zendesk_v.zd_sfdc_mapping m
    ON m.org_id = o.id
  LEFT JOIN sfdc.account a
    ON m.account_id = a.id
  LEFT JOIN sfdc.user usr
    ON usr.id = a.sales_engineer_se__c
  LEFT JOIN confluent_sfdc.accounts_view av
    ON av.id = m.account_id
  LEFT JOIN sfdc.user usr2
    ON usr2.id = av.account_manager_c
--    LEFT JOIN (SELECT * FROM renewals where rn = 1) opp
--     ON m.account_id = opp.account_id
 WHERE o.deleted_at is null
      AND NOT (effective_date is null AND renewal_date is null AND renewal_risk_status__c is null)
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
 ORDER BY renewal_date desc
"""
sql["rep_organization_mapping"] = """

 SELECT usr.name AS user,
        usr.firstname AS first_name,
        usr.lastname AS last_name,
        'Rep' AS role,
        o.owner_role__c AS owner_role,
        m.org_id AS organization_id,
        m.org_name AS organization_name
   FROM sfdc.opportunity o
   JOIN zendesk_v.zd_sfdc_mapping m
     ON o.id = m.opp_id
   LEFT JOIN sfdc.user usr
    ON o.ownerid = usr.id
  WHERE m.org_id <> -1
  GROUP BY 1,2,3,4,5,6,7
  -- Rep on opportunity
  UNION ALL
  -- Managers who covers the roles
 SELECT r.manager AS user,
 		usr.firstname AS first_name,
        usr.lastname AS last_name,
        'Manager' AS role,
        manager_role AS user_role,
        m.org_id,
        m.org_name
   FROM metrics.opportunity_fact o
   JOIN zendesk_v.zd_sfdc_mapping m
     ON o.id = opp_id
   LEFT JOIN workspace_yiying.sfdc_user_role_mapping r
     ON r.role = o.owner_role
   LEFT JOIN sfdc.user usr
    ON r.manager_id = usr.id
  WHERE m.org_id <> -1
    AND r.manager_role <> 'Companywide'
  GROUP BY 1,2,3,4,5,6,7
  UNION ALL
   -- Account manager
 SELECT usr.name AS user,
        usr.firstname AS first_name,
        usr.lastname AS last_name,
        'Account Manager' AS role,
        r.name AS owner_role,
        m.org_id AS organization_id,
        m.org_name AS organization_name
   FROM confluent_sfdc.accounts_view a
   JOIN zendesk_v.zd_sfdc_mapping m
     ON a.id = m.account_id
   LEFT JOIN sfdc.user usr
     ON usr.id = a.account_manager_c
   LEFT JOIN stitch_sfdc.UserRole_view r
     ON r.id = usr.userroleid
  WHERE m.org_id <> -1
    AND account_manager_c IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7
  UNION ALL
   -- SE
  SELECT usr.name AS user,
         usr.firstname AS first_name,
         usr.lastname AS last_name,
        'SE' AS role,
        r.name AS owner_role,
        m.org_id AS organization_id,
        m.org_name AS organization_name
   FROM sfdc.account a
   JOIN zendesk_v.zd_sfdc_mapping m
     ON a.id = m.account_id
   LEFT JOIN sfdc.user usr
     ON usr.id = a.Sales_Engineer_SE__c
   LEFT JOIN stitch_sfdc.UserRole_view r
     ON r.id = usr.userroleid
  WHERE m.org_id <> -1
    AND Sales_Engineer_SE__c IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7
  UNION ALL
  -- SE Manager
 SELECT rm.manager AS user,
        usr2.firstname AS first_name,
        usr2.lastname AS last_name,
        'SE Manager' AS role,
        rm.manager_role AS owner_role,
        m.org_id AS organization_id,
        m.org_name AS organization_name
   FROM sfdc.account a
   JOIN zendesk_v.zd_sfdc_mapping m
     ON a.id = m.account_id
   LEFT JOIN sfdc.user usr
     ON usr.id = a.Sales_Engineer_SE__c
   LEFT JOIN stitch_sfdc.UserRole_view r
     ON r.id = usr.userroleid
   LEFT JOIN workspace_yiying.sfdc_user_role_mapping rm
     ON r.name = rm.role
   LEFT JOIN sfdc.user usr2
    ON rm.manager_id = usr2.id
  WHERE 1 = 1
    AND rm.manager_role <> 'Companywide'
    AND m.org_id <> -1
    AND rm.manager IS NOT NULL
    AND rm.manager_role LIKE '%SE%'
  GROUP BY 1,2,3,4,5,6,7
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