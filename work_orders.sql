{{
    config(
        materialized = 'table',
        schema = 'analytics',
        tags = ["fact", "hourly", "analytics"],
        pre_hook = [
            "USE ROLE SERVICE_DBT"
        ],
        post_hook = [
            "{{ SET_PRIMARY_KEY(['work_order_id']) }}",
            "{{ SET_FOREIGN_KEY('service_request_id', 'service_requests', 'service_request_id') }}",
            "{{ SET_FOREIGN_KEY('vendor_id', 'vendors', 'vendor_id') }}",
            "{{ SET_FOREIGN_KEY('assigned_bo_user_id', 'otto_users', 'bo_user_id') }}"
        ]
    )
}}

WITH
    cte_enums AS (
        -- Capture all enums information to reduce external joins
        SELECT
            enums.constant_name,
            enums.constant_key,
            enums.enum_name
        FROM {{ source('enums', 'enums') }}
    ),

    cte_work_order_status AS (
        -- Get work order status enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS work_order_status,
            cte_enums.constant_key                                                                                                                  AS work_order_status_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.workorders.WorkOrder.Status'
    ),

    cte_completed_by_type AS (
        -- Get entity type enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS completed_by_type,
            cte_enums.constant_key                                                                                                                  AS completed_by_type_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.workorders.CompleteWorkOrderRequest.EntityType'
    ),

    cte_invitation_status AS (
        -- Get invitation status enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS invitation_status_name,
            cte_enums.constant_key                                                                                                                  AS invitation_status_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.workorders.InvitationStatus'
    ),

    cte_updated_by_type AS (
        -- Get invitation entity type enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS updated_by_type,
            cte_enums.constant_key                                                                                                                  AS updated_by_type_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.workorders.InvitationEntityType'

    ),

    cte_reference_type AS (
        -- Get reference type enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS reference_type,
            cte_enums.constant_key                                                                                                                  AS reference_type_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.internal.service.servicerequests.notes.ReferenceType'
    ),

    cte_created_by_type AS (
        -- Get created by type enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS created_by_type,
            cte_enums.constant_key                                                                                                                  AS created_by_type_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.internal.service.servicerequests.notes.CreatedByType'
    ),

    cte_invoice_created_by_type AS (
        -- Get invoice created by type enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS invoice_created_by_type,
            cte_enums.constant_key                                                                                                                  AS invoice_created_by_type_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.internal.service.servicerequests.invoices.CreatedByType'
    ),

    cte_tracking_object_type AS (
        -- Get tracking object type enums for later join
        SELECT
            cte_enums.constant_name                                                                                                                 AS tracking_object_type,
            cte_enums.constant_key                                                                                                                  AS tracking_object_type_code
        FROM cte_enums
        WHERE cte_enums.enum_name = '.internal.service.taskmanagement.timetracking.TimeTrackingObjectType'
            AND cte_enums.constant_name = 'WORK_ORDER'
    ),

    cte_line_items AS (
        -- Capture all line item & invoice data to reduce external joins
        SELECT
            line_item_to_work_order.work_order_id,
            line_items.amount,
            invoices.invoice_id,
            invoices.invoice_date,
            invoices.created_date                                                                                                                    AS created_at
        FROM {{ source('accounting', 'line_item_to_work_order') }}
            INNER JOIN {{ source('accounting', 'line_items') }}
                ON line_item_to_work_order.line_item_id = line_items.line_item_id
            INNER JOIN {{ ref('invoices') }}
                ON line_items.invoice_id = invoices.invoice_id
        WHERE invoices.is_voided = FALSE
            AND invoices.is_paid = TRUE
            AND invoices.is_deleted = FALSE
    ),

    cte_work_orders AS (
        -- Capture all work_orders data to reduce external joins
        SELECT
            work_orders.assigned_bo_user_id,
            work_orders.assigned_vendor_id,
            work_orders.billable_minutes,
            work_orders.completed_by_type,
            work_orders.external_id,
            work_orders.scope_of_work,
            work_orders.service_request_id,
            work_orders.status,
            work_orders.summary,
            work_orders.work_order_id,
            {{ CONVERT_TIMEZONE_PT('work_orders.completed_at') }}                                                                                   AS completed_at,
            {{ CONVERT_TIMEZONE_PT('work_orders.created_at') }}                                                                                     AS created_at
        FROM {{ source('service_requests', 'work_orders') }}
    ),

    cte_trimmed_work_order AS (
        -- Trim down work orders to only those under pre-filtered service requests and work orders that don't have a cancelled or removed status.
        -- Work orders in result are cascaded to all other subsequent CTE.
        SELECT
            cte_work_orders.work_order_id,
            cte_work_orders.external_id,
            cte_work_orders.created_at                                                                                                              AS created_at,
            cte_work_orders.completed_at                                                                                                            AS completed_at,
            cte_work_orders.scope_of_work,
            cte_work_orders.summary,
            service_requests.service_request_id,
            service_requests.lease_flow_id,
            service_requests.type                                                                                                                   AS sr_type,
            cte_work_orders.assigned_vendor_id,
            cte_work_orders.assigned_bo_user_id,
            service_requests.area_id,
            properties.market_id,
            CASE
                WHEN cte_completed_by_type.completed_by_type = 'BO_USER'
                    THEN 'Mynd'
                WHEN cte_completed_by_type.completed_by_type = 'VENDOR'
                    THEN 'Vendor'
            END::VARCHAR                                                                                                                            AS completed_by,
            INITCAP(REPLACE(cte_work_order_status.work_order_status, '_', ' '))::VARCHAR                                                            AS work_order_status,
            ROUND(cte_work_orders.billable_minutes::NUMERIC / 60, 2)                                                                                AS billable_hours
        FROM {{ ref('service_requests') }}
            INNER JOIN cte_work_orders
                ON service_requests.service_request_id = cte_work_orders.service_request_id
            INNER JOIN cte_work_order_status
                ON cte_work_orders.status = cte_work_order_status.work_order_status_code
            LEFT JOIN cte_completed_by_type
                ON cte_work_orders.completed_by_type = cte_completed_by_type.completed_by_type_code
            LEFT JOIN {{ ref('properties') }}
                ON service_requests.asset_id = properties.asset_id
        WHERE cte_work_order_status.work_order_status NOT IN ('CANCELLED', 'REMOVED')
    ),

    cte_in_house_tech_labor_rates AS (
        -- Retrieve Mynd in-house-tech labor rate for work orders completed by Mynd
        SELECT
            cte_trimmed_work_order.work_order_id,
            area_maintenance_rates.maintenance_rate_amount,
            area_maintenance_rates.first_hour_maintenance_rate_amount, --first hour billed at this rate then subsequent time billed at maintenance_rate_amount
            area_maintenance_rates.after_hours_emergency_rate --labor rate multiple for work orders outside business hours
        FROM cte_trimmed_work_order
            LEFT JOIN {{ source('service_requests', 'area_maintenance_rates') }}
                ON cte_trimmed_work_order.area_id = area_maintenance_rates.area_id
        WHERE cte_trimmed_work_order.completed_by = 'Mynd'
    ),

    cte_worker_type AS (
        -- Determines who is performing the work for the work order.
        SELECT
            cte_trimmed_work_order.work_order_id,
            cte_trimmed_work_order.service_request_id,
            vendors.vendor_id,
            vendors.is_mynd_iht_vendor,
            COALESCE(cte_trimmed_work_order.assigned_bo_user_id, cte_trimmed_work_order.assigned_vendor_id)                                         AS worker_id,
            CASE
                WHEN cte_trimmed_work_order.assigned_bo_user_id IS NOT NULL
                    THEN 'Service Tech'
                WHEN cte_trimmed_work_order.assigned_vendor_id IS NOT NULL
                    THEN 'Vendor'
            END::VARCHAR                                                                                                                            AS worker_type,
            -- Indicates whether vendor is Mynd IHT Vendor (True) or 3rd party vendor (False)
            CASE
                WHEN cte_trimmed_work_order.assigned_bo_user_id IS NOT NULL
                    THEN otto_users.name
                WHEN vendors.is_mynd_iht_vendor = TRUE
                    THEN 'Service Tech'
                WHEN cte_trimmed_work_order.assigned_vendor_id IS NOT NULL
                    THEN vendors.name
            END::VARCHAR                                                                                                                            AS worker_name
        FROM cte_trimmed_work_order
            LEFT JOIN {{ ref('vendors') }}
                ON cte_trimmed_work_order.assigned_vendor_id = vendors.vendor_id
            LEFT JOIN {{ ref('otto_users') }}
                ON cte_trimmed_work_order.assigned_bo_user_id = otto_users.bo_user_id
    ),

    cte_invitations AS (
        -- Track the invitation data related to a work order.
        -- Determine when it was rejected, accepted and if a remainder was required. Additionally, who updated the record.
        -- Data is to help figure out timing and who is interacting with the system (mynd vs vendor).
        SELECT
            cte_trimmed_work_order.work_order_id,
            MAX(IFF(cte_invitation_status.invitation_status_name = 'REJECTED', 1, 0)) OVER (
                PARTITION BY vendor_invitations.work_order_id)                                                                                      AS was_rejected,
            MAX(IFF(cte_invitation_status.invitation_status_name = 'ACCEPTED', 1, 0)) OVER (
                PARTITION BY vendor_invitations.work_order_id)                                                                                      AS was_accepted,
            MAX(CASE
                    WHEN cte_invitation_status.invitation_status_name = 'ACCEPTED' AND cte_updated_by_type.updated_by_type = 'VENDOR'
                        THEN 1
                    WHEN cte_invitation_status.invitation_status_name = 'ACCEPTED' AND cte_updated_by_type.updated_by_type = 'BO_USER'
                        THEN 2
                END) OVER (PARTITION BY vendor_invitations.work_order_id)                                                                           AS accepted_by,
            MAX(CASE
                    WHEN vendor_invitations.reminder_was_sent = TRUE AND cte_invitation_status.invitation_status_name = 'ACCEPTED'
                        THEN 1
                    ELSE 0
                END) OVER (PARTITION BY vendor_invitations.work_order_id)                                                                           AS was_reminded,
            MAX(CASE
                    WHEN cte_invitation_status.invitation_status_name = 'ACCEPTED'
                        THEN ROUND(((EXTRACT(EPOCH FROM vendor_invitations.updated_at) - EXTRACT(EPOCH FROM vendor_invitations.created_at)) / 3600)::NUMERIC, 2)
                END) OVER (PARTITION BY vendor_invitations.work_order_id)                                                                           AS hours_to_accept,
            MAX(CASE
                    WHEN cte_invitation_status.invitation_status_name = 'ACCEPTED'
                        THEN ({{ CONVERT_TIMEZONE_PT('vendor_invitations.updated_at') }})
                END) OVER (PARTITION BY vendor_invitations.work_order_id)                                                                           AS accepted_at
        FROM cte_trimmed_work_order
            INNER JOIN {{ source('service_requests', 'vendor_invitations') }}
                ON cte_trimmed_work_order.work_order_id = vendor_invitations.work_order_id
            INNER JOIN cte_invitation_status
                ON vendor_invitations.status = cte_invitation_status.invitation_status_code
            INNER JOIN cte_updated_by_type
                ON vendor_invitations.updated_by_entity_type = cte_updated_by_type.updated_by_type_code
        WHERE cte_invitation_status.invitation_status_name IN ('REJECTED', 'ACCEPTED')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY cte_trimmed_work_order.work_order_id
            ORDER BY vendor_invitations.updated_at DESC) = 1
    ),

    cte_notes AS (
        -- If Mynd or the vendor are adding notes to the work orders.
        SELECT
            cte_trimmed_work_order.work_order_id,
            SUM(IFF(cte_created_by_type.created_by_type = 'VENDOR', 1, 0))                                                                          AS vendor_notes,
            SUM(IFF(cte_created_by_type.created_by_type = 'BO_USER', 1, 0))                                                                         AS mynder_notes,
            SUM(CASE
                    WHEN cte_created_by_type.created_by_type = 'VENDOR' AND note_files.file_id IS NOT NULL
                        THEN 1
                    ELSE 0
                END)                                                                                                                                AS vendor_attachments,
            SUM(CASE
                    WHEN cte_created_by_type.created_by_type = 'BO_USER' AND note_files.file_id IS NOT NULL
                        THEN 1
                    ELSE 0
                END)                                                                                                                                AS mynder_attachments,
            COUNT(notes.note_id)                                                                                                                    AS total_notes,
            COUNT(note_files.file_id)                                                                                                               AS total_attachments
        FROM cte_trimmed_work_order
            INNER JOIN {{ source('service_requests', 'notes') }}
                ON cte_trimmed_work_order.work_order_id = notes.reference_id
            INNER JOIN cte_reference_type
                ON notes.reference_type = cte_reference_type.reference_type_code
            INNER JOIN cte_created_by_type
                ON notes.created_by_type = cte_created_by_type.created_by_type_code
            LEFT JOIN {{ source('service_requests', 'note_files') }}
                ON notes.note_id = note_files.note_id
        WHERE cte_reference_type.reference_type = 'WORK_ORDER'
            AND cte_created_by_type.created_by_type IN ('VENDOR', 'BO_USER')
        GROUP BY cte_trimmed_work_order.work_order_id
    ),

    cte_scheduling AS (
        -- Determining when work is scheduled to be done.
        -- We do not have data on who did the scheduling.
        SELECT
            cte_trimmed_work_order.work_order_id,
            FIRST_VALUE({{ CONVERT_TIMEZONE_PT('scheduled_services.created_at') }}) OVER (
                PARTITION BY scheduled_services.work_order_id
                ORDER BY scheduled_services.created_at ASC)                                                                                         AS initial_scheduled_created_at,
            FIRST_VALUE(scheduled_services.scheduled_date) OVER (
                PARTITION BY scheduled_services.work_order_id
                ORDER BY scheduled_services.created_at ASC)                                                                                         AS initial_scheduled_date
        FROM cte_trimmed_work_order
            INNER JOIN {{ source('service_requests', 'scheduled_services') }}
                ON cte_trimmed_work_order.work_order_id = scheduled_services.work_order_id
        WHERE scheduled_services.removed = 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY scheduled_services.work_order_id
            ORDER BY scheduled_services.created_at ASC) = 1
    ),

    cte_service_requests_invoices AS (
        -- Capture source service_requests.invoice data as cte to resolve SQLFluff L031 | Avoid aliases in from clauses and join conditions.
        SELECT
            invoices.invoice_id,
            invoices.created_by_type
        FROM {{ source('service_requests', 'invoices') }}
    ),

    cte_invoice_details AS (
        -- Data related to invoices and who loaded the invoice (mynd vs vendor).
        -- If multiple then:
        -- First/last dates, total number of invoices
        SELECT
            cte_trimmed_work_order.work_order_id,
            cte_line_items.invoice_id,
            cte_line_items.invoice_date,
            cte_line_items.created_at,
            cte_invoice_created_by_type.invoice_created_by_type
        FROM cte_trimmed_work_order
            INNER JOIN cte_line_items
                ON cte_trimmed_work_order.work_order_id = cte_line_items.work_order_id
            LEFT JOIN cte_service_requests_invoices
                ON cte_line_items.invoice_id = cte_service_requests_invoices.invoice_id
            LEFT JOIN cte_invoice_created_by_type
                ON cte_service_requests_invoices.created_by_type = cte_invoice_created_by_type.invoice_created_by_type_code
        GROUP BY cte_trimmed_work_order.work_order_id, cte_line_items.invoice_id, cte_line_items.invoice_date,
            cte_line_items.created_at, cte_invoice_created_by_type.invoice_created_by_type
    ),

    cte_invoice_aggregation AS (
        -- Aggregating invoice data to avoid duplication and conflicting summation logic
        SELECT
            cte_invoice_details.work_order_id,
            COUNT_IF(cte_invoice_details.invoice_created_by_type = 'VENDOR')                                                                        AS vendor_created_invoices,
            COUNT_IF(cte_invoice_details.invoice_created_by_type = 'BO_USER')                                                                       AS mynd_created_invoices,
            COUNT(cte_invoice_details.work_order_id)                                                                                                AS total_invoices,
            MIN(cte_invoice_details.invoice_date)                                                                                                   AS first_invoice_date,
            MIN(cte_invoice_details.created_at)                                                                                                     AS first_invoice_created_at,
            MAX(cte_invoice_details.invoice_date)                                                                                                   AS last_invoice_date,
            MAX(cte_invoice_details.created_at)                                                                                                     AS last_invoice_created_at
        FROM cte_invoice_details
        GROUP BY cte_invoice_details.work_order_id

    ),

    cte_line_items_amount AS (
        -- Fetching WO amount based on the line items
        SELECT
            cte_trimmed_work_order.work_order_id,
            SUM(cte_line_items.amount)                                                                                                              AS amount
        FROM cte_trimmed_work_order
            INNER JOIN cte_line_items
                ON cte_trimmed_work_order.work_order_id = cte_line_items.work_order_id
        GROUP BY cte_trimmed_work_order.work_order_id
    ),

    cte_logged_hours_aggregation AS (
        -- Get hours logged for work id
        SELECT
            cte_trimmed_work_order.work_order_id,
            ROUND(SUM(((EXTRACT(EPOCH FROM tasks_time_tracking.end_period)
                - EXTRACT(EPOCH FROM tasks_time_tracking.start_period)) / 60 / 60))::NUMERIC, 2)                                                    AS logged_hours
        FROM cte_trimmed_work_order
            INNER JOIN {{ source('task_management', 'tasks_time_tracking') }}
                ON cte_trimmed_work_order.work_order_id = tasks_time_tracking.object_id
            INNER JOIN cte_tracking_object_type
                ON tasks_time_tracking.object_type = cte_tracking_object_type.tracking_object_type_code
        GROUP BY cte_trimmed_work_order.work_order_id
    ),

    cte_wo_approvals AS (
        -- Get earliest updated date for approval status
        SELECT
            cte_work_orders.work_order_id,
            cte_work_orders.created_at                                                                                                              AS created_at,
            MIN(IFF(approval_status_history.approval_status = 1, approval_status_history.updated_at, NULL))                                         AS authorization_requested,
            MIN(IFF(approval_status_history.approval_status IN (2, 3), approval_status_history.updated_at, NULL))                                   AS authorization_approved
        FROM  cte_work_orders
            INNER JOIN {{ source('task_management', 'approvals') }}
                ON cte_work_orders.work_order_id = approvals.related_entity_id
            INNER JOIN {{ source('task_management', 'approvers') }}
                ON approvals.approval_id = approvers.approval_id
            INNER JOIN {{ source('task_management', 'approval_status_history') }}
                ON approvers.approval_id = approval_status_history.approval_id
        GROUP BY cte_work_orders.work_order_id, cte_work_orders.created_at
    ),

    cte_service_requests_tr AS (
        -- Select only work orders that are created within 30 days of move in for a lease
        SELECT
            cte_trimmed_work_order.work_order_id,
            cte_trimmed_work_order.service_request_id,
            leases.lease_flow_id,
            leases.move_in_date
        FROM cte_trimmed_work_order
            INNER JOIN {{ ref('leases') }}
                ON cte_trimmed_work_order.lease_flow_id = leases.lease_flow_id
        WHERE cte_trimmed_work_order.sr_type = 'Service Request'
            AND cte_trimmed_work_order.created_at BETWEEN leases.move_in_date AND DATEADD('day', 30, leases.move_in_date)
    ),

    cte_tech_vendor_csat AS (
        -- Gets the CSAT score of the specific vendor or tech on an SR
        SELECT
            csat.survey_id,
            csat.related_entity_id                                                                                                                  AS service_request_id,
            customer_survey_questions.question_id,
            customer_survey_questions.question_type_id,
            customer_survey_question_types.question_type,
            customer_survey_question_types.text                                                                                                     AS survey_text,
            customer_survey_questions.related_entity_id                                                                                             AS vendor_or_tech_id,
            customer_survey_questions.answer                                                                                                        AS vendor_or_tech_rating,
            csat.surveyed_time                                                                                                                      AS survey_created_at,
            csat.comment                                                                                                                            AS vendor_tech_csat_comment,
            -- Identify the latest record associated with a given service_request_id and vendor_or_tech_id
            -- This allows us to see the latest survey (and related information) associated with a given work_order_id: AE-101
            ROW_NUMBER() OVER (
                PARTITION BY csat.related_entity_id,
                    customer_survey_questions.related_entity_id
                ORDER BY csat.surveyed_time DESC) = 1                                                                                      AS latest_work_order_id_survey
        FROM {{ source('service_requests', 'customer_survey_questions') }}
            INNER JOIN {{ source('service_requests', 'customer_survey_question_types') }}
                ON customer_survey_questions.question_type_id = customer_survey_question_types.question_type_id
            -- AE-42: updated to reference csat model; removes tests [survey_id IN (‘8pmkm8n6qnu5uus1’,‘r31269tah8mlbkjv’)]
            INNER JOIN {{ ref('csat') }}
                ON customer_survey_questions.survey_id = csat.survey_id
        WHERE customer_survey_questions.question_type_id IN (3, 4, 7) -- Survey questions specific to vendor or tech performance
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY csat.survey_id,
                customer_survey_questions.related_entity_id,
                customer_survey_questions.question_type_id
            ORDER BY customer_survey_questions.question_type_id) = 1
    )

-- Combine all work orders and calculate worker type
SELECT
    cte_trimmed_work_order.work_order_id,
    cte_trimmed_work_order.service_request_id,
    cte_trimmed_work_order.external_id,
    cte_trimmed_work_order.lease_flow_id,
    cte_trimmed_work_order.created_at,
    cte_trimmed_work_order.completed_at,
    cte_trimmed_work_order.completed_by,
    cte_trimmed_work_order.work_order_status,
    cte_worker_type.worker_type,
    cte_worker_type.vendor_id,
    cte_worker_type.worker_name,
    -- TODO: rename house_to_accept_invitation to hours_to_accept_invitation (typo); wait for Sigma repo
    cte_invitations.hours_to_accept                                                                                                                 AS house_to_accept_invitaiton,
    cte_invitations.accepted_at                                                                                                                     AS invitation_accepted_at,
    cte_notes.total_notes,
    cte_notes.vendor_notes,
    cte_notes.mynder_notes,
    cte_trimmed_work_order.scope_of_work,
    cte_trimmed_work_order.summary,
    cte_notes.total_attachments,
    cte_notes.vendor_attachments,
    cte_notes.mynder_attachments,
    cte_scheduling.initial_scheduled_created_at,
    cte_scheduling.initial_scheduled_date,
    cte_line_items_amount.amount                                                                                                                    AS invoice_amount,
    cte_invoice_aggregation.total_invoices,
    cte_invoice_aggregation.first_invoice_date,
    cte_invoice_aggregation.first_invoice_created_at,
    cte_invoice_aggregation.last_invoice_date,
    cte_invoice_aggregation.last_invoice_created_at,
    cte_invoice_aggregation.vendor_created_invoices,
    cte_invoice_aggregation.mynd_created_invoices,
    cte_trimmed_work_order.billable_hours,
    cte_logged_hours_aggregation.logged_hours,
    cte_trimmed_work_order.assigned_bo_user_id::VARCHAR(144)                                                                                        AS assigned_bo_user_id,
    cte_wo_approvals.authorization_requested,
    cte_wo_approvals.authorization_approved,
    cte_in_house_tech_labor_rates.maintenance_rate_amount,
    cte_in_house_tech_labor_rates.first_hour_maintenance_rate_amount,
    cte_in_house_tech_labor_rates.after_hours_emergency_rate,
    cte_worker_type.worker_id,
    COALESCE(cte_invitations.was_rejected = 1, FALSE)                                                                                               AS invitation_rejected,
    COALESCE(cte_invitations.was_accepted = 1, FALSE)                                                                                               AS invitation_accepted,
    CASE
        WHEN cte_invitations.accepted_by = 1
            THEN 'Vendor'
        WHEN cte_invitations.accepted_by = 2
            THEN 'Mynd'
    END::VARCHAR                                                                                                                                    AS invitation_accepted_by,
    COALESCE(cte_invitations.was_reminded = 1, FALSE)                                                                                               AS invitation_reminded,
    (EXTRACT(EPOCH FROM cte_scheduling.initial_scheduled_date) - EXTRACT(EPOCH FROM cte_invitations.accepted_at)) / 3600                            AS hours_to_schedule_work,
    (EXTRACT(EPOCH FROM cte_trimmed_work_order.completed_at) - EXTRACT(EPOCH FROM cte_invitations.accepted_at)) / 3600                              AS hours_to_close_work,
    (EXTRACT(EPOCH FROM cte_invoice_aggregation.last_invoice_created_at) - EXTRACT(EPOCH FROM cte_trimmed_work_order.completed_at)) / 3600          AS hours_to_submit_last_invoice,
    IFF(cte_service_requests_tr.work_order_id IS NOT NULL, 1, 0)                                                                                    AS wo_within_30_move_in,
    cte_worker_type.is_mynd_iht_vendor,
    IFF(cte_worker_type.worker_type = 'Service Tech' AND  cte_trimmed_work_order.work_order_status = 'Completed',
        cte_trimmed_work_order.market_id, NULL)                                                                                                     AS iht_in_market,
    cte_tech_vendor_csat.survey_id,
    cte_tech_vendor_csat.question_id,
    cte_tech_vendor_csat.survey_created_at,
    cte_tech_vendor_csat.vendor_or_tech_rating,
    cte_tech_vendor_csat.vendor_tech_csat_comment
FROM cte_trimmed_work_order
    LEFT JOIN cte_worker_type
        ON cte_trimmed_work_order.work_order_id = cte_worker_type.work_order_id
    LEFT JOIN cte_invitations
        ON cte_trimmed_work_order.work_order_id = cte_invitations.work_order_id
    LEFT JOIN cte_notes
        ON cte_trimmed_work_order.work_order_id = cte_notes.work_order_id
    LEFT JOIN cte_scheduling
        ON cte_trimmed_work_order.work_order_id = cte_scheduling.work_order_id
    LEFT JOIN cte_invoice_aggregation
        ON cte_trimmed_work_order.work_order_id = cte_invoice_aggregation.work_order_id
    LEFT JOIN cte_line_items_amount
        ON cte_trimmed_work_order.work_order_id = cte_line_items_amount.work_order_id
    LEFT JOIN cte_logged_hours_aggregation
        ON cte_trimmed_work_order.work_order_id = cte_logged_hours_aggregation.work_order_id
    LEFT JOIN cte_wo_approvals
        ON cte_trimmed_work_order.work_order_id = cte_wo_approvals.work_order_id
    LEFT JOIN cte_service_requests_tr
        ON cte_trimmed_work_order.work_order_id = cte_service_requests_tr.work_order_id
    LEFT JOIN cte_in_house_tech_labor_rates
        ON cte_trimmed_work_order.work_order_id = cte_in_house_tech_labor_rates.work_order_id
    LEFT JOIN cte_tech_vendor_csat
        ON cte_tech_vendor_csat.service_request_id = cte_worker_type.service_request_id
            AND cte_worker_type.worker_id = cte_tech_vendor_csat.vendor_or_tech_id
            -- Select only the latest survey record
            AND cte_tech_vendor_csat.latest_work_order_id_survey = TRUE