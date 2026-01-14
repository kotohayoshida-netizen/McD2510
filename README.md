WITH
/*
Step 1: Get all coupon claims, including cashback amount.
Converted to Databricks syntax using prod_silver schema.
*/
coupon_claims AS (
    SELECT
        customer_id,
        coupon_campaign_id,
        claimed_at AS claimed_at_ts, -- Databricks: timestamp is already in proper format
        current_redemption_amount AS cashback_amount
    FROM
        prod_silver.clm.coupon_wallet_customer_coupon_instance
    WHERE
        coupon_campaign_id IN (100349519, 100349530, 100349524)
),

/*
Step 2: Get all historical payments, including txn_id, timestamp, and state.
Using Databricks paylite_acquiring_orders table.
*/
payment_history AS (
    SELECT
        user_id AS customer_id,
        merchant_id AS channel_id,
        uuid AS txn_id,
        paid_at AS paid_at_ts, -- Databricks: timestamp already in proper format
        state AS txn_state
    FROM
        prod_silver.payment.paylite_acquiring_orders
    WHERE
        user_type = 'PAYPAY'
        AND state IN ('COMPLETED', 'AUTHORIZED')
        AND merchant_id IN (
            495441343976603648, -- Delivery
            277976768834846720  -- MO
        )
        AND paid_at >= (CURRENT_TIMESTAMP() - INTERVAL '900' DAY) -- Extended lookback period
),

/*
Step 3: Rank all historical payments for each claim.
*/
ranked_claim_history AS (
    SELECT
        c.customer_id,
        c.coupon_campaign_id,
        c.claimed_at_ts,
        c.cashback_amount,
        h.channel_id,
        h.txn_id,
        h.paid_at_ts,
        h.txn_state,

        -- Rank historical payments for each claim and channel.
        -- rn = 1 will be the most recent (previous) transaction.
        ROW_NUMBER() OVER (
            PARTITION BY
                c.customer_id,
                c.coupon_campaign_id,
                c.claimed_at_ts,
                h.channel_id
            ORDER BY
                h.paid_at_ts DESC
        ) AS rn
    FROM
        coupon_claims AS c
    LEFT JOIN
        payment_history AS h
        ON c.customer_id = h.customer_id
        -- Condition 1: Payment must be *before* the claim
        AND h.paid_at_ts < c.claimed_at_ts
        -- Condition 2: Payment must be *within* 365 days of the claim
        AND h.paid_at_ts >= (c.claimed_at_ts - INTERVAL '365' DAY)
),

/*
Step 4: Group by each claim to get one row per claim.
Pivot to find the txn_id, txn_time, and txn_state for the
most recent (rn = 1) historical payment for each channel.
*/
final_claim_data AS (
    SELECT
        customer_id,
        coupon_campaign_id,
        claimed_at_ts,
        cashback_amount,

        -- Delivery (495...)
        MAX(
            CASE
                WHEN channel_id = 495441343976603648 AND rn = 1 THEN txn_id
                ELSE NULL
            END
        ) AS previous_delivery_txn_id,
        MAX(
            CASE
                WHEN channel_id = 495441343976603648 AND rn = 1 THEN paid_at_ts
                ELSE NULL
            END
        ) AS previous_delivery_txn_time,
        MAX(
            CASE
                WHEN channel_id = 495441343976603648 AND rn = 1 THEN txn_state
                ELSE NULL
            END
        ) AS previous_delivery_txn_state,

        -- MO (277...)
        MAX(
            CASE
                WHEN channel_id = 277976768834846720 AND rn = 1 THEN txn_id
                ELSE NULL
            END
        ) AS previous_mo_txn_id,
        MAX(
            CASE
                WHEN channel_id = 277976768834846720 AND rn = 1 THEN paid_at_ts
                ELSE NULL
            END
        ) AS previous_mo_txn_time,
        MAX(
            CASE
                WHEN channel_id = 277976768834846720 AND rn = 1 THEN txn_state
                ELSE NULL
            END
        ) AS previous_mo_txn_state

    FROM
        ranked_claim_history
    GROUP BY
        customer_id, coupon_campaign_id, claimed_at_ts, cashback_amount
),

/*
Step 5: Filter for INCORRECTLY claimed coupons only.
These are coupons claimed by users who had previous transactions
(meaning they should NOT have received "new user" coupons).
*/
incorrectly_claimed_coupons AS (
    SELECT
        customer_id,
        coupon_campaign_id,
        claimed_at_ts,
        cashback_amount,
        previous_delivery_txn_id,
        previous_delivery_txn_time,
        previous_delivery_txn_state,
        previous_mo_txn_id,
        previous_mo_txn_time,
        previous_mo_txn_state,

        -- Determine which channel had the incorrect claim
        CASE
            WHEN previous_delivery_txn_id IS NOT NULL AND previous_mo_txn_id IS NOT NULL THEN 'Both Channels'
            WHEN previous_delivery_txn_id IS NOT NULL THEN 'Delivery'
            WHEN previous_mo_txn_id IS NOT NULL THEN 'MO'
            ELSE NULL
        END AS incorrectly_claimed_channel
    FROM
        final_claim_data
    WHERE
        -- Only keep claims where user had previous transactions
        -- (incorrectly received "new user" coupon)
        previous_delivery_txn_id IS NOT NULL
        OR previous_mo_txn_id IS NOT NULL
),

/*
Step 6: Enrich rewards with customer_id by joining to promo_event.
This CTE pre-filters rewards and adds customer information to enable proper filtering.
*/
rewards_with_customer AS (
    SELECT
        r.campaign_id,
        r.cents,
        pe.customer_id,
        pe.event_key AS redemption_txn_id,
        pe.original_amount AS redemption_txn_amount,
        pe.created_at AS redemption_time
    FROM
        prod_silver.clm.clm_santa_reward AS r
    JOIN
        prod_silver.clm.clm_santa_promo_event AS pe
        ON r.promo_event_id = pe.id
    WHERE
        r.created_at > (CURRENT_TIMESTAMP() - INTERVAL '900' DAY)
        AND pe.created_at > (CURRENT_TIMESTAMP() - INTERVAL '900' DAY)
        AND try_cast(pe.event_key AS BIGINT) IS NOT NULL
),

/*
Step 7: Find all redemption transactions for these incorrectly claimed coupons.
Now using rewards_with_customer to ensure we match on both campaign_id AND customer_id.
*/
all_redemption_transactions AS (
    SELECT
        ic.customer_id,
        ic.coupon_campaign_id,
        ic.claimed_at_ts,
        ic.cashback_amount AS final_redemption_amount, -- This is current_redemption_amount from CCI
        ic.incorrectly_claimed_channel,
        ic.previous_delivery_txn_id,
        ic.previous_delivery_txn_time,
        ic.previous_delivery_txn_state,
        ic.previous_mo_txn_id,
        ic.previous_mo_txn_time,
        ic.previous_mo_txn_state,
        r.redemption_txn_id,
        r.redemption_txn_amount,
        r.redemption_time
    FROM
        incorrectly_claimed_coupons AS ic
    JOIN
        prod_silver.clm.coupon_wallet_customer_coupon_instance AS cci
        ON ic.customer_id = cci.customer_id
        AND ic.coupon_campaign_id = cci.coupon_campaign_id
        AND ic.claimed_at_ts = cci.claimed_at
    JOIN
        rewards_with_customer AS r
        ON cci.coupon_campaign_id = r.campaign_id
        AND cci.customer_id = r.customer_id  -- Now filters by customer!
),

/*
Step 8: Get payout transaction info with parsed cashback JSON for each redemption transaction.
*/
payout_json AS (
    SELECT
        art.redemption_txn_id AS order_id,
        pt.tax_rate,
        from_json(
            pt.cashback_info,
            'STRUCT<cashbackDetails: ARRAY<STRUCT<feeType: STRING, commissionRate: DOUBLE, commissionAmount: DOUBLE, feeEligibleAmount: DOUBLE, taxAmount: DOUBLE>>>'
        ) AS cashback_json
    FROM
        prod_silver.payout.ddb_payout_transaction AS pt
    JOIN
        all_redemption_transactions AS art
        ON pt.order_id = art.redemption_txn_id
    WHERE
        pt.created_at > (CURRENT_TIMESTAMP() - INTERVAL '900' DAY)
),

/*
Step 9: Extract PLC fees from the cashback JSON for each redemption transaction.
*/
payout_fees_per_transaction AS (
    SELECT
        pj.order_id,
        pj.tax_rate,
        detail.commissionRate AS commission_rate,
        SUM(detail.feeEligibleAmount) AS fee_eligible_amount,
        SUM(detail.commissionAmount) AS plc_fee,
        SUM(detail.taxAmount) AS plc_tax
    FROM
        payout_json AS pj
        LATERAL VIEW explode(pj.cashback_json.cashbackDetails) AS detail
    WHERE
        detail.feeType = 'PLC'
    GROUP BY
        pj.order_id,
        pj.tax_rate,
        detail.commissionRate
),

/*
Step 10: Deduplicate and prepare final output.
Group by user_id + coupon_id + redemption_txn_id to ensure one row per combination.
*/
deduplicated_data AS (
    SELECT
        art.customer_id,
        art.coupon_campaign_id,
        art.redemption_txn_id,
        MAX(art.claimed_at_ts) AS claimed_at_ts,
        MAX(art.final_redemption_amount) AS final_redemption_amount,
        MAX(art.incorrectly_claimed_channel) AS incorrectly_claimed_channel,
        MAX(art.previous_delivery_txn_id) AS previous_delivery_txn_id,
        MAX(art.previous_delivery_txn_time) AS previous_delivery_txn_time,
        MAX(art.previous_delivery_txn_state) AS previous_delivery_txn_state,
        MAX(art.previous_mo_txn_id) AS previous_mo_txn_id,
        MAX(art.previous_mo_txn_time) AS previous_mo_txn_time,
        MAX(art.previous_mo_txn_state) AS previous_mo_txn_state,
        MAX(art.redemption_txn_amount) AS redemption_txn_amount,
        MAX(art.redemption_time) AS redemption_time,
        MAX(pf.fee_eligible_amount) AS fee_eligible_amount,
        MAX(pf.tax_rate) AS tax_rate,
        MAX(pf.commission_rate) AS commission_rate,
        MAX(pf.plc_fee) AS plc_fee,
        MAX(pf.plc_tax) AS plc_tax
    FROM
        all_redemption_transactions AS art
    LEFT JOIN
        payout_fees_per_transaction AS pf
        ON art.redemption_txn_id = pf.order_id
    GROUP BY
        art.customer_id,
        art.coupon_campaign_id,
        art.redemption_txn_id
)

/*
Step 11: Final output with all relevant information.
One row per user_id + coupon_id + redemption_txn_id combination.
*/
SELECT
    customer_id AS user_id,

    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY claimed_at_ts ASC, redemption_time ASC
    ) AS transaction_sequence_by_user,

    coupon_campaign_id AS coupon_id,
    final_redemption_amount,
    claimed_at_ts AS coupon_claimed_at,
    incorrectly_claimed_channel,

    -- User status flags
    CASE
        WHEN previous_delivery_txn_id IS NOT NULL THEN 'Existing Delivery User'
        ELSE 'New Delivery User'
    END AS Delivery_User_Status,

    CASE
        WHEN previous_mo_txn_id IS NOT NULL THEN 'Existing MO User'
        ELSE 'New MO User'
    END AS MO_User_Status,

    -- Previous transaction details (proof of existing user)
    previous_delivery_txn_id,
    previous_delivery_txn_time,
    previous_delivery_txn_state,
    previous_mo_txn_id,
    previous_mo_txn_time,
    previous_mo_txn_state,

    -- Redemption transaction details
    redemption_txn_id,
    redemption_txn_amount,
    redemption_time,

    -- Payout fee details for this specific transaction
    fee_eligible_amount,
    tax_rate,
    commission_rate,
    plc_fee,
    plc_tax,

    -- Calculated total payout cost for this transaction
    (COALESCE(plc_fee, 0) + COALESCE(plc_tax, 0)) AS total_payout_cost

FROM
    deduplicated_data
ORDER BY
    user_id,
    transaction_sequence_by_user;
