SELECT
    COUNT(*) AS cat_leads_signed_in_april
FROM {{ ref('marketing') }} AS l
WHERE 
    l.lead_behavior_profile = 'cat'  
    AND EXTRACT(MONTH FROM l.won_date) = 4  
