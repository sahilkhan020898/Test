USE SCHEMA {{ params.region_lower }}_etl_full;

CREATE OR REPLACE TABLE etl_basket_item_qty_promo_rec (
 receipt_id int NOT NULL,
 item_order integer,
 rsd varchar(1500),
 rin varchar(80),
 gtin varchar(80),
 qty integer,
 amount double,
 promo_adj_amt double,
 promo_adj_type varchar(80),
 gross_total double,
 orig_gross_total double,
 type varchar(40),
 form varchar(8),
 savings double,
 uom varchar(8),
 uom_amount double,
 sold_by varchar(150),
 banner varchar(160),
 is_promo_adj boolean,
 -- Pulling down from rdl_receipt
 medium varchar
);

INSERT INTO etl_basket_item_qty_promo_rec
SELECT
 e.receipt_id,
 e.item_order,
 e.rsd,
 e.rin,
 e.gtin,
 e.qty,
 e.amount,
 NULL,
 NULL,
 e.gross_total,
 e.gross_total,
 e.type,
 e.form,
 e.savings,
 e.uom,
 e.uom_amount,
 e.sold_by,
 c.banner,
 NULL,
 c.medium -- Pulling down from rdl_receipt
FROM
    etl_basket_item_qty_extracted e
    LEFT JOIN
    etl_basket_banner_cleansed c ON e.receipt_id = c.receipt_id
ORDER BY medium, type, rsd, banner;

--Adjustment type: RSD_MATCH
CREATE OR REPLACE TEMPORARY TABLE promo_rec_rsd_match AS
WITH coupon_rsd_tot AS
(
SELECT
    receipt_id,
     lower(regexp_replace(rsd,'\ ','')) as rsd_trim,
    SUM(gross_total) AS coupon_rsd_gross_total
FROM
    etl_basket_item_qty_extracted r
WHERE
    banner = 'food_lion' AND
    type = 'Coupon'
GROUP BY 1,2
HAVING TRIM(rsd_trim) != ''
)
SELECT
    r.receipt_id,
    r.rsd_trim,
    CAST(r.coupon_rsd_gross_total/item_rsd_count AS double) AS item_rsd_discount_amt
FROM
    coupon_rsd_tot r
    JOIN
    (
    SELECT
        receipt_id,
        lower(regexp_replace(rsd,'\ ',''))as rsd_trim,
        COUNT(*) AS item_rsd_count
    FROM
        etl_basket_item_qty_extracted
    WHERE
        type = 'Item'
    GROUP BY 1,2
    HAVING TRIM(rsd_trim) != ''
    ) i ON 
    (contains(i.rsd_trim,r.rsd_trim) or contains(r.rsd_trim,i.rsd_trim))
    AND i.receipt_id = r.receipt_id;

UPDATE etl_basket_item_qty_promo_rec b
SET
    b.gross_total =
        CASE
        WHEN b.gross_total + d.item_rsd_discount_amt < 0 THEN b.gross_total
        ELSE b.gross_total + d.item_rsd_discount_amt
        END,
    b.promo_adj_amt = item_rsd_discount_amt,
    b.is_promo_adj =
        CASE
        WHEN b.gross_total + d.item_rsd_discount_amt != b.gross_total THEN TRUE ELSE FALSE END,
    b.promo_adj_type = 'RSD_MATCH'
FROM
    (
    SELECT
        r.*,
        COALESCE(item_rsd_discount_amt,0) AS item_rsd_discount_amt
    FROM
        etl_basket_item_qty_promo_rec r
        JOIN
        promo_rec_rsd_match rsd ON (r.receipt_id = rsd.receipt_id AND 
        TRIM(LOWER(regexp_replace(r.rsd,'\ ',''))) != '' AND
        (contains(rsd.rsd_trim,lower(regexp_replace(r.rsd,'\ ',''))) or contains(lower(regexp_replace(r.rsd,'\ ','')),rsd.rsd_trim))
          )
    ) d
WHERE
    d.receipt_id = b.receipt_id AND
    d.rsd=b.rsd AND
    b.type = 'Item';

--Adjustment type: TARGET_SPECIAL_PROMO
UPDATE etl_basket_item_qty_promo_rec
SET
  gross_total = savings*qty,
  promo_adj_type = 'TARGET_SPECIAL_PROMO',
  is_promo_adj = TRUE
WHERE
  receipt_id IN (SELECT object_id FROM pricescout.kvstore_tag WHERE key = 'target_special_promo') AND
  banner = 'target' AND
  savings < amount AND
  amount !=0 AND
  round((amount-savings)/amount,2) <= .4;

--Adjustment type: CVS_NET_PRICE_PAID
CREATE OR REPLACE TEMPORARY TABLE cvs_coupon_total AS
SELECT
  rec.receipt_id,
  SUM(CASE WHEN rec.type = 'Coupon' AND rec.rsd IN ('COUPON', 'MFR COUPON', 'CVS COUPON', 'CVS MFR COUPON', 'BEAUTY CLUB EXTRABUCKS', 'CVS MER COUPON', 'MFR', 'QUARTERLY EXTRABUCKS RE', 'GET 30% OFF JUST FOR YOU') THEN gross_total ELSE 0 END) AS coupon_total
FROM
  etl_basket_item_qty_promo_rec rec
  LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.retailer_id = 'cvs' AND
  s.medium = 'PAPER' AND
  s.transaction_dt BETWEEN '2016-01-01' and '2018-10-31'
GROUP BY 1;
DELETE FROM cvs_coupon_total WHERE coupon_total = 0;

CREATE OR REPLACE TEMPORARY TABLE cvs_item_total AS
SELECT
  receipt_id,
  SUM(CASE WHEN type = 'Item' THEN gross_total ELSE 0 END) item_total
FROM
  etl_basket_item_qty_promo_rec WHERE receipt_id IN (SELECT receipt_id FROM cvs_coupon_total)
GROUP BY 1;
DELETE FROM cvs_item_total WHERE item_total = 0;


UPDATE etl_basket_item_qty_promo_rec r
SET
  gross_total =
  CASE WHEN gross_total+ROUND(((gross_total/item_total)*coupon_total),2) <0  THEN 0 ELSE gross_total+ROUND(((gross_total/item_total)*coupon_total),2) END,
  is_promo_adj = TRUE,
  promo_adj_type = 'CVS_NET_PRICE_PAID',
  promo_adj_amt = ROUND(((gross_total/item_total)*coupon_total),2)
FROM
  cvs_coupon_total c , cvs_item_total t
WHERE
  c.receipt_id = r.receipt_id AND t.receipt_id = r.receipt_id AND
  r.type = 'Item';


--ADJUSTMENT TYPE: LINE ITEM ORDERING (PUBLIX)

CREATE OR REPLACE TEMPORARY TABLE publix_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.TYPE,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.retailer_id = 'publix' AND
  s.medium = 'PAPER'
ORDER BY type, type_row, rsd_row);

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE WHEN p.rsd_row IN ('PROMO', 'PROMOTION', 'Promotion', 'promotion', 'PROMOTIONS', 'PROMTION',
'PROMOITON', 'PRMOTION', 'PROMOTIN', 'PROMTOION', 'PRMOTION', 'PROMATION', 'PORMOTION',
'PROMOTON', 'PROMOTIION', 'PROMOTOIN', 'PROMOTOIN', 'PREMOTION', 'PROMOTIO'
)
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  ---since coupon amounts are negative values
  THEN r.gross_total + p.gross_total_lead_row ELSE r.gross_total END,
--is_promo_adj
is_promo_adj =  CASE WHEN p.rsd_row IN ('PROMO', 'PROMOTION', 'Promotion', 'promotion', 'PROMOTIONS', 'PROMTION',
'PROMOITON', 'PRMOTION', 'PROMOTIN', 'PROMTOION', 'PRMOTION', 'PROMATION', 'PORMOTION',
'PROMOTON', 'PROMOTIION', 'PROMOTOIN', 'PROMOTOIN', 'PREMOTION', 'PROMOTIO'
) AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE WHEN p.rsd_row IN ('PROMO', 'PROMOTION', 'Promotion', 'promotion', 'PROMOTIONS', 'PROMTION',
'PROMOITON', 'PRMOTION', 'PROMOTIN', 'PROMTOION', 'PRMOTION', 'PROMATION', 'PORMOTION',
'PROMOTON', 'PROMOTIION', 'PROMOTOIN', 'PROMOTOIN', 'PREMOTION', 'PROMOTIO'
) AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' ELSE NULL END
FROM publix_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;



-----ADJUSTMENT TYPE: LINE ITEM ORDERING + RSD MATCH-----


---First line item adjustment: match individual coupons with leading item

CREATE OR REPLACE TEMPORARY TABLE gm_ss_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty, rec.qty*rec.amount AS gross_total, rec.type,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
LEAD(REC.qty) OVER (PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE b.key in ('stop_and_shop', 'giant_martins','giant')
and s.medium = 'PAPER'
and s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row);



---Second line item adjustment: aggregate coupon combinations (i.e. bonus buy savings + personal discounts)

CREATE OR REPLACE TEMPORARY TABLE personal_disc_add as
(SELECT receipt_id,  item_order, rsd, qty, gross_total, type, rsd_row, qty_row, gross_total_lead_row, type_row,
CASE WHEN 
( LOWER(LEFT(rsd,5))='bonus' 
OR LOWER(RIGHT(rsd,4))='disc' 
OR LOWER(LEFT(rsd,2))='sc' 
OR LOWER(LEFT(rsd,7))='savings'
OR LOWER(rsd)='savings') 
AND (
LOWER(LEFT(rsd_row,2))='sc' 
OR LOWER(RIGHT(rsd_row,13))='personal disc'
OR LOWER(LEFT(rsd_row,2))='mc' )
   AND TYPE = 'Coupon'
      AND type_row = 'Coupon'
      AND qty_row = 1
      THEN gross_total + gross_total_lead_row
      ELSE gross_total END AS agg_coupon_gross_total,
lead(agg_coupon_gross_total) OVER (PARTITION BY receipt_id ORDER BY item_order ASC) AS lead_agg_coupon
FROM gm_ss_promo_line_order
ORDER BY type, type_row, rsd_row);


---Third line item adjustment: apply either individual coupons or aggregated coupons to each item

CREATE OR REPLACE TEMPORARY TABLE final_line_item_gm_ss AS
(SELECT receipt_id, item_order, rsd, qty, type, gross_total, gross_total_lead_row, qty_row, rsd_row, type_row, lead_agg_coupon,
CASE WHEN 
( LOWER(LEFT(rsd_row,5))='bonus' 
OR LOWER(RIGHT(rsd_row,4))='disc' 
OR LOWER(LEFT(rsd_row,2))='sc' 
OR LOWER(LEFT(rsd_row,7))='savings'
OR LOWER(rsd_row)='savings'
OR LOWER(RIGHT(rsd_row,13))='personal disc'
OR LOWER(LEFT(rsd_row,2))='mc' 
) 
AND TYPE = 'Item'
AND type_row = 'Coupon'
AND abs(lead_agg_coupon) <= gross_total
THEN gross_total + lead_agg_coupon ELSE

(CASE WHEN abs(GROSS_TOTAL_LEAD_ROW) <= gross_total
AND 
( LOWER(LEFT(rsd_row,5))='bonus' 
OR LOWER(RIGHT(rsd_row,4))='disc' 
OR LOWER(LEFT(rsd_row,2))='sc' 
OR LOWER(LEFT(rsd_row,7))='savings'
OR LOWER(rsd_row)='savings'
OR LOWER(RIGHT(rsd_row,13))='personal disc'
OR LOWER(LEFT(rsd_row,2))='mc' 
) 

AND TYPE = 'Item'
AND type_row = 'Coupon'
THEN gross_total + gross_total_lead_row ELSE gross_total END) END AS final_gross_total
FROM personal_disc_add
ORDER BY type, rsd);

---ITEM RSD MATCH

CREATE OR REPLACE TEMPORARY TABLE ss_gm_rsd_match AS
(WITH rsd_coupons AS
(SELECT receipt_id, rsd, sum(final_gross_total) AS rsd_coupon_gross
FROM final_line_item_gm_ss
WHERE 
( LOWER(LEFT(rsd,5))!='bonus' 
OR LOWER(RIGHT(rsd,4))!='disc' 
OR LOWER(LEFT(rsd,2))!='sc' 
OR LOWER(LEFT(rsd,7))!='savings'
OR LOWER(rsd)!='savings'
OR LOWER(RIGHT(rsd,13))!='personal disc'
OR LOWER(LEFT(rsd,2))!='mc' 
) 

AND TYPE = 'Coupon' GROUP BY 1,2)
SELECT r.receipt_id, r.rsd, round(cast(rsd_coupon_gross/item_rsd_count AS double),2) AS item_rsd_discount_amt
FROM rsd_coupons r
JOIN (SELECT receipt_id, rsd, sum(qty) AS item_rsd_count FROM final_line_item_gm_ss WHERE TYPE = 'Item'
GROUP BY 1,2
) i ON i.rsd = r.rsd AND i.receipt_id = r.receipt_id);


--FINAL STEP: ITEM RSD + LINE ITEM ORDERING

CREATE OR REPLACE TEMPORARY TABLE rsd_line_item_final AS
(SELECT m.receipt_id, m.item_order, m.rsd, m.qty, m.type, m.gross_total, m.gross_total_lead_row, m.qty_row, m.rsd_row, m.type_row, m.lead_agg_coupon,
m.final_gross_total, zeroifnull(r.item_rsd_discount_amt) AS item_rsd_discount_amt,
CASE WHEN m.final_gross_total + zeroifnull(r.item_rsd_discount_amt) < 0 THEN 0
ELSE m.final_gross_total + zeroifnull(r.item_rsd_discount_amt) END AS ultimate_gross_total
FROM final_line_item_gm_ss m
LEFT JOIN ss_gm_rsd_match r ON m.receipt_id = r.receipt_id
AND m.rsd = r.rsd AND m.TYPE = 'Item'
ORDER BY type, type_row, rsd_row
);


----UPDATE PROMO REC TABLE WITH ADJUSTED GROSS_TOTALS

UPDATE etl_basket_item_qty_promo_rec b

---gross totals
SET b.gross_total =
CASE WHEN
b.type = 'Item'
AND r.ultimate_gross_total <= b.gross_total THEN r.ultimate_gross_total
ELSE b.gross_total END,

---is_promo_adj
b.is_promo_adj =
CASE WHEN b.type = 'Item'
AND r.ultimate_gross_total < b.gross_total THEN TRUE ELSE FALSE END,

----promo_type
b.promo_adj_type =
CASE WHEN
b.type = 'Item' AND ABS(r.item_rsd_discount_amt) > 0 AND r.final_gross_total = b.gross_total THEN 'RSD_MATCH'
WHEN b.type = 'Item' AND r.item_rsd_discount_amt = 0 AND r.final_gross_total < b.gross_total THEN 'LINE_ITEM_ORDER'
WHEN b.type = 'Item' AND abs(r.item_rsd_discount_amt) > 0 AND r.final_gross_total < b.gross_total THEN 'RSD_MATCH_LINE_ITEM_ORDER'
ELSE NULL END
from rsd_line_item_final r where r.receipt_id = b.receipt_id and r.item_order = b.item_order;



--Set non-adjusted row values
UPDATE etl_basket_item_qty_promo_rec
SET
  is_promo_adj = COALESCE(is_promo_adj,FALSE),
  promo_adj_type = COALESCE(promo_adj_type,'na');


  --ADJUSTMENT TYPE: LINE ITEM ORDERING (SHOPRITE part 1) --full_complete receipts only 


CREATE OR REPLACE TEMPORARY TABLE shoprite_promo_line_order1 AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
 
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key = 'shop_rite' AND
  s.medium = 'PAPER'  AND 
  s.detailed_state='full_complete'
ORDER BY type, type_row, rsd_row
); 

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row ELSE r.gross_total END,
--is_promo_adj
is_promo_adj =  CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' ELSE NULL END
FROM shoprite_promo_line_order1 p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (SHOPRITE part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE shoprite_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row,
editdistance(rec.rsd,rsd_row) as text_dis  -- too add filter later at rule 
 
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key = 'shop_rite' AND
  s.medium = 'PAPER' AND 
  s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row
); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN text_dis < 15 AND p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row 
WHEN LEFT(p.rsd_row,2) in ('SC', 'PC', 'pc', 'sc', 'Sc', 'Pc') AND text_dis < 25 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,
--is_promo_adj
is_promo_adj =  CASE 
WHEN text_dis < 15 AND p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE 
WHEN LEFT(p.rsd_row,2) in ('SC', 'PC', 'pc', 'sc', 'Sc', 'Pc') AND text_dis < 25 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE
ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE
WHEN text_dis < 15 AND p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
WHEN LEFT(p.rsd_row,2) in ('SC', 'PC', 'pc', 'sc', 'Sc', 'Pc') AND text_dis < 25 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM shoprite_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;


--ADJUSTMENT TYPE: LINE ITEM ORDERING (HARRIS TEETER)

CREATE OR REPLACE TEMPORARY TABLE harris_teeter_promo_line_order AS
  (SELECT rec.receipt_id,
          rec.item_order,
          rec.rsd,
          rec.qty*rec.amount AS gross_total,
          rec.type,
          lead(rec.rsd) over(PARTITION BY rec.receipt_id
                           ORDER BY rec.item_order) AS rsd_row,
          lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id
                                        ORDER BY rec.item_order) AS gross_total_lead_row,
          lead(rec.type) over(PARTITION BY rec.receipt_id
                              ORDER BY rec.item_order) AS type_row
   FROM etl_basket_item_qty_promo_rec rec
   LEFT JOIN datamaster.datamaster_banner b
     ON b.KEY = rec.banner
   LEFT JOIN etl_basket_store s
     ON s.receipt_id = rec.receipt_id
   WHERE b.key = 'harris_teeter'
     AND s.medium = 'PAPER'
   ORDER BY type, type_row, rsd_row);

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS

UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE
                        WHEN LEFT(p.rsd_row,3) in ('SC ',
                                                   'sc ',
                                                   'Sc ')
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total
                             THEN r.gross_total + p.gross_total_lead_row
                        WHEN LEFT(p.rsd_row,4) in ('VIC ',
                                                   'vic ',
                                                   'Vic ')
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total
                             THEN r.gross_total + p.gross_total_lead_row
                        WHEN RIGHT(p.rsd,3) = ' PC'
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total
                             THEN r.gross_total + p.gross_total_lead_row
                        WHEN p.rsd_row=p.rsd
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total
                             THEN r.gross_total + p.gross_total_lead_row
                        WHEN LEFT(p.rsd_row,5) in ('EVIC ',
                                                   'evic ')
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total
                             THEN r.gross_total + p.gross_total_lead_row
                        ELSE r.gross_total
                    END,
--is_promo_adj
is_promo_adj = CASE
                   WHEN LEFT(p.rsd_row,3) in ('SC ',
                                              'sc ',
                                              'Sc ')
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total
                        THEN TRUE
                   WHEN LEFT(p.rsd_row,4) in ('VIC ',
                                              'vic ',
                                              'Vic ')
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total
                        THEN TRUE
                   WHEN RIGHT(p.rsd,3) = ' PC'
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total
                        THEN TRUE
                   WHEN p.rsd_row=p.rsd
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total
                        THEN TRUE
                   WHEN LEFT(p.rsd_row,5) in ('EVIC ',
                                              'evic ')
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total
                        THEN TRUE
                   ELSE FALSE
               END,

--promo_adj_type
 promo_adj_type = CASE
                      WHEN LEFT(p.rsd_row,3) in ('SC ',
                                                 'sc ',
                                                 'Sc ')
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total
                           THEN 'LINE_ITEM_ORDER'
                      WHEN LEFT(p.rsd_row,4) in ('VIC ',
                                                 'vic ',
                                                 'Vic ')
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total
                           THEN 'LINE_ITEM_ORDER'
                      WHEN RIGHT(p.rsd,3) = ' PC'
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total
                           THEN 'LINE_ITEM_ORDER'
                      WHEN p.rsd_row=p.rsd
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total
                           THEN 'LINE_ITEM_ORDER'
                      WHEN LEFT(p.rsd_row,5) in ('EVIC ',
                                                 'evic ')
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total
                           THEN 'LINE_ITEM_ORDER'
                      ELSE NULL
                  END
FROM harris_teeter_promo_line_order p
WHERE p.receipt_id = r.receipt_id
  AND p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (WINNDIXIE)

CREATE OR REPLACE
TEMPORARY TABLE winndixie_promo_line_order AS
  (SELECT rec.receipt_id,
          rec.item_order,
          rec.rsd,
          rec.qty*rec.amount AS gross_total,
          rec.type,
          lead(rec.rsd) over(PARTITION BY rec.receipt_id
                           ORDER BY rec.item_order) AS rsd_row,
          lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id
                                        ORDER BY rec.item_order) AS gross_total_lead_row,
          lead(rec.type) over(PARTITION BY rec.receipt_id
                              ORDER BY rec.item_order) AS type_row
   FROM etl_basket_item_qty_promo_rec rec
   LEFT JOIN datamaster.datamaster_banner b
     ON b.KEY = rec.banner
   LEFT JOIN etl_basket_store s
     ON s.receipt_id = rec.receipt_id
   WHERE b.key = 'winn_dixie'
     AND s.medium = 'PAPER'
   ORDER BY type, type_row, rsd_row);

 ----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS

UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE
                        WHEN LEFT(p.rsd_row,3) in ('RC ',
                                                   'MC ',
                                                   'mc ',
                                                   'Rc ',
                                                   'rc ',
                                                   'Mc ')
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        WHEN p.rsd_row=p.rsd
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        WHEN LEFT(p.rsd_row,3)= LEFT(p.rsd,3)
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        ELSE r.gross_total
                    END,
--is_promo_adj
is_promo_adj = CASE
                   WHEN LEFT(p.rsd_row,3) in ('RC ',
                                              'MC ',
                                              'mc ',
                                              'rc ',
                                              'Rc ',
                                              'Mc ')
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
                   WHEN p.rsd_row=p.rsd
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
                   WHEN LEFT(p.rsd_row,3)= LEFT(p.rsd,3)
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
                   ELSE FALSE
               END,
--promo_adj_type
 promo_adj_type = CASE
                      WHEN LEFT(p.rsd_row,3) in ('RC ',
                                                 'MC ',
                                                 'mc ',
                                                 'rc ',
                                                 'Rc ',
                                                 'Mc ')
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
                      WHEN p.rsd_row=p.rsd
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
                      WHEN LEFT(p.rsd_row,3)= LEFT(p.rsd,3)
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
                      ELSE NULL
                  END
FROM winndixie_promo_line_order p
WHERE p.receipt_id = r.receipt_id
AND p.item_order = r.item_order;

 --ADJUSTMENT TYPE: LINE ITEM ORDERING (GIANTEAGLE)

CREATE OR REPLACE
TEMPORARY TABLE gianteagle_promo_line_order AS
  (SELECT rec.receipt_id,
          rec.item_order,
          rec.rsd,
          rec.qty*rec.amount AS gross_total,
          rec.type,
          lead(rec.rsd) over(PARTITION BY rec.receipt_id
                           ORDER BY rec.item_order) AS rsd_row,
          lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id
                                        ORDER BY rec.item_order) AS gross_total_lead_row,
          lead(rec.type) over(PARTITION BY rec.receipt_id
                              ORDER BY rec.item_order) AS type_row
   FROM etl_basket_item_qty_promo_rec rec
   LEFT JOIN datamaster.datamaster_banner b
     ON b.KEY = rec.banner
   LEFT JOIN etl_basket_store s
     ON s.receipt_id = rec.receipt_id
   WHERE b.key = 'giant_eagle'
     AND s.medium = 'PAPER'
   ORDER BY type, type_row, rsd_row);

 ----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS

UPDATE etl_basket_item_qty_promo_rec r --gross_totals
SET r.gross_total = CASE
                        WHEN LEFT(p.rsd_row,3) in ('SC ',
                                                   'sc ',
                                                   'Sc ')
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        WHEN p.rsd_row=p.rsd
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        WHEN LEFT(p.rsd_row,3)= LEFT(p.rsd,3)
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        WHEN LEFT(p.rsd_row,5)= 'FREE '
                             AND p.type_row = 'Coupon'
                             AND abs(p.gross_total_lead_row) = r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        ELSE r.gross_total
                    END,
--is_promo_adj
is_promo_adj = CASE
                   WHEN LEFT(p.rsd_row,3) in ('SC ',
                                              'sc ',
                                              'Sc ')
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
                   WHEN p.rsd_row=p.rsd
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
                   WHEN LEFT(p.rsd_row,3)= LEFT(p.rsd,3)
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
                   WHEN LEFT(p.rsd_row,5)= 'FREE '
                        AND p.type_row = 'Coupon'
                        AND abs(p.gross_total_lead_row) = r.gross_total THEN TRUE
                   ELSE FALSE
               END,
--promo_adj_type
promo_adj_type = CASE
                      WHEN LEFT(p.rsd_row,3) in ('SC ',
                                                 'sc ',
                                                 'Sc ')
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
                      WHEN p.rsd_row=p.rsd
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
                      WHEN LEFT(p.rsd_row,3)= LEFT(p.rsd,3)
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
                      WHEN LEFT(p.rsd_row,5)= 'FREE '
                           AND p.type_row = 'Coupon'
                           AND abs(p.gross_total_lead_row) = r.gross_total THEN 'LINE_ITEM_ORDER'
                      ELSE NULL
                  END
FROM gianteagle_promo_line_order p
WHERE p.receipt_id = r.receipt_id
AND p.item_order = r.item_order;

 
-- ADJUSTMENT TYPE: LINE ITEM ORDERING (RITE_AID)
CREATE OR REPLACE
TEMPORARY TABLE riteaid_promo_line_order AS
  (SELECT rec.receipt_id,
          rec.item_order,
          rec.rsd,
          rec.qty*rec.amount AS gross_total,
          rec.type,
          rec.qty AS qty,
          lead(rec.rsd) over(PARTITION BY rec.receipt_id
                           ORDER BY rec.item_order) AS rsd_row,
          lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id
                                        ORDER BY rec.item_order) AS gross_total_lead_row,
          lead(rec.type) over(PARTITION BY rec.receipt_id
                              ORDER BY rec.item_order) AS type_row,
          lead(rec.qty) over(PARTITION BY rec.receipt_id
                             ORDER BY rec.item_order) AS qty_row
   FROM etl_basket_item_qty_promo_rec rec
   LEFT JOIN datamaster.datamaster_banner b
     ON b.KEY = rec.banner
   LEFT JOIN etl_basket_store s
     ON s.receipt_id = rec.receipt_id
   WHERE b.key = 'rite_aid'
     AND s.medium = 'PAPER'
   ORDER BY type, type_row, rsd_row);

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS

UPDATE etl_basket_item_qty_promo_rec r 
--gross_totals
SET r.gross_total = CASE WHEN LEFT(p.rsd_row,6) in ('MERCHA',
                                                   'MECHAN',
                                                   'Mercha',
                                                   'MERCHN',
                                                   'MERCAN',
                                                   'mercha',
                                                   'MARCHA',
                                                   '1 MERC',
                                                   'LOAD2C',
                                                   'load2c')
                             AND p.qty=p.qty_row
                             AND p.type_row = 'Coupon'
                             AND p.type != 'Coupon'
                             AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
                        ELSE r.gross_total
                    END,
--is_promo_adj
is_promo_adj = CASE WHEN LEFT(p.rsd_row,6) in ('MERCHA',
                                              'MECHAN',
                                              'Mercha',
                                              'MERCHN',
                                              'MERCAN',
                                              'mercha',
                                              'MARCHA',
                                              '1 MERC',
                                              'LOAD2C',
                                              'load2c')
                        AND p.qty=p.qty_row
                        AND p.type_row = 'Coupon'
                        AND p.type != 'Coupon'
                        AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
                   ELSE FALSE
               END, 
--promo_adj_type
 promo_adj_type = CASE WHEN LEFT(p.rsd_row,6) in ('MERCHA',
                                                 'MECHAN',
                                                 'Mercha',
                                                 'MERCHN',
                                                 'MERCAN',
                                                 'mercha',
                                                 'MARCHA',
                                                 '1 MERC',
                                                 'LOAD2C',
                                                 'load2c')
                           AND p.qty=p.qty_row
                           AND p.type_row = 'Coupon'
                           AND p.type != 'Coupon'
                           AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
                      ELSE NULL
                  END
FROM riteaid_promo_line_order p
WHERE p.receipt_id = r.receipt_id
  AND p.item_order = r.item_order;


-- ADJUSTMENT TYPE: LINE ITEM ORDERING (COSTCO)
CREATE OR REPLACE
TEMPORARY TABLE costco_promo_line_order AS
(SELECT rec.receipt_id,
rec.item_order,
rec.RIN,
rec.qty*rec.amount AS gross_total,
rec.type,
rec.qty AS qty,
lead(rec.RIN) over(PARTITION BY rec.receipt_id
ORDER BY rec.item_order) AS RIN_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id
ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id
ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id
ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b
ON b.KEY = rec.banner
LEFT JOIN etl_basket_store s
ON s.receipt_id = rec.receipt_id
WHERE b.key = 'costco'
AND s.medium = 'PAPER'
ORDER BY type, type_row
);

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS

UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE WHEN p.type = 'Item'
AND p.qty=p.qty_row
AND p.type_row = 'Coupon'
AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total
END,

--is_promo_adj
is_promo_adj = CASE
  WHEN p.type = 'Item'
AND p.qty=p.qty_row
AND p.type_row = 'Coupon'
  AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
ELSE FALSE
END,

--promo_adj_type
 promo_adj_type = CASE
  WHEN p.type = 'Item'
AND p.qty=p.qty_row
AND p.type_row = 'Coupon'
  AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
ELSE NULL 
END
FROM costco_promo_line_order p 
WHERE p.receipt_id = r.receipt_id
AND p.item_order = r.item_order;




-- ADJUSTMENT TYPE: LINE ITEM ORDERING (walmart)
CREATE OR REPLACE
TEMPORARY TABLE walmart_promo_line_order AS
(SELECT rec.receipt_id,
rec.item_order,
rec.qty*rec.amount AS gross_total,
rec.type,
rec.qty AS qty,
 rec.rsd,
 lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b
ON b.KEY = rec.banner
WHERE b.key = 'walmart'
AND rec.medium = 'PAPER'
ORDER BY type, type_row, rsd_row
);

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS

UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN p.type = 'Item'
AND p.qty=p.qty_row
AND p.type_row = 'Coupon'
AND abs(p.gross_total_lead_row) <= r.gross_total THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total
END,

--is_promo_adj
is_promo_adj = CASE
  WHEN p.type = 'Item'
AND p.qty=p.qty_row
AND p.type_row = 'Coupon'
  AND abs(p.gross_total_lead_row) <= r.gross_total THEN TRUE
ELSE FALSE
END,

--promo_adj_type
 promo_adj_type = CASE
  WHEN p.type = 'Item'
AND p.qty=p.qty_row
AND p.type_row = 'Coupon'
  AND abs(p.gross_total_lead_row) <= r.gross_total THEN 'LINE_ITEM_ORDER'
ELSE NULL 
END
FROM walmart_promo_line_order  p 
WHERE p.receipt_id = r.receipt_id
AND p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (KROGER part 1) --full_complete receipts only 

CREATE OR REPLACE TEMPORARY TABLE kroger_promo_line_order1 AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
 
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key = 'kroger' AND
  s.medium = 'PAPER'  AND 
  s.detailed_state='full_complete'
ORDER BY type, type_row, rsd_row
); 

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row ELSE r.gross_total END,
--is_promo_adj
is_promo_adj =  CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' ELSE NULL END
FROM kroger_promo_line_order1 p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (KROGER part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE kroger_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key = 'kroger' AND
  s.medium = 'PAPER' AND 
  s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row  
); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN (LOWER(left(p.rsd_row,4))='ecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),6))='scecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),6))='mcecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),9))='megaevent'  
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),11))='scmegaevent' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),11))='mcmegaevent' 
or lower(left(p.rsd_row,7))='scanned'
or lower(left(p.rsd_row,3))='sc ')  --coupon type=ECPN/MEGA EVENT/SCANNED COUPON(CPN PCT>5%)
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,

--is_promo_adj
is_promo_adj =  
CASE
WHEN (LOWER(left(p.rsd_row,4))='ecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),6))='scecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),6))='mcecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),9))='megaevent'  
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),11))='scmegaevent' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),11))='mcmegaevent' 
or lower(left(p.rsd_row,7))='scanned'
or lower(left(p.rsd_row,3))='sc ')  --coupon type=ECPN/MEGA EVENT/SCANNED COUPON(CPN PCT>5%)
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE
ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE
WHEN (LOWER(left(p.rsd_row,4))='ecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),6))='scecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),6))='mcecpn' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),9))='megaevent'  
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),11))='scmegaevent' 
or LOWER(left(regexp_replace(p.rsd_row,'\ ',''),11))='mcmegaevent' 
or lower(left(p.rsd_row,7))='scanned'
or lower(left(p.rsd_row,3))='sc ')  --coupon type=ECPN/MEGA EVENT/SCANNED COUPON(CPN PCT>5%)
  AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM kroger_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;


 --ADJUSTMENT TYPE: (WEIS_MARKETS)LINE ITEM ORDERING for full_complete and conditional for non_full

CREATE OR REPLACE TEMPORARY TABLE weis_markets_promo_line_order1 AS
(SELECT
rec.receipt_id,
rec.item_order,
rec.rsd,
rec.qty * rec.amount AS gross_total,
rec.type,
rec.qty,
lead(rec.rsd) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty * rec.amount) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec AS rec
LEFT JOIN datamaster.datamaster_banner AS b ON b.key = rec.banner
    LEFT JOIN etl_basket_store AS s ON s.receipt_id = rec.receipt_id
WHERE
    b.key = 'weis_markets' AND
    s.medium = 'PAPER' AND 
    s.detailed_state = 'full_complete'
ORDER BY type, type_row, rsd_row
); 

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
     AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
    THEN r.gross_total + p.gross_total_lead_row ELSE r.gross_total END,
--is_promo_adj
is_promo_adj =  CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
     AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
    THEN TRUE ELSE FALSE END,
--promo_adj_type
    promo_adj_type = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
     AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
    THEN 'LINE_ITEM_ORDER' ELSE NULL END
FROM weis_markets_promo_line_order1 p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (weis_markets part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE weis_markets_promo_line_order AS
(SELECT
rec.receipt_id,
rec.item_order,
rec.rsd,
rec.qty * rec.amount AS gross_total,
rec.type,
rec.qty,
lead(rec.rsd) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty * rec.amount) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) OVER(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec AS rec
LEFT JOIN datamaster.datamaster_banner AS b ON b.key = rec.banner
    LEFT JOIN etl_basket_store AS s ON s.receipt_id = rec.receipt_id
WHERE
    b.key = 'weis_markets' AND
    s.medium = 'PAPER' AND 
    s.detailed_state != 'full_complete'
ORDER BY type, type_row, rsd_row
); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN  (LOWER(left(p.rsd_row, 3))='sc ' or
LOWER(RIGHT(p.rsd_row, 8))='discount' or
p.rsd_row=p.rsd or 
LEFT(p.rsd_row, 3)= LEFT(p.rsd, 3)
)  --coupon type=SC/DISCOUNT (PCT>29%)
 AND p.type = 'Item'
    AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
    THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,

--is_promo_adj
is_promo_adj =  
CASE 
WHEN  (LOWER(left(p.rsd_row, 3))='sc ' or
LOWER(RIGHT(p.rsd_row, 8))='discount' or
p.rsd_row=p.rsd or 
LEFT(p.rsd_row, 3)= LEFT(p.rsd, 3)
)  --coupon type=SC/DISCOUNT (PCT>29%)
 AND p.type = 'Item'
    AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
    THEN TRUE
ELSE FALSE END,
--promo_adj_type
    promo_adj_type = CASE
WHEN  (LOWER(left(p.rsd_row, 3))='sc ' or
LOWER(RIGHT(p.rsd_row, 8))='discount' or
p.rsd_row=p.rsd or 
LEFT(p.rsd_row, 3)= LEFT(p.rsd, 3)
)  --coupon type=SC/DISCOUNT (PCT>29%)
  AND p.type = 'Item'
    AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
    THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM weis_markets_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (Walgreens part 1) --full_complete receipts only 

CREATE OR REPLACE TEMPORARY TABLE walgreens_promo_line_order1 AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key = 'walgreens' AND
  s.medium = 'PAPER' AND 
  s.detailed_state='full_complete'
ORDER BY type, type_row, rsd_row
); 

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row ELSE r.gross_total END,
--is_promo_adj
is_promo_adj =  CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' ELSE NULL END
FROM walgreens_promo_line_order1 p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (walgreens part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE walgreens_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key = 'walgreens' AND
  s.medium = 'PAPER' AND 
  s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row
); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN 
( LOWER(RIGHT(P.RSD_ROW,7))='mfg cpn' OR LOWER(RIGHT(P.RSD_ROW,6))='mfgcpn'
OR LOWER(RIGHT(P.RSD_ROW,4))=' cpn'
OR LOWER(RIGHT(P.RSD_ROW,5))=' ecpn' OR LOWER(RIGHT(P.RSD_ROW,7))='mfgecpn' ) --COUPON TYPE IN (ECPN,CPN,) PCT>70% 
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,

--is_promo_adj
is_promo_adj =  
CASE
WHEN  
( LOWER(RIGHT(P.RSD_ROW,7))='mfg cpn' OR LOWER(RIGHT(P.RSD_ROW,6))='mfgcpn'
OR LOWER(RIGHT(P.RSD_ROW,4))=' cpn'
OR LOWER(RIGHT(P.RSD_ROW,5))=' ecpn' OR LOWER(RIGHT(P.RSD_ROW,7))='mfgecpn' ) --COUPON TYPE IN (ECPN,CPN,) PCT>70% 

 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE
ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE
WHEN 
( LOWER(RIGHT(P.RSD_ROW,7))='mfg cpn' OR LOWER(RIGHT(P.RSD_ROW,6))='mfgcpn'
OR LOWER(RIGHT(P.RSD_ROW,4))=' cpn'
OR LOWER(RIGHT(P.RSD_ROW,5))=' ecpn' OR LOWER(RIGHT(P.RSD_ROW,7))='mfgecpn' ) --COUPON TYPE IN (ECPN,CPN,) PCT>70% 
    AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM walgreens_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;

--ADJUSTMENT TYPE: LINE ITEM ORDERING (for meijer,bjs,frys,tops,giant,giant martins,stop) --full_complete receipts only 

CREATE OR REPLACE TEMPORARY TABLE full_com_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key in ('meijer','bjs','frys_marketplace', 'tops_friendly_markets',
'giant','giant_martins', 'stop_and_shop')
 AND
  s.medium = 'PAPER'  AND 
  s.detailed_state='full_complete'
ORDER BY type, type_row, rsd_row
); 

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row ELSE r.gross_total END,
--is_promo_adj
is_promo_adj =  CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE WHEN p.qty=p.qty_row AND p.type = 'Item'
   AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' ELSE NULL END
FROM full_com_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;



--ADJUSTMENT TYPE: LINE ITEM ORDERING (meijer part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE meijer_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key ='meijer' AND
  s.medium = 'PAPER' AND 
  s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row  
); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN 
( LOWER(RIGHT(P.RSD_ROW,3))='off' 
OR LOWER(RIGHT(P.RSD_ROW,3))='oft' 
OR LOWER(RIGHT(P.RSD_ROW,4))='item' 
OR LOWER(LEFT(P.RSD_ROW,6))='coupon' 
 OR LOWER(LEFT(P.RSD_ROW,6))='vendor'
OR LOWER(LEFT(P.RSD_ROW,4))=LOWER(LEFT(P.RSD,4))
OR LOWER(LEFT(REGEXP_REPLACE(P.RSD_ROW,'\ ',''),1))= '=' 
OR LOWER(LEFT(P.RSD_ROW,2))='->' 
OR LOWER(LEFT(P.RSD_ROW,1))='>' 
OR REGEXP_LIKE(P.RSD_ROW,'[0-9]{2}\%.*')
)
--PCT >25%
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,

--is_promo_adj
is_promo_adj =  
CASE
WHEN  
( LOWER(RIGHT(P.RSD_ROW,3))='off' 
OR LOWER(RIGHT(P.RSD_ROW,3))='oft' 
OR LOWER(RIGHT(P.RSD_ROW,4))='item' 
OR LOWER(LEFT(P.RSD_ROW,6))='coupon' 
 OR LOWER(LEFT(P.RSD_ROW,6))='vendor'
OR LOWER(LEFT(P.RSD_ROW,4))=LOWER(LEFT(P.RSD,4))
OR LOWER(LEFT(REGEXP_REPLACE(P.RSD_ROW,'\ ',''),1))= '=' 
OR LOWER(LEFT(P.RSD_ROW,2))='->' 
OR LOWER(LEFT(P.RSD_ROW,1))='>' 
OR REGEXP_LIKE(P.RSD_ROW,'[0-9]{2}\%.*')
)
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE
ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE
WHEN 
( LOWER(RIGHT(P.RSD_ROW,3))='off' 
OR LOWER(RIGHT(P.RSD_ROW,3))='oft' 
OR LOWER(RIGHT(P.RSD_ROW,4))='item' 
OR LOWER(LEFT(P.RSD_ROW,6))='coupon' 
 OR LOWER(LEFT(P.RSD_ROW,6))='vendor'
OR LOWER(LEFT(P.RSD_ROW,4))=LOWER(LEFT(P.RSD,4))
OR LOWER(LEFT(REGEXP_REPLACE(P.RSD_ROW,'\ ',''),1))= '=' 
OR LOWER(LEFT(P.RSD_ROW,2))='->' 
OR LOWER(LEFT(P.RSD_ROW,1))='>' 
OR REGEXP_LIKE(P.RSD_ROW,'[0-9]{2}\%.*')
)
  AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM meijer_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;


--ADJUSTMENT TYPE: LINE ITEM ORDERING (bjs part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE bjs_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key ='bjs' AND
  s.medium = 'PAPER' AND 
  s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row  
); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN 
( LOWER(LEFT(P.RSD_ROW,4))='ecpn' 
OR LOWER(LEFT(P.RSD_ROW,4))='ccpn' 
 OR LOWER(LEFT(P.RSD_ROW,4))='pcpn'
OR LOWER(LEFT(P.RSD_ROW,7))='scanned')
--PCT >38%
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,

--is_promo_adj
is_promo_adj =  
CASE
WHEN  
( LOWER(LEFT(P.RSD_ROW,4))='ecpn' 
OR LOWER(LEFT(P.RSD_ROW,4))='ccpn' 
 OR LOWER(LEFT(P.RSD_ROW,4))='pcpn'
OR LOWER(LEFT(P.RSD_ROW,7))='scanned')
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE
ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE
WHEN 
( LOWER(LEFT(P.RSD_ROW,4))='ecpn' 
OR LOWER(LEFT(P.RSD_ROW,4))='ccpn' 
 OR LOWER(LEFT(P.RSD_ROW,4))='pcpn'
OR LOWER(LEFT(P.RSD_ROW,7))='scanned')
  AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM bjs_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;


--ADJUSTMENT TYPE: LINE ITEM ORDERING (frys_marketplace part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE frys_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key ='frys_marketplace' AND
  s.medium = 'PAPER' AND 
  s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row
); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN 
( LOWER(LEFT(P.RSD_ROW,4))='ecpn' 
OR LOWER(LEFT(P.RSD_ROW,4))='mega' 
 OR LOWER(LEFT(P.RSD_ROW,4))='save'
OR LOWER(LEFT(P.RSD_ROW,2))='sc')
--PCT >20%
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,

--is_promo_adj
is_promo_adj =  
CASE
WHEN  
( LOWER(LEFT(P.RSD_ROW,4))='ecpn' 
OR LOWER(LEFT(P.RSD_ROW,4))='mega' 
 OR LOWER(LEFT(P.RSD_ROW,4))='save'
OR LOWER(LEFT(P.RSD_ROW,2))='sc')
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE
ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE
WHEN 
( LOWER(LEFT(P.RSD_ROW,4))='ecpn' 
OR LOWER(LEFT(P.RSD_ROW,4))='mega' 
 OR LOWER(LEFT(P.RSD_ROW,4))='save'
OR LOWER(LEFT(P.RSD_ROW,2))='sc')
  AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM frys_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;



--ADJUSTMENT TYPE: LINE ITEM ORDERING (tops part 2) --everything else (mostly in summary)

CREATE OR REPLACE TEMPORARY TABLE tops_promo_line_order AS
(SELECT rec.receipt_id, rec.item_order, rec.rsd, rec.qty*rec.amount AS gross_total, rec.type, rec.qty,
lead(rec.rsd) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS rsd_row,
lead(rec.qty*rec.amount) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS gross_total_lead_row,
lead(rec.type) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS type_row,
lead(rec.qty) over(PARTITION BY rec.receipt_id ORDER BY rec.item_order) AS qty_row
FROM etl_basket_item_qty_promo_rec rec
LEFT JOIN datamaster.datamaster_banner b ON b.KEY = rec.banner
  LEFT JOIN etl_basket_store s ON s.receipt_id = rec.receipt_id
WHERE
  b.key ='tops_friendly_markets' AND
  s.medium = 'PAPER' AND 
  s.detailed_state !='full_complete'
ORDER BY type, type_row, rsd_row
  ); --setting line by line promo match for all full_complete receipts

----UPDATE PROMO REC TABLE WITH NETTED GROSS TOTALS
UPDATE etl_basket_item_qty_promo_rec r
--gross_totals
SET r.gross_total = CASE 
WHEN 
( LOWER(LEFT(P.RSD_ROW,5))='bonus' 
OR LOWER(RIGHT(P.RSD_ROW,5))='svngs' 
 OR LOWER(RIGHT(P.RSD_ROW,7))='savings'
OR LOWER(LEFT(P.RSD_ROW,2))='sc')
--PCT >14%

 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN r.gross_total + p.gross_total_lead_row
ELSE r.gross_total END,

--is_promo_adj
is_promo_adj =  
CASE
WHEN  
( LOWER(LEFT(P.RSD_ROW,5))='bonus' 
OR LOWER(RIGHT(P.RSD_ROW,5))='svngs' 
 OR LOWER(RIGHT(P.RSD_ROW,7))='savings'
OR LOWER(LEFT(P.RSD_ROW,2))='sc')
 AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN TRUE
ELSE FALSE END,
--promo_adj_type
  promo_adj_type = CASE
WHEN 
( LOWER(LEFT(P.RSD_ROW,5))='bonus' 
OR LOWER(RIGHT(P.RSD_ROW,5))='svngs' 
 OR LOWER(RIGHT(P.RSD_ROW,7))='savings'
OR LOWER(LEFT(P.RSD_ROW,2))='sc')
  AND p.type = 'Item'
  AND p.type_row = 'Coupon' AND abs(p.gross_total_lead_row) <= r.gross_total
  THEN 'LINE_ITEM_ORDER' 
ELSE NULL END
FROM tops_promo_line_order p
WHERE p.receipt_id = r.receipt_id
and p.item_order = r.item_order;





 
