ALTER TABLE orders
ALTER COLUMN shopid TYPE VARCHAR;

ALTER TABLE performance
ALTER COLUMN shopid TYPE VARCHAR;

ALTER TABLE orders
ALTER COLUMN buyerid TYPE VARCHAR;

ALTER TABLE orders
ALTER COLUMN orderid TYPE VARCHAR;

ALTER TABLE orders
ALTER COLUMN itemid TYPE int;

ALTER TABLE quiros.public.user
ALTER COLUMN shopid TYPE VARCHAR;

ALTER TABLE quiros.public.user
ALTER COLUMN buyerid TYPE VARCHAR;


-- question 1
with even as (
select u.country  ,count(distinct o.buyerid) "even_buyer"
from orders o 
left join (select distinct buyerid ,country  from "user") u on o.buyerid = u.buyerid 
where o.itemid % 2 = 0 group by u.country),
odd as (select u.country  ,count(distinct o.buyerid) "odd_buyer"
from orders o 
left join (select distinct buyerid ,country  from "user") u on o.buyerid = u.buyerid 
where o.itemid % 2 = 1 group by u.country )
select distinct u2.country, e.even_buyer, od.odd_buyer
from "user" u2 
left join even e on u2.country = e.country
left join odd od on u2.country = od.country;


-- question 2
with g_ord as (select o.shopid ,count(o.orderid) "total_order" 
from orders o group by 1),
g_view as(select p.shopid , sum(p."Item_views") "total_view"
from performance p group by 1),
g_cli_imp as (select p.shopid , sum(p.total_clicks) "total_click",sum(p.impressions) "total_impression"
from performance p group by 1)
select go.shopid, go.total_order, vi.total_view, gci.total_click, gci.total_impression
from g_ord go
left join g_view vi 
	on go.shopid = vi.shopid
left join g_cli_imp gci 
	on go.shopid = gci.shopid;


-- question 3
select o.buyerid ,sum(o.gmv)"total_gmv" from orders o 
inner join (select distinct buyerid , country from "user") u on u.buyerid = o.buyerid 
where u.country = 'ID'
group by 1
order by 2 desc
limit 10
;

-- question 3
select o.buyerid ,sum(o.gmv)"total_gmv" from orders o 
inner join (select distinct buyerid , country from "user") u on u.buyerid = o.buyerid 
where u.country = 'SG'
group by 1
order by 2 desc
limit 10
;




