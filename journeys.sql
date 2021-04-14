-- Order the hits in sequence and relabel when it makes sense
with sequence as (
SELECT
    visitid as visit,
    case
      when hit_eventinfo_eventcategory='navigation' and hit_eventinfo_eventaction='navigated to' then 'navigation||' || hit_eventinfo_eventlabel
      when hit_eventinfo_eventcategory='media' and hit_eventinfo_eventaction IN ('played video','saw video') then 'media||watched video'
      when hit_eventinfo_eventcategory='betting' and hit_eventinfo_eventaction IN ('submitted bet','placed_bet') then hit_eventinfo_eventcategory || '||placed bet'
      when hit_eventinfo_eventcategory='betting' then hit_eventinfo_eventcategory || '||' || hit_eventinfo_eventaction
      when hit_eventinfo_eventcategory='payments' and hit_eventinfo_eventaction like '%deposit%' then 'payments||' || hit_eventinfo_eventaction
      else hit_eventinfo_eventcategory || '||' || hit_eventinfo_eventcategory
    end as event,
    ROW_NUMBER() OVER (PARTITION BY visitid ORDER BY hit_timestamp) as seq
  FROM omni_ga.vw_site_visit_hit_bi
  WHERE (brand ILIKE 'PP') AND (hit_timestamp >= (TIMESTAMP '2021-04-05 10:00:00') AND hit_timestamp < (TIMESTAMP '2021-04-05 11:00:00'))
    AND hit_eventinfo_eventcategory IS NOT NULL AND hit_eventinfo_eventcategory NOT IN ('interface')
    -- AND visitid = 1617534021
)
-- Group the events that occur next to each other in the sequence, but NOT the same events that occur later on
,grouped as (
  select visit, event, min(seq) as seq, count(grp) as num_occurances_together
  from (
    select t.*,
            (select count(*)
             from sequence as t2
             where t2.seq < t.seq and t2.event <> t.event and t2.visit=t.visit
            ) as grp
        from sequence as t
    )
  group by visit, event, grp
  order by 3
),
-- Transpose into a table showing the previous 3 events that happened upto the target event
journey as (
  SELECT
  visit,
  -- LAG(event,5) OVER (PARTITION BY visit ORDER BY seq) as event_minus_5,
  -- LAG(event,4) OVER (PARTITION BY visit ORDER BY seq) as event_minus_4,
  LAG(event,3) OVER (PARTITION BY visit ORDER BY seq) as event_minus_3,
  LAG(event,2) OVER (PARTITION BY visit ORDER BY seq) as event_minus_2,
  LAG(event,1) OVER (PARTITION BY visit ORDER BY seq) as event_minus_1,
  event as target_event
  from grouped
)
-- Group the journeys together to show the most popular
SELECT /*event_minus_5, event_minus_4,*/ event_minus_3, event_minus_2, event_minus_1, target_event, count(1) as occurances
from journey
-- Example usage to look for events that led up to a bet being placed
-- where target_event='betting||placed bet'
group by 1,2,3,4/*,5,6*/
having occurances > 1000
order by occurances desc