create table if not exists public.store_machine_summaries (
    store_id uuid not null references public.stores (id) on delete cascade,
    machine_name text not null,
    latest_date date not null,
    slot_count integer not null,
    average_difference numeric,
    average_games numeric,
    average_payout numeric,
    updated_at timestamptz not null default now(),
    primary key (store_id, machine_name)
);

create index if not exists store_machine_summaries_store_latest_idx
    on public.store_machine_summaries (store_id, latest_date desc, machine_name asc);

with latest_dates as (
    select
        store_id,
        machine_name,
        max(target_date::date) as latest_date
    from public.machine_daily_results
    where machine_name is not null
      and machine_name <> ''
      and target_date is not null
    group by store_id, machine_name
),
latest_rows as (
    select
        results.store_id,
        results.machine_name,
        latest_dates.latest_date,
        results.slot_number,
        results.difference_value,
        results.games_count,
        results.payout_rate
    from public.machine_daily_results as results
    inner join latest_dates
        on latest_dates.store_id = results.store_id
       and latest_dates.machine_name = results.machine_name
       and latest_dates.latest_date = results.target_date::date
)
insert into public.store_machine_summaries (
    store_id,
    machine_name,
    latest_date,
    slot_count,
    average_difference,
    average_games,
    average_payout,
    updated_at
)
select
    store_id,
    machine_name,
    latest_date,
    count(distinct slot_number) as slot_count,
    avg(difference_value) as average_difference,
    avg(games_count) as average_games,
    avg(payout_rate) as average_payout,
    now() as updated_at
from latest_rows
group by store_id, machine_name, latest_date
on conflict (store_id, machine_name) do update
set
    latest_date = excluded.latest_date,
    slot_count = excluded.slot_count,
    average_difference = excluded.average_difference,
    average_games = excluded.average_games,
    average_payout = excluded.average_payout,
    updated_at = excluded.updated_at;
