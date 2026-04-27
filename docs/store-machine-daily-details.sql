create table if not exists public.store_machine_daily_details (
    store_id uuid not null references public.stores (id) on delete cascade,
    machine_name text not null,
    target_date date not null,
    slot_count integer not null,
    average_difference numeric,
    average_games numeric,
    average_payout numeric,
    records_by_slot jsonb not null,
    updated_at timestamptz not null default now(),
    primary key (store_id, machine_name, target_date)
);

create index if not exists store_machine_daily_details_store_machine_date_idx
    on public.store_machine_daily_details (store_id, machine_name, target_date desc);

with normalized as (
    select
        store_id,
        machine_name,
        target_date::date as target_date,
        slot_number,
        difference_value,
        games_count,
        payout_rate,
        bb_count,
        rb_count,
        combined_ratio_text,
        bb_ratio_text,
        rb_ratio_text
    from public.machine_daily_results
    where machine_name is not null
      and machine_name <> ''
      and target_date is not null
      and slot_number is not null
      and slot_number <> ''
),
grouped as (
    select
        store_id,
        machine_name,
        target_date,
        count(*) as slot_count,
        avg(difference_value) as average_difference,
        avg(games_count) as average_games,
        avg(payout_rate) as average_payout,
        jsonb_object_agg(
            slot_number,
            jsonb_strip_nulls(
                jsonb_build_object(
                    'difference_value', difference_value,
                    'games_count', games_count,
                    'payout_rate', payout_rate,
                    'bb_count', bb_count,
                    'rb_count', rb_count,
                    'combined_ratio_text', combined_ratio_text,
                    'bb_ratio_text', bb_ratio_text,
                    'rb_ratio_text', rb_ratio_text
                )
            )
        ) as records_by_slot
    from normalized
    group by store_id, machine_name, target_date
)
insert into public.store_machine_daily_details (
    store_id,
    machine_name,
    target_date,
    slot_count,
    average_difference,
    average_games,
    average_payout,
    records_by_slot,
    updated_at
)
select
    store_id,
    machine_name,
    target_date,
    slot_count,
    average_difference,
    average_games,
    average_payout,
    records_by_slot,
    now() as updated_at
from grouped
on conflict (store_id, machine_name, target_date) do update
set
    slot_count = excluded.slot_count,
    average_difference = excluded.average_difference,
    average_games = excluded.average_games,
    average_payout = excluded.average_payout,
    records_by_slot = excluded.records_by_slot,
    updated_at = excluded.updated_at;
