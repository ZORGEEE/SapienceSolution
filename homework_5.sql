create or replace function std12_50.f_full_load(
        p_table_from text,
        p_table_to text,
        p_truncate bool
    )
    returns int8
    language plpgsql
    security definer
    volatile
as $$
    declare v_script text;
    v_count int8;
begin
        if coalesce(p_truncate, false) is true then perform f_truncate(p_table_to);
        end if;
        execute 'INSERT INTO ' || p_table_to || ' SELECT * FROM ' || p_table_from;
        get diagnostics v_count = ROW_COUNT;
        raise notice '% rows was inserted', v_count;
        return v_count;
end;
$$

execute on any;


-- function for truncation
create or replace function std12_50.f_truncate(p_table text)
    returns void
    language plpgsql
    security definer
    volatile
as $$
    begin execute 'TRUNCATE TABLE ' || p_table;
end;
$$
execute on any;

--function for download partitions

create or replace function std12_50.f_delta_partition(
        p_schema text,
        p_table_from text,
        p_table_to text,
        p_partition_key text,
        p_partition_start timestamp,
        p_partition_end timestamp
    )
    returns void
    language plpgsql
    security definer
    volatile
as $$
    declare
        v_end_date date;
        v_interval interval;
        v_partition_start_date date;
        v_partition_end_date date;
        v_tmp_table_name text;

        v_where text;
        v_sql text;
    begin
        perform std12_50.f_create_partition(p_schema, p_table_to, p_partition_end);

        -- Заменяем партиции
        v_interval = '1 month'::interval;
        v_end_date = DATE_TRUNC('month', p_partition_end::date) + v_interval;
        v_partition_start_date = DATE_TRUNC('month', p_partition_start::date);
        LOOP
            v_partition_end_date = v_partition_start_date + v_interval;
            EXIT WHEN v_partition_end_date = v_end_date;

            -- Создаем временную таблицу
            v_tmp_table_name = f_create_tmp(p_table_to, 'partition_'||to_char(v_partition_start_date, 'YYYYMMDD'));
            RAISE NOTICE 'tmp table %', v_tmp_table_name;

            -- Заполняем временную таблицу данными
            v_where = p_partition_key ||' >= '''||v_partition_start_date|| ''' and ' ||p_partition_key ||' < '''||v_partition_end_date||'''';
            v_sql = 'INSERT INTO '||v_tmp_table_name|| ' SELECT * FROM '||p_table_from||' WHERE '||v_where;
            RAISE NOTICE 'sql %', v_sql;
            EXECUTE v_sql;

            -- Подменяем партицию на таблицу
            v_sql = 'ALTER TABLE '||p_table_to|| ' EXCHANGE PARTITION FOR (DATE '''||v_partition_start_date||''') WITH TABLE '||v_tmp_table_name|| ' WITH VALIDATION';
            RAISE NOTICE 'sql %', v_sql;
            EXECUTE v_sql;

            --Удаляем врменную таблицу
            v_sql = 'DROP TABLE ' ||v_tmp_table_name;
            EXECUTE v_sql;

            v_partition_start_date = v_partition_end_date;
        END LOOP;
    end;
    $$
execute on any;

--function for creating partition
CREATE OR REPLACE FUNCTION std12_50.f_create_partition(p_schema text, p_table text, p_end_date timestamp)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

DECLARE
	v_end_date timestamp;

    v_partition_text text;
	v_partition_date timestamp;
	v_partition_end timestamp;

    v_partitions_count int;

	v_sql text;

BEGIN

	v_end_date = p_end_date::timestamp;

	--Проверяем наличие хотя бы одной партиции
	SELECT Count(*) INTO v_partitions_count
	                FROM pg_partitions p
	                WHERE p.schemaname = p_schema
	                  AND p.tablename = p_table
	                  AND partitionisdefault != TRUE;

	RAISE NOTICE 'log %', v_partitions_count;

	IF v_partitions_count >= 1 THEN
		LOOP
		    -- Смотрим последнюю партицию
			RAISE NOTICE 'check last partition';

			SELECT partitionrangeend INTO v_partition_text
			FROM (SELECT p.*, RANK() OVER (ORDER BY partitionrank DESC) rnk
				      FROM pg_partitions p
				      WHERE p.schemaname = p_schema
				        AND p.tablename = p_table
				        AND p.partitionisdefault != TRUE
				) q
			WHERE rnk = 1;

			EXECUTE 'SELECT '||v_partition_text INTO v_partition_date;

			-- Выходим если создали все партиции
			RAISE NOTICE 'last %', v_partition_date;
			RAISE NOTICE 'end %', v_end_date;
			EXIT WHEN v_partition_date > v_end_date;

			-- Добавляем новую партицию
			v_partition_end = v_partition_date + '1 month'::interval;
			RAISE NOTICE 'New partition start %', to_char(v_partition_date,'YYYY-MM-DD');
			RAISE NOTICE 'New partition end %', to_char(v_partition_end,'YYYY-MM-DD');
			v_sql = 'ALTER TABLE '|| p_table ||' SPLIT DEFAULT PARTITION
	  					START ( ''' || to_char(v_partition_date,'YYYY-MM-DD') || ''') END (''' || to_char(v_partition_end,'YYYY-MM-DD') ||''')';
			EXECUTE v_sql;
		END LOOP;
	END IF;
END;

$$
EXECUTE ON ANY;

--function for temporary table
create or replace function std12_50.f_create_tmp(
    p_table_to text,
    p_prefix_name text
)
returns text
language plpgsql
security definer
volatile
as $$
    declare
        v_text text[];
        v_schema_name text;
        v_table_name text;
        v_tmp_table_name text;
        v_sql text;
    begin
        v_tmp_table_name = p_table_to || '_tmp_'||p_prefix_name;
        v_sql = 'CREATE TABLE '||v_tmp_table_name||' ( LIKE '||p_table_to||' INCLUDING ALL )';
        RAISE NOTICE 'sql %', v_sql;
        EXECUTE v_sql;
        return v_tmp_table_name;
    end;
    $$
execute on any;

--function for data mart
CREATE OR REPLACE FUNCTION std12_50.f_calculate_data_mart(p_year text, p_month text)
	RETURNS void
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

    DECLARE
        v_table_name text;
        v_start_date date;
        v_end_date date;
        v_row_cnt int8;

        v_fact_sql text;
        v_plan_sql text;
        v_plan_fact_sql text;
        v_top_material_in_region_sql text;
        v_cte text;
        v_sql text;
    BEGIN
        v_start_date = to_date(p_year||p_month||'01', 'YYYYMMDD');
        v_end_date = to_date(p_year||p_month||'01', 'YYYYMMDD') + '1 month'::interval;
        v_table_name = 'plan_fact_'|| to_char(v_start_date, 'YYYYMM');

        RAISE NOTICE 'Mart name %',v_table_name;
        RAISE NOTICE 'Start %', v_start_date;
        RAISE NOTICE 'End %', v_end_date;

        -- Удаляем старую витрину
        EXECUTE 'drop table if exists '||v_table_name;

        --Создаем витрину
        EXECUTE 'create table '|| v_table_name ||'(
        region varchar(20),
        matdirec varchar(20),
        distr_chan varchar(100),
        plan_quantity int4,
        fact_quantity int4,
        percent decimal,
        material varchar(20)
        )
        distributed by (region)';


        RAISE NOTICE 'Table created %', v_table_name;

        --Вставляем данные в витрину
        v_fact_sql = 'sale as (
            select *
            from sales s
            where s."date" >= '''||v_start_date||''' and s."date" < '''||v_end_date||''')';

        v_plan_sql = 'pl as (
            select *
            from plan p
            where p."date" >= '''||v_start_date||''' and p."date" < '''||v_end_date||''')';

        v_plan_fact_sql = 'plan_fact as (
            select
            case when s.region is null then plan.region else s.region end,
            case when s.distr_chan is null then plan.distr_chan else s.distr_chan end,
            case when p.matdirec is null then plan.matdirec else p.matdirec end,
            s.quantity as fact,
            plan.quantity as plan
            from sale s
            left join product p on s.material = p.material
            full join pl as plan on plan.region = s.region and plan.distr_chan = s.distr_chan and plan.matdirec = p.matdirec)';

        v_top_material_in_region_sql = 'top_material as (
            select region, material
            from(
            select
            region,
            material,
            SUM(quantity) as sale_count,
            RANK() over (partition by region order by SUM(quantity) DESC) as rank
            from sale
            group by region, material) as top
            where rank = 1)';

        v_cte = 'with '||v_fact_sql|| ',' ||v_plan_sql|| ',' ||v_plan_fact_sql|| ','||v_top_material_in_region_sql;

        v_sql = 'select f.region, matdirec, distr_chan, SUM(plan) as plan, SUM(fact) as fact, SUM(fact)::decimal/SUM(plan)*100 as percent, MAX(material) as top_material
            from plan_fact as f
            left join top_material as t on t.region = f.region
            group by f.region, matdirec, distr_chan';

        v_row_cnt = f_insert_data(v_table_name, v_cte||v_sql, NULL);

        EXECUTE 'ANALYZE '||v_table_name;

        -- Создаем представление
        EXECUTE 'create or replace view v_' || v_table_name ||' as
        select pf.region , rs.txt as region_txt, pf.matdirec, pf.distr_chan, cs.txtsh as distr_chan_txt, pf."percent", pf.material, ps.brand, ps.txt as material_txt, ps2.price
        from plan_fact_202101 pf
        left join region rs on pf.region = rs.region
        left join chanel cs  on cs.distr_chan = pf.distr_chan
        left join product ps on ps.material = pf.material
        left join price ps2 on ps2.region = pf.region and ps2.distr_chan = pf.distr_chan and ps2.material = pf.material';

    END;
$$
EXECUTE ON ANY;

--function for data insert
CREATE OR REPLACE FUNCTION std12_50.f_insert_data(p_table_to text, p_select text, p_truncate bool)
	RETURNS int4
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
AS $$

DECLARE
	v_sql text;
	v_row_cnt int;
BEGIN
    -- очищаем таблицу
	IF COALESCE(p_truncate, FALSE) IS TRUE THEN
		PERFORM f_truncate_table(p_table_to);
	END IF;

	-- загружаем данные в таблицу
	v_sql = 'INSERT INTO ' || p_table_to || ' '||p_select;
	EXECUTE v_sql;

	GET DIAGNOSTICS v_row_cnt = ROW_COUNT;
	RAISE NOTICE 'Inserted %', v_row_cnt;
	RETURN v_row_cnt;
END;
$$
EXECUTE ON ANY;


select std12_50.f_full_load('std12_50.chanel_ext', 'chanel', True);
select * from chanel limit 10;

select std12_50.f_full_load('std12_50.price_ext', 'price', True);
select * from price limit 10;

select std12_50.f_full_load('std12_50.product_ext', 'product', True);
select * from product limit 10;

select std12_50.f_full_load('std12_50.region_ext', 'region', True);
select * from region limit 10;

select std12_50.f_delta_partition('std12_50', 'std12_50.plan_ext', 'plan', 'date', '2021-01-01', '2021-12-31');
select * from plan limit 10;

select std12_50.f_delta_partition('std12_50', 'std12_50.sales_ext', 'sales', 'date', '2021-01-01', '2021-12-31');
select * from sales limit 10;

drop function std12_50.f_delta_partition(text, text, text, text, timestamp, timestamp);

drop external table sales_ext;

drop function std12_50.f_calculate_data_mart(text, text);

select std12_50.f_calculate_data_mart('2021', '06');
select * from plan_fact_202106 limit 10;