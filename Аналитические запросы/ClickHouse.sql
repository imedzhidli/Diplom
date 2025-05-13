-- Выполнение запроса 1
SELECT id
FROM (
    SELECT DISTINCT o.id AS id, o.cadastral_number AS cn, 'asdfg' AS sa, 'dfgbhn' AS fa, 'hybgvf' AS ar
    FROM objects o
    JOIN source_destructing sd ON sd.cadastral_number = o.cadastral_number
    UNION ALL
    SELECT DISTINCT oa.object_id AS id, '-21345' AS cn, oa.simple_address AS sa, oa.full_address AS fa, oa.address_reference AS ar
    FROM object_addresses oa
    JOIN source_destructing sd ON has([oa.simple_address, oa.full_address, oa.address_reference], sd.address)
) AS b
GROUP BY id;

-- Выполнение запроса 2
SELECT id
FROM (
    SELECT DISTINCT o.id AS id, o.cadastral_number AS cn, 'asdfg' AS sa, 'dfgbhn' AS fa, 'hybgvf' AS ar
    FROM objects o
    JOIN source_undivided su ON su.cadastral_number = o.cadastral_number
    UNION ALL
    SELECT DISTINCT oa.object_id AS id, '-21345' AS cn, oa.simple_address AS sa, oa.full_address AS fa, oa.address_reference AS ar
    FROM object_addresses oa
    JOIN source_undivided su ON has([oa.simple_address, oa.full_address, oa.address_reference], su.location)
) AS b
GROUP BY id;

-- Выполнение запроса 3
DROP TABLE IF EXISTS temp_undivided
CREATE TABLE temp_undivided
ENGINE = MergeTree()
ORDER BY assumeNotNull(id) AS
SELECT
    su.id as id,
    su.problem_id,
    su.active,
    su.global_id,
    su.cadastral_number,
    su.`location`,
    su.field_area,
    su.onwer_type,
    su.rental_state,
    su.permitted_use,
    su.geodata,
    su.geocenter,
    assumeNotNull(t.id) as object_id
FROM (
    SELECT DISTINCT 
        o.id AS id, 
        o.cadastral_number AS cn, 
        'asdfg' AS sa, 
        'dfgbhn' AS fa, 
        'hybgvf' AS ar
    FROM objects o
    JOIN source_undivided su ON su.cadastral_number = o.cadastral_number
    UNION ALL
    SELECT DISTINCT 
        oa.object_id AS id, 
        '-21345' AS cn, 
        oa.simple_address AS sa, 
        oa.full_address AS fa, 
        oa.address_reference AS ar
    FROM object_addresses oa
    JOIN source_undivided su ON has([oa.simple_address, oa.full_address, oa.address_reference], su.location)
) AS t
RIGHT JOIN source_undivided su on su.cadastral_number = t.cn or su.location = t.sa or su.location = t.fa or su.location = t.ar
DROP TABLE source_undivided;
RENAME TABLE temp_undivided TO "source_undivided"