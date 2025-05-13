-- Выполнение запроса 1
select id
FROM (
SELECT DISTINCT o.id AS id, o.cadastral_number AS cn, 'asdfg' AS sa , 'dfgbhn' AS fa, 'hybgvf' AS ar FROM objects o
JOIN source_destructing sd ON sd.cadastral_number = o.cadastral_number
UNION
SELECT DISTINCT oa.object_id AS id, '-21345' AS cn, oa.simple_address AS sa, oa.full_address AS fa, oa.address_reference AS ar FROM object_addresses oa
JOIN source_destructing sd ON sd.address IN (oa.simple_address, oa.full_address, oa.address_reference)
) AS b

-- Выполнение запроса 2
select id
FROM (
    SELECT DISTINCT o.id AS id, o.cadastral_number AS cn, 'asdfg' AS sa, 'dfgbhn' AS fa, 'hybgvf' AS ar
    FROM objects o
    JOIN source_undivided su ON su.cadastral_number = o.cadastral_number
    UNION
    SELECT DISTINCT oa.object_id AS id, '-21345' AS cn, oa.simple_address AS sa, oa.full_address AS fa, oa.address_reference AS ar
    FROM object_addresses oa
    JOIN source_undivided su ON su.location IN (oa.simple_address, oa.full_address, oa.address_reference)
) AS b;

-- Выполнение запроса 3
UPDATE source_undivided su
SET object_id = b.id
FROM (
    SELECT DISTINCT o.id AS id, o.cadastral_number AS cn, 'asdfg' AS sa, 'dfgbhn' AS fa, 'hybgvf' AS ar
    FROM objects o
    JOIN source_undivided su ON su.cadastral_number = o.cadastral_number
    UNION
    SELECT DISTINCT oa.object_id AS id, '-21345' AS cn, oa.simple_address AS sa, oa.full_address AS fa, oa.address_reference AS ar
    FROM object_addresses oa
    JOIN source_undivided su ON su.location IN (oa.simple_address, oa.full_address, oa.address_reference)
) AS b
WHERE su.cadastral_number = b.cn OR su.location IN (b.sa, b.fa, b.ar);