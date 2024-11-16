DECLARE filas_faltantes INT64;

-- 1. Vaciar tablas `vendornews.news.seleccionadas_log` y `vendornews.news.nueva_tabla_sample`
TRUNCATE TABLE `vendornews.news.seleccionadas_log`;
TRUNCATE TABLE `vendornews.news.nueva_tabla_sample`;

-- 2. Vaciar tabla `vendornews.news.news` (ADVERTENCIA: considera guardar los datos de la tabla antes de borrarlos)
TRUNCATE TABLE `vendornews.news.news`;

-- 3. Inicializar el contador de filas faltantes
SET filas_faltantes = (
  SELECT COUNT(*) 
  FROM `vendornews.news.vendors_prueba_dataset` 
  WHERE ID NOT IN (SELECT ID FROM `vendornews.news.seleccionadas_log`)
);

-- 4. Bucle iterativo para seleccionar lotes de 100 filas
WHILE filas_faltantes > 0 DO

  -- Seleccionar 100 filas aleatorias que no hayan sido seleccionadas y aplicar las transformaciones necesarias
  CREATE OR REPLACE TABLE `vendornews.news.nueva_tabla_sample` AS
  WITH ProveedoresNormalizados AS (
    SELECT 
      ID AS ID,
      Vendor AS vendor_id,
      SID AS SID,
      COALESCE(VendorName, '') AS nombre_proveedor_original,
      COALESCE(VendorName_Clean, '') AS nombre_proveedor_clean,
      COALESCE(VendorName_NoAbbr, '') AS nombre_proveedor_no_abbr,
      COALESCE(VendorName_Translated, '') AS nombre_proveedor_translated,
      COALESCE(VendorName_Translated_NoAbbr, '') AS nombre_proveedor_translated_no_abbr
    FROM `vendornews.news.vendors_prueba_dataset`
    WHERE ID NOT IN (SELECT ID FROM `vendornews.news.seleccionadas_log`)
    LIMIT 100
  ),
  ProveedoresEscapados AS (
    SELECT
      ID AS ID,
      vendor_id AS vendor_id,
      SID AS SID,
      ARRAY(
        SELECT DISTINCT REGEXP_REPLACE(nombre, r'([.^$|?*+(){}\[\]\\])', r'\\\1')
        FROM UNNEST([
          nombre_proveedor_original, 
          nombre_proveedor_clean, 
          nombre_proveedor_no_abbr, 
          nombre_proveedor_translated, 
          nombre_proveedor_translated_no_abbr
        ]) AS nombre
        WHERE nombre != ''
      ) AS nombres_proveedor_unicos
    FROM ProveedoresNormalizados
  )
  SELECT * FROM ProveedoresEscapados;

  -- Insertar los resultados de la búsqueda en la tabla de acumulación `news`
  INSERT INTO `vendornews.news.news` (vendor_id, sid, VendorNames, DATE, DocumentIdentifier, Persons, Organizations, V2Tone_First)
  SELECT
    p.vendor_id,
    p.sid,
    p.nombres_proveedor_unicos AS VendorNames,
    g.DATE,
    g.DocumentIdentifier,
    g.Persons,
    g.Organizations,
    g.V2Tone_First
  FROM `vendornews.news.gkg_clustered` g
  CROSS JOIN `vendornews.news.nueva_tabla_sample` p
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(p.nombres_proveedor_unicos) AS nombre_unico
    WHERE LOWER(CONCAT(';', g.Persons, ';', g.Organizations, ';')) LIKE CONCAT('%;', LOWER(nombre_unico), ';%')
  );

  -- Añadir columnas de título y lenguaje utilizando la tabla `gqg`
  CREATE OR REPLACE TABLE `vendornews.news.news` AS
  SELECT 
    n.vendor_id,
    n.sid,
    n.VendorNames,
    n.DATE,
    n.DocumentIdentifier,
    n.Persons,
    n.Organizations,
    n.V2Tone_First,
    COALESCE(g.title, '') AS title,
    COALESCE(g.lang, '') AS lang 
  FROM `vendornews.news.news` n
  LEFT JOIN `gdelt-bq.gdeltv2.gqg` g
  ON g.url = n.DocumentIdentifier
  WHERE TIMESTAMP_TRUNC(g.date, DAY) >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)) OR g.url IS NULL;


  -- Insertar las nuevas filas en la tabla de registro
  INSERT INTO `vendornews.news.seleccionadas_log` (ID)
  SELECT ID
  FROM `vendornews.news.nueva_tabla_sample`;

  -- Actualizar el contador de filas faltantes
  SET filas_faltantes = (
    SELECT COUNT(*)
    FROM `vendornews.news.vendors_prueba_dataset`
    WHERE ID NOT IN (SELECT ID FROM `vendornews.news.seleccionadas_log`)
  );

END WHILE;

-- SELECT 
--   `vendor_id` AS `vendor_id`,
--   `sid` AS `sid`,
--   `DATE` AS `DATE`, 
--   `DocumentIdentifier` AS `DocumentIdentifier`,
--   `title` AS `title`,
--   `lang` AS `lang`, 
--   `Persons` AS `Persons`, 
--   `V2Tone_First` AS `V2Tone_First`, 
--   ARRAY_TO_STRING(`VendorNames`, ', ') AS `VendorNames`
-- FROM (
--   SELECT * 
--   FROM `vendornews.news.news`
-- )

