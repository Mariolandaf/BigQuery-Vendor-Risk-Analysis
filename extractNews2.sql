DECLARE filas_faltantes INT64;

-- 1. Crear la tabla de registro si no existe
-- 2. Crear la tabla de resultados acumulativos si no existe

-- 3. Inicializar el contador de filas faltantes
SET filas_faltantes = (SELECT COUNT(*) FROM `vendornews.news.tachados_escapados` WHERE ID NOT IN (SELECT ID FROM `vendornews.news.seleccionadas_log`));

-- 4. Bucle iterativo para seleccionar lotes de 100 filas
WHILE filas_faltantes > 0 DO

  -- Seleccionar 100 filas aleatorias que no hayan sido seleccionadas
  CREATE OR REPLACE TABLE `vendornews.news.nueva_tabla_sample` AS
  WITH nuevas_filas AS (
      SELECT *
      FROM `vendornews.news.tachados_escapados`
      WHERE ID NOT IN (SELECT ID FROM `vendornews.news.seleccionadas_log`)
      LIMIT 100
  )
  SELECT * FROM nuevas_filas;

  -- Insertar los resultados de la búsqueda en la tabla de acumulación `pruebanews`
  INSERT INTO `vendornews.news.news` (VendorNames, DATE, DocumentIdentifier, Persons, Organizations, V2Tone_First)
  SELECT
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

  -- Insertar las nuevas filas en la tabla de registro
  INSERT INTO `vendornews.news.seleccionadas_log` (ID)
  SELECT ID
  FROM `vendornews.news.nueva_tabla_sample`;

  -- Actualizar el contador de filas faltantes
  SET filas_faltantes = (SELECT COUNT(*) FROM `vendornews.news.tachados_escapados` WHERE ID NOT IN (SELECT ID FROM `vendornews.news.seleccionadas_log`));

END WHILE;
