CREATE OR REPLACE TABLE `vendornews.news.tachados_escapados` AS
WITH ProveedoresNormalizados AS (
  SELECT 
    ID AS ID,
    COALESCE(VendorName, '') AS nombre_proveedor_original,
    COALESCE(VendorName_Clean, '') AS nombre_proveedor_clean,
    COALESCE(VendorName_NoAbbr, '') AS nombre_proveedor_no_abbr,
    COALESCE(VendorName_Translated, '') AS nombre_proveedor_translated,
    COALESCE(VendorName_Translated_NoAbbr, '') AS nombre_proveedor_translated_no_abbr
  FROM 
    `vendornews.news.tachados`
),
ProveedoresEscapados AS (
  SELECT
    ID AS ID,
    ARRAY(
      SELECT DISTINCT REGEXP_REPLACE(nombre, r'([.^$|?*+(){}\[\]\\])', r'\\\1')
      FROM UNNEST([nombre_proveedor_original, nombre_proveedor_clean, nombre_proveedor_no_abbr, nombre_proveedor_translated, nombre_proveedor_translated_no_abbr]) AS nombre
    ) AS nombres_proveedor_unicos
  FROM 
    ProveedoresNormalizados
)
SELECT *
FROM ProveedoresEscapados;

