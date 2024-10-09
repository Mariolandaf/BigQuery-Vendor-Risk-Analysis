# Código para traducir y normalizar una lista de nombres de proveedores.
# Tarda entre 2min y 5min para 2500 celdas (nombres de proveedores). 
# De ser necesario, ejecutar por lotes de no más de 2500 celdas para evitar fallas en APIs.

import pandas as pd
import re
import time
from langdetect import detect, LangDetectException
from deep_translator import GoogleTranslator 
from joblib import Parallel, delayed  # Para paralelización
import unicodedata  # Para normalizar Unicode
import numpy as np 

# Ruta del archivo CSV
file_path = r"C:\Users\e-malandaf\Downloads\tachados.csv"

# Leer el archivo CSV
df = pd.read_csv(file_path)

# Función para normalizar caracteres Unicode (acentos y diacríticos)
def normalize_unicode(name):
    name = unicodedata.normalize('NFKD', name)
    return ''.join([c for c in name if not unicodedata.combining(c)])

# Función para limpiar los nombres de proveedores
def clean_name(name):
    if pd.isnull(name):
        return ''  # Cambiar None a cadena vacía para evitar problemas
    name = re.sub(r'[\"\'\(\),\*\$%\+=#;:@]', ' ', name)  # Convertir comillas, paréntesis, comas, *, $, %, +, =, #, ; y @ en espacios
    name = re.sub(r'\s\.\s', ' ', name) #patrones de puntos (.) que estén rodeados por espacios en blanco y reemplazarlos por un espacio
    name = re.sub(r'\s+-\s+|\s+-|-\s+', ' ', name)  # Reemplazar guiones que tengan espacios a un lado o ambos por un espacio
    name = re.sub(r'^-+\s*|\s*-+$', ' ', name)  # Reemplazar guiones al principio o al final de la expresión por un espacio
    name = re.sub(r'\s[^a-zA-Z0-9\u00C0-\u017F\u0400-\u04FF\u0600-\u06FF\u4E00-\u9FFF]\s*$', '', name)  # Eliminar solo si hay un espacio antes del carácter no alfanumérico al final
    name = re.sub(r'^[^a-zA-Z0-9\u00C0-\u017F\u0400-\u04FF\u0600-\u06FF\u4E00-\u9FFF]\s+', '', name) #eliminar caracteres al principio que están antes de un espacio
    name = re.sub(r'\s+', ' ', name).strip()  # Reemplazar múltiples espacios por uno solo y eliminar espacios extra
    name = re.sub(r'\.{2,}', '.', name) #reemplazar puntos múltiples por uno solo
    name = name.lower()  # Convertir a minúsculas
    name = normalize_unicode(name)  # Normalizar caracteres Unicode
    return name

# Función para eliminar abreviaciones comunes
def remove_abbreviations(name):
    if not name:
        return ''
    
    abbreviations = [
        'inc', 'inc.', 'incorporated', 's.a.u.', 's.a.u', 'corp', 'corp.', 'corporation', 'co', 'co.', 'company',
        'ltd', 'ltd.', 'limited', 'llc', 'l.l.c.', 'l c', 'plc', 'plc.', 'gmbh', 's.a', 's.a.', 
        's.a. de c.v.', 's.a de c.v.', 's a', 's a.', 's a de c v', 'sa de cv', 's de rl', 's de r l', 
        'bv', 'b.v.', 'nv', 'n.v.', 'ag', 'a.g.', 'kg', 'oy', 'kft', 'as', 'a.s.', 'ab', 'a.b.',
        'oü', 'srl', 's.r.l', 'z.o.o', 'z.o.o.', 'k/s', 'snc', 's.n.c.', 
        'scs', 's.c.s.', 'eurl', 'e.u.r.l.', 'sarl', 's.a.r.l.', 'sl', 's.l.', 'pty ltd', 
        'pty ltd.', 'spa', 's.p.a.', 'sa', 'sa.', 'd.o.o.', 'd.o.o',
        'sp.', 'llp', 'l.l.p.', 'lp', 'l.p.', 'g.m.b.h.', 'corporación',
        's.p.r.l.', 'c.b.', 'gesellschaft mit beschränkter haftung', 'anónima', 
        'sociedad anónima', 'sociedad limitada', 'compagnie', 'compañía', 'k.s.', 
        'responsabilidad limitada', 'public limited company', 'c.v.', 'kommanditgesellschaft', 
        'v.o.f.', 'bvba', 'nvsa', 'o.o.o.', 'llp.', 'lllp', 'l.l.l.p.', 'b.v.i.', 
        't.o.v.', 'e.e.', 'yhtiö', 'f.lli', 'societá', 'di', 'spa.', 'sas', 
        'sas.', 'se', 's.e.', 'jsc', 'j.s.c.', 'ao', 'a.o.', 'zao', 'z.a.o.', 's.l', 'z o.o.', 'd.o',
        'sc de rl de cv', 'de cv', 'de rl de cv', 's.r.o.', 's.a. de c.v', 'cv', 's.a.s.', 'doo', 'sp',
        'sp. z o.o.', 'sf', 'de c.v', 'for sf', 'd. o. o.', 's. a.', 's r.o.', 's.a.c.', 'sa de dv', 's r l',
        's.g', 's.a.b de c.v', 's r. o.', 's.l.u', 's.l.u.', 's.ade c.v.', 's a s', 'pte ltd', 'incorp.', 'saic',
        's.a.i.c.', 'gmbh & co', 'gmbh & co kg', 'ltda', 'e.k', 'ek', 'kgaa', 'k.g.a.a.', 'nuf', 'a.p.s.', 'oyj', 'oyj.', 
        'kda', 'kk', 'saog', 'sociedad anonima abierta', 'saop', 'sociedad anonima ordinaria', 
        'prc', 'comm.v.', 'cvba', 'eeig', 'sasac', 'kc', 'kab', 'responsabilidad anónima', 'gesmbh', 'ltda.', 's.c.a.',
        'sez', 'oop', 'vzw', 'ewiv', 'kt', 'lp.', 'dsp.', 'a.d.', 'y.k.', 'yo.k.', 'partnership limited by shares',
        'jtd', 'c-corp', 'zrt', 'z.r.t', 'co.l.l.c', 'sa decv', 'c. x a.', 'j.d.o.o.', 'spol.', 'spol.s r.o.', 's.c.',
        'spol .', 'spol', 's', 'z o. o.', 'l t d a', 'd.d.', 'd. d.', 'בעמ', 'on p', 'at p', 'in p', 'de r.l. de c.v',
        'de r.l.', 's de rl de cv', 'e.k.', 'k.', 'z.s.', 's. r. l.', 's.a.e', 's.a.e.', 'z o o', 's. l.', 's a p i', 
        'sa d e cv', 's.n.c', 's.a.p.i.de c.v.', 's.r.o', 's.a.p.i. of c.v.', 's.l.l.', 's.l.l', 'sp.k.', 'f.z.e',
        'f.z.e.', 'y asoc.', 'e.v.', 'e.v', 'v.o.s', 'v.o.s.', 'sp.o.o.', 'c. por a.', 'c por a', 'a.s',
        'a. s.', 'sp.j.', 'sp. j.', 'sp.z.o.o', 's.k.a', 's.k.a.', 'ac', 'ja', 'y co', '& co.kg', 'co.kg', 's.r.o.spol.',
        's. r. o.', 's.a.p.i. de c.v.', 'spol.s', 's.c.p', 's.c.p.', 's.', 'z o.', 'e. v.', 'e. v', 'e v.',
        'a.c.', 'z.', 'e.s.p', 'e.s.p.', 'sab de cv', 's.a.b. de c.v.', 'a c', 's.a.l', 's.a.l.',
        'e.i.r.l', 'e.i.r.l.', 's.de.r.l.de.c.v.', 'e i r l', 'm.b.h', 'm.b.h.', 'm b h', 's.k.', 'sk', 's.k',
        'r.l', 'r.l.', 'rl', 'u.a', 'u.a.', 'ua', 's. a. de c. v.', 's a c', '& co.k', 'z o.o.s.k.a.', 's.l.p',
        'k.d.', 'kd', 'k.d', 's.a.a.', 'limited.', 's.a de c', 's p a', 's. k. a.', 'inh.', 'sa de ccv', 'sacv',
        'de r.l. de c.v.', 's.l.p.', 'r. o.', 's .r. o.', '&co.kg', 'gmbh&co.kg', 'de c.v.', 'de rl', 'de c', 'cz',
        '.gmbh', 'sp.z.o.o.', 'de c.a. s.', 's a c', 'e. k.', 'sa de', 's.p.', 'z o.o.sp.k.', 's. de r.l. de c.v.',
        's. de r.l.', 's a p i de cv', 'de cv', 'o.o.', 's. c.', 'sas de cv', 'z. o. o.', 'sp.z o. o.', 'sa d cv',
        's.l.p.u', 's.c.v.', 'b.a.', 'b.a', 'dba'
    ]

    # Ordenar abreviaciones de mayor a menor longitud
    abbreviations = sorted(abbreviations, key=len, reverse=True)

    # Crear patrón de regex para buscar las abreviaciones, permitiendo espacios y puntos opcionales
    pattern = r'(^|\s)(' + '|'.join(
        re.escape(abbr).replace(r'\ ', r'\s*').replace(r'\.', r'\.*') for abbr in abbreviations
    ) + r')(\s|$)'

    # Aplicar la eliminación de abreviaciones tres veces para asegurar que se eliminen correctamente
    for _ in range(3):
        name = re.sub(pattern, ' ', name).strip()

    return name

# Diccionario para almacenar traducciones previamente hechas (caché)
translation_cache = {}

# Función para traducir solo si es necesario, ajustando la abreviación del hebreo y manejando errores
def translate_if_needed(name):
    if name in translation_cache:
        return translation_cache[name]
    
    try:
        # Intentar detectar el idioma con langdetect
        detected_lang = detect(name)
        
        # Corregir la abreviación de hebreo de 'he' a 'iw' para GoogleTranslator
        if detected_lang == 'he':
            detected_lang = 'iw'
    except LangDetectException:
        # Si falla la detección, usar 'auto' para que GoogleTranslator detecte el idioma
        print(f"Language detection failed for '{name}'. Using GoogleTranslator auto-detect.")
        detected_lang = 'auto'
    
    try:
        if detected_lang != 'en':  # Solo traducir si el idioma no es inglés
            translator = GoogleTranslator(source=detected_lang, target='en')
            translation = translator.translate(name)
            if translation:
                translation_cache[name] = (translation.lower(), detected_lang)
                return translation.lower(), detected_lang  # Retornar la traducción y el idioma detectado
            else:
                raise ValueError("Invalid translation response received")
        else:
            translation_cache[name] = (name, detected_lang)
            return name, detected_lang  # Si el idioma es inglés, no traducir
    except Exception as e:
        print(f"Error translating '{name}': {e}")
        return name, 'error'  # Marcar el idioma como 'error' en caso de falla general


# Función para procesar los nombres de forma paralela
def process_names(df_chunk):
    # Limpieza inicial de los nombres
    df_chunk['VendorName_Clean'] = df_chunk['VendorName'].apply(lambda x: clean_name(x) if pd.notnull(x) else '')
    
    # Eliminar abreviaciones comunes
    df_chunk['VendorName_NoAbbr'] = df_chunk['VendorName_Clean'].apply(remove_abbreviations)
    
    # Limpieza adicional después de remover abreviaciones
    df_chunk['VendorName_NoAbbr'] = df_chunk['VendorName_NoAbbr'].apply(clean_name)
    
    # Aplicar traducción y detectar el idioma sobre el texto limpio
    df_chunk[['VendorName_Translated', 'langdetect']] = df_chunk['VendorName_Clean'].apply(
        lambda x: pd.Series(translate_if_needed(x))
    )
    
    # Remover abreviaciones de la versión traducida y limpiarla
    df_chunk['VendorName_Translated_NoAbbr'] = df_chunk['VendorName_Translated'].apply(remove_abbreviations)
    df_chunk['VendorName_Translated_NoAbbr'] = df_chunk['VendorName_Translated_NoAbbr'].apply(clean_name)
    
    return df_chunk


# Función para dividir el dataframe y aplicar el procesamiento en paralelo
def process_in_parallel(df, n_jobs=8):
    df_split = np.array_split(df, n_jobs)
    results = Parallel(n_jobs=n_jobs)(delayed(process_names)(chunk) for chunk in df_split)
    return pd.concat(results)

# Verificar el número de filas antes de procesar
initial_rows = len(df)

# Aplicar el procesamiento a los datos en paralelo
df_processed = process_in_parallel(df, n_jobs=8)

# Eliminar filas con valores nulos o vacíos en 'VendorName_Clean'
df_processed = df_processed[df_processed['VendorName_Clean'] != '']

# Verificar el número de filas después del procesamiento
final_rows = len(df_processed)

# Guardar el resultado en un nuevo archivo CSV
output_file_path = r"C:\Users\e-malandaf\Downloads\cleanedVendors.csv"
df_processed.to_csv(output_file_path, index=False, encoding='utf-8-sig')

# Imprimir la diferencia en el número de filas
print(f"Filas iniciales: {initial_rows}")
print(f"Filas resultantes: {final_rows}")
print(f"Limpieza completada. Archivo guardado en: {output_file_path}")

