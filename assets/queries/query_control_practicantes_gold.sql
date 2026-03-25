WITH calculo_tiempo AS (
    SELECT 
        DNI,
        CONDICION,
        "FECHA ING" AS Fecha_Ingreso,
        "F. RENOVACION" AS Fecha_Renovacion,
        FECHA_DOCUMENTO,
        SEDE,
        UNIVERSIDAD,
        "JEFE INMEDIATO",
        GERENCIA,
        CASE 
            WHEN "F. RENOVACION" IS NOT NULL 
            THEN CAST("F. RENOVACION" - "FECHA ING" AS INTEGER)
            ELSE NULL
        END AS dias_servicio,
        -- Ajuste: Si tiene 335 días o más, lo tratamos como año 1 para el reporte visual
        CASE 
            WHEN "F. RENOVACION" IS NOT NULL 
            THEN CAST(FLOOR(("F. RENOVACION" - "FECHA ING") / 365.25) AS INTEGER)
            ELSE NULL
        END AS anios_servicio,
        CASE 
            WHEN "F. RENOVACION" IS NOT NULL 
            THEN CAST(FLOOR((("F. RENOVACION" - "FECHA ING") % 365.25) / 30.44) AS INTEGER)
            ELSE NULL
        END AS meses_servicio
    FROM control_practicantes_silver
),
flags_evaluacion AS (
    SELECT 
        *,
        CASE 
            WHEN dias_servicio IS NULL THEN NULL
            WHEN CONDICION = 'PRACTICANTE PROFESIONAL' 
                AND dias_servicio BETWEEN 335 AND 364 
            THEN 'SI' 
            ELSE 'NO' 
        END AS por_cumplir_1,
        CASE 
            WHEN dias_servicio IS NULL THEN NULL
            WHEN CONDICION = 'PRACTICANTE PROFESIONAL' 
                AND dias_servicio >= 365 
            THEN 'SI' 
            ELSE 'NO' 
        END AS cumplio_1,
        CASE 
            WHEN dias_servicio IS NULL THEN NULL
            WHEN CONDICION = 'PRACTICANTE PROFESIONAL' 
                AND dias_servicio BETWEEN 640 AND 729 
            THEN 'SI' 
            ELSE 'NO' 
        END AS por_cumplir_2
    FROM calculo_tiempo
)

SELECT 
    *,
    -- Lógica de tiempo_servicio ajustada para evitar confusión en renovaciones
    CASE 
        WHEN anios_servicio IS NULL THEN NULL
        -- SI ESTÁ POR CUMPLIR EL AÑO (Día 335-364), mostramos "12 meses (Límite)" o "1 año"
        WHEN por_cumplir_1 = 'SI' THEN '1 año (En límite)'
        WHEN por_cumplir_2 = 'SI' THEN '2 años (Límite Máximo)'
        
        -- Lógica estándar para los demás casos
        WHEN anios_servicio = 0 THEN meses_servicio || ' meses'
        WHEN anios_servicio = 1 AND meses_servicio = 0 THEN '1 año'
        WHEN anios_servicio = 1 THEN '1 año y ' || meses_servicio || ' meses'
        WHEN meses_servicio = 0 THEN anios_servicio || ' años'
        ELSE anios_servicio || ' años y ' || meses_servicio || ' meses'
    END AS tiempo_servicio
FROM flags_evaluacion
ORDER BY Fecha_Ingreso DESC
