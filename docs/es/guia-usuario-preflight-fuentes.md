# Guia de Usuario: Contratos Preflight por Fuente

Esta guia explica que debe cumplir cada archivo fuente antes de ejecutar un ETL en la aplicacion. Si una fuente no cumple estas reglas, el proceso se detiene antes de transformar datos.

## Que valida el sistema en general

Todas las fuentes se validan primero contra un contrato. En terminos practicos, el sistema revisa:

- que el archivo exista
- que sea un Excel permitido: `.xlsx`, `.xlsm` o `.xls`
- que el libro pueda abrirse correctamente
- que existan las hojas esperadas
- que el encabezado empiece en la fila y columna correctas
- que existan las columnas minimas requeridas
- que el nombre del archivo cumpla el patron esperado cuando aplique

## Recomendaciones generales antes de cargar archivos

- No cambies los nombres de las hojas.
- No insertes filas encima del encabezado esperado.
- No renombres columnas obligatorias.
- Evita copiar y pegar tablas con encabezados desplazados.
- Manten el archivo en formato Excel compatible.
- Si el ETL pide una fecha en el nombre del archivo, respeta exactamente ese formato.

## 1. Base de Datos (BD)

Fuente validada por el contrato `bd`.

- Nombre de archivo esperado:
  `BD. DD.MM.YYYY..xlsx`
  Tambien se aceptan `.xlsm` y `.xls`
- Regla importante:
  la fecha del nombre debe existir de verdad. Por ejemplo, `31.02.2026` falla.
- Hoja obligatoria:
  `METSO`
- Encabezado esperado:
  fila `10`, columna `A`
- Columnas minimas esperadas:
  `NUMERO DE DOC`
  `CODIGO SAP2`
  `NOMBRE COMPLETOS`
  `GERENCIA`
  `SEXO`
  `SEDE3`
  `WHITE COLLAR / BLUE COLLAR`
  `Modalidad de Contrato`
  `Fecha de Termino`
  `SERVICIO`
  `REGIMEN DE TRABAJO`
  `FECH_INGR.`
  `FECHA DE NAC.`

## 2. Nomina

Fuente validada por el contrato `nomina`.

- Extensiones permitidas:
  `.xlsx`, `.xlsm`, `.xls`
- Hoja obligatoria:
  `Planilla`
- Encabezado esperado:
  fila `6`, columna `A`
- Columnas minimas esperadas:
  `APELLIDO PATERNO`
  `NOMBRES`
  `TIPO DE DOCUMENTO`
  `DNI/CEX`

### Importante para el ETL de Nomina

Ademas de las planillas de nomina, el proceso valida un archivo adicional de licencias. Si ese archivo no cumple contrato, el ETL tambien falla aunque la planilla de nomina este correcta.

- Archivo esperado por el flujo:
  `licencias/CONTROL DE LICENCIAS.xlsx`

## 3. Licencias

Fuente validada por el contrato `licencias`.

- Extensiones permitidas:
  `.xlsx`, `.xlsm`, `.xls`
- Hojas obligatorias:
  `LICENCIA CON GOCE`
  `LICENCIA SIN GOCE`
- Encabezado esperado en ambas hojas:
  fila `2`, columna `A`

### Hoja `LICENCIA CON GOCE`

Columnas minimas esperadas:

- `PERIODO`
- `DNI/CEX`
- `APELLIDO PATERNO`
- `APELLIDO MATERNO`
- `NOMBRES`
- `MOTIVO DE LIC.CON GOCE`

### Hoja `LICENCIA SIN GOCE`

Columnas minimas esperadas:

- `PERIODO`
- `DNI/CEX`
- `APELLIDO PATERNO`
- `APELLIDO MATERNO`
- `NOMBRES`
- `MOTIVO DE LIC.S.G.H`

## 4. Nomina Regimen Minero

Fuente validada por el contrato `regimen_minero`.

- Extensiones permitidas:
  `.xlsx`, `.xlsm`, `.xls`
- Nombre de archivo esperado:
  debe contener un periodo con formato `YYYY-MM`
  ejemplo: `2026-03 planilla regimen minero.xlsx`
- Hoja obligatoria:
  `Planilla`
- Encabezado esperado:
  fila `6`, columna `A`
- Columna minima obligatoria:
  `DNI/CEX`

## 5. Control de Practicantes

Fuente validada por el contrato `control_practicantes`.

- Nombre de archivo esperado:
  `BD Practicantes DD.MM.YYYY.xlsx`
  Tambien se aceptan `.xlsm` y `.xls`
- Regla importante:
  la fecha del nombre debe ser valida
- Hoja obligatoria:
  `Practicantes`
- Encabezado esperado:
  fila `4`, columna `A`
- Columnas minimas esperadas:
  `N°`
  `DNI`
  `APELLIDOS Y NOMBRES`
  `CONDICION`
  `FECHA ING`
  `F. RENOVACION`
  `SEDE`
  `UNIVERSIDAD`
  `JEFE INMEDIATO`
  `GERENCIA`

## 6. PDT - Relacion de Ingresos

Fuente validada por el contrato `ingresos`.

- Extensiones permitidas:
  `.xlsx`, `.xlsm`, `.xls`
- El ETL espera un solo archivo con dos hojas obligatorias:
  `EMPLEADOS`
  `PRACTICANTES`

### Hoja `EMPLEADOS`

- Encabezado esperado:
  fila `2`, columna `B`
- Columnas minimas esperadas:
  `AÑO`
  `MES`
  `DNI`
  `N° DOCUM.`

### Hoja `PRACTICANTES`

- Encabezado esperado:
  fila `2`, columna `A`
- Columnas minimas esperadas:
  `AÑO`
  `MES`
  `Numero Documento`

## 7. Examen Retiro

Fuente validada por el contrato `examen_retiro`.

- Extensiones permitidas:
  `.xlsx`, `.xlsm`, `.xls`
- Hoja obligatoria:
  `DATA`
- Encabezado esperado:
  fila `3`, columna `A`
- Columnas minimas esperadas:
  `NOMBRE`
  `DNI`
  `FECHA DE CESE`

## Errores frecuentes y como corregirlos

- `Filename does not match regex`:
  revisa el nombre exacto del archivo, incluidos espacios, puntos y fecha.
- `Invalid document date in filename`:
  el nombre tiene una fecha imposible o mal escrita.
- `Missing sheet`:
  falta una hoja obligatoria o fue renombrada.
- `Expected header ... but found empty/shifted`:
  el encabezado empezo mas abajo, mas arriba o en otra columna.
- `Missing required columns`:
  una o varias columnas obligatorias no existen o tienen otro nombre.
- `Unable to open Excel file`:
  el archivo esta dañado, bloqueado o no es un Excel compatible.

## Checklist rapido antes de ejecutar

1. Verifica que estas usando el archivo correcto para el ETL correcto.
2. Confirma que el nombre del archivo respeta el formato requerido si aplica.
3. Abre el Excel y revisa que la hoja obligatoria exista.
4. Revisa que el encabezado empiece en la fila y columna esperadas.
5. Confirma que las columnas obligatorias no fueron renombradas.
6. Si ejecutas Nomina, valida tambien el archivo de licencias.

## Referencia tecnica

Si necesitas revisar la definicion tecnica exacta, los contratos viven en `assets/validate_source/` y la validacion se ejecuta desde [src/utils/validate_source.py](c:/Users/ricuculm/OneDrive%20-%20Metso/Documents/VS%20Code/12.%20Reportabilidad/src/utils/validate_source.py).
