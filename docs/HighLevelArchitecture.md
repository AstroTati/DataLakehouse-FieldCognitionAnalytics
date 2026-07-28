# Documento de Arquitectura Técnica: Sistema de IA Geoeléctrica — Pipeline de Datos Satelitales
Versión 1.0 | Proyecto: Segmentación de Zonas de Aridez con UNet

## 1. Visión General del Proyecto

Este proyecto construye una arquitectura de datos de extremo a extremo para entrenar una red neuronal convolucional de tipo UNet capaz de segmentar zonas de aridez a partir de imágenes satelitales multiespectrales. La fuente primaria de datos es Sentinel-2, el satélite de observación terrestre de la Agencia Espacial Europea (ESA), que proporciona imágenes con resolución de 10 a 60 metros por píxel y revisita cada 5 días.

El contexto geofísico del proyecto son los Sondeos Eléctricos Verticales (SEV), una técnica de prospección geoeléctrica que mide la resistividad del subsuelo. La hipótesis central es que las características espectrales visibles desde el espacio (humedad superficial, cobertura vegetal, exposición de minerales áridos) correlacionan con las propiedades eléctricas del subsuelo medidas en campo con los SEV.

El pipeline completo sigue el patrón de Medallion Architecture sobre Azure, con tres capas de datos progresivamente más refinados: Bronze (datos crudos), Silver (datos preprocesados y estructurados) y Gold (features listos para el modelo).

---

## 2. Stack Tecnológico y Justificación

### 2.1 Python (Extracción y Orquestación Local)

Python es el lenguaje central por su ecosistema geoespacial maduro. Las librerías clave son:

**Rasterio** es la librería estándar de la industria para leer y escribir archivos GeoTIFF. Está construida sobre GDAL (Geospatial Data Abstraction Library) y expone una API Pythónica para manipular datos ráster. Su importancia radica en que puede leer Cloud-Optimized GeoTIFFs (COG) directamente desde URLs remotas sin descargar el archivo completo, lo que es crítico cuando se trabaja con imágenes de varios gigabytes.

**GeoPandas** extiende pandas con soporte nativo para geometrías vectoriales. Se utiliza para manejar los polígonos de las zonas de interés (áreas de SEV), reproyectarlos entre sistemas de referencia de coordenadas (CRS) y calcular intersecciones espaciales con las escenas satelitales disponibles.

**Shapely** proporciona el motor de geometría computacional subyacente. Se usa para operaciones como unión de polígonos, cálculo de intersección entre el área de interés y la huella de cada escena satelital, y conversión entre formatos WKT (Well-Known Text) y GeoJSON.

**Pydantic** con pydantic-settings proporciona validación de esquemas en tiempo de carga. Toda la configuración del sistema (credenciales, parámetros de búsqueda, fechas) se valida mediante modelos Pydantic, lo que convierte errores de configuración en excepciones descriptivas antes de que el pipeline comience a ejecutarse.

**Tenacity** implementa la política de reintentos con backoff exponencial para todas las llamadas a APIs externas y a Azure, lo que hace el pipeline resiliente a fallos transitorios de red.

**Loguru** reemplaza el módulo logging estándar de Python con una API más ergonómica que soporta rotación automática de archivos, compresión y niveles de log por módulo.

### 2.2 Azure Data Lake Storage Gen2 (ADLS Gen2)

ADLS Gen2 es el sistema de almacenamiento central del pipeline. Se diferencia de Azure Blob Storage estándar en que implementa un sistema de archivos jerárquico verdadero (HDFS-compatible) mediante el protocolo DFS (Data Lake Storage), lo que permite operaciones atómicas de renombrado de directorios y listados de árbol eficientes, funcionalidades críticas para Delta Lake.

La autenticación a ADLS Gen2 sigue una cadena de credenciales con prioridad explícita:
1. **Managed Identity** — cuando el código corre dentro de un recurso Azure (Databricks, ADF, Azure Functions), la identidad del recurso se usa directamente sin credenciales en texto claro.
2. **Service Principal con Client Secret** — para desarrollo local, se crea un Service Principal en Azure Active Directory con el rol mínimo necesario (Storage Blob Data Contributor) y sus credenciales se cargan desde un archivo .env que nunca se sube a control de versiones.
3. **DefaultAzureCredential** — como fallback, incluye el login de Azure CLI, credenciales de Visual Studio Code y otras fuentes del entorno de desarrollo.

Este patrón de cadena de credenciales garantiza que el mismo código funcione sin modificaciones tanto en desarrollo local como en producción sobre Azure.

### 2.3 Azure Databricks con PySpark

Databricks es la plataforma de procesamiento distribuido. Se elige sobre Azure HDInsight o Azure Synapse Analytics porque provee un entorno optimizado para Apache Spark con librerías preinstaladas para machine learning (MLlib, MLflow) y procesamiento de datos geoespaciales.

PySpark permite procesar los GeoTIFFs en paralelo distribuyendo el trabajo entre los nodos del clúster. Un GeoTIFF de una escena Sentinel-2 completa puede ocupar entre 500 MB y 2 GB, y el pipeline necesita procesar cientos de escenas. Con un clúster de, por ejemplo, 8 nodos de 32 GB de RAM cada uno, PySpark puede distribuir la carga de tiling (fragmentación en parches) de forma que el proceso que tomaría horas en una sola máquina se complete en minutos.

### 2.4 Delta Lake

Delta Lake es una capa de almacenamiento transaccional sobre Parquet. Se utiliza en la capa Silver para garantizar:
- **Transacciones ACID**: si un job de Databricks falla a mitad del proceso de tiling, los datos escritos hasta ese momento se revierten automáticamente, evitando estados corruptos.
- **Schema Evolution**: el esquema de los parches puede evolucionar entre versiones sin romper lecturas anteriores.
- **Time Travel**: se puede acceder a versiones anteriores del dataset de parches, lo que es fundamental para reproducibilidad de experimentos de ML.
- **Optimización Z-Order**: los datos se pueden ordenar físicamente por columnas frecuentemente consultadas (tile_id, fecha, calidad) para acelerar las lecturas.

### 2.5 MLflow

MLflow se integra en el notebook de Databricks para registrar experimentos. En este contexto, un "experimento" de MLflow no registra un modelo de ML sino las características del dataset generado: número de parches, distribución de fechas, estadísticas de bandas espectrales, versión de Delta Lake utilizada. Esto crea un rastro de linaje de datos que permite reproducir exactamente el conjunto de datos que se usó para entrenar cualquier versión del modelo UNet.

### 2.6 Azure Data Factory (ADF)

ADF es el orquestador de pipelines de datos. Se eligió sobre alternativas como Apache Airflow o Prefect porque se integra nativamente con todos los servicios Azure utilizados (ADLS Gen2, Databricks, Key Vault) sin necesidad de infraestructura adicional. Permite definir pipelines como JSON, lo que facilita el control de versiones de la lógica de orquestación.

---

## 3. Arquitectura de Datos: Medallion Architecture

### 3.1 Capa Bronze — Datos Crudos

La capa Bronze contiene los datos en su formato más fiel al origen. Para este proyecto, son los archivos .SAFE de Sentinel-2 empaquetados como .zip. Cada paquete .SAFE contiene las 13 bandas espectrales de una escena, los metadatos XML del satélite, los ángulos de iluminación solar y la máscara de clasificación de escena (SCL).

La estructura de directorios en ADLS Gen2 Bronze es:
```
bronze/raw/sentinel2/{tile_id}/{año_mes_día}/{nombre_producto}/
    {nombre_producto}.zip     ← Archivo .SAFE comprimido
    metadata.json             ← Metadatos enriquecidos en JSON
```

El campo tile_id corresponde al grid MGRS (Military Grid Reference System) de Sentinel-2. Cada tile cubre aproximadamente 110×110 km². Para el Desierto de Atacama, el tile relevante es T19HBU.

### 3.2 Capa Silver — Datos Preprocesados

La capa Silver contiene parches (tiles) individuales de 256×256 píxeles extraídos de las escenas Bronze. Cada parche se almacena como un array NumPy serializado en formato Parquet dentro de una tabla Delta Lake. Las columnas de la tabla incluyen:
- El array de píxeles con las bandas seleccionadas (B04, B08, B11, B12, SCL)
- Coordenadas geográficas del parche (bounding box en WGS84)
- Metadatos de calidad (porcentaje de nubosidad del parche, porcentaje de datos válidos)
- Referencias al archivo Bronze de origen (tile_id, fecha, product_name)

Se descartan automáticamente los parches donde más del 20% de los píxeles son inválidos (nubes, sombras de nubes, agua, nieve según la capa SCL).

### 3.3 Bandas Espectrales para Análisis de Aridez

Las bandas de Sentinel-2 seleccionadas para este proyecto son:
- **Banda B04 (Rojo, 665nm)**: Reflectancia del suelo desnudo. Los suelos áridos tienen alta reflectancia en esta banda.
- **Banda B08 (Infrarrojo Cercano, 842nm)**: Usada para calcular el NDVI (Normalized Difference Vegetation Index). La aridez se manifiesta como valores bajos de NDVI.
- **Banda B11 (SWIR1, 1610nm)**: Sensible a la humedad del suelo y la vegetación. Los suelos áridos muestran alta reflectancia.
- **Banda B12 (SWIR2, 2190nm)**: Sensible a minerales específicos de ambientes áridos como óxidos de hierro y carbonatos.
- **Capa SCL (Scene Classification Layer)**: Máscara que identifica nubes, sombras, nieve, agua y tipos de suelo. Esencial para el control de calidad.

---

## 4. Hito 1 — Extracción Satelital (Python)

### 4.1 Objetivo

Descargar imágenes Sentinel-2 desde la API de Copernicus Data Space Ecosystem (CDSE) y almacenarlas en la capa Bronze de ADLS Gen2. El proceso debe ser idempotente (puede re-ejecutarse sin duplicar datos) y resiliente a fallos de red.

### 4.2 API de Copernicus Data Space Ecosystem

El CDSE es el nuevo portal de la ESA que reemplaza al antiguo Copernicus Open Hub (SciHub, cerrado en 2023). Ofrece:
- **Acceso gratuito**: 10 TB/mes de cuota de descarga sin costo
- **OData API**: protocolo estándar para consultas de catálogo con filtros complejos
- **STAC API**: interfaz moderna para consultas geoespaciales
- **OAuth2**: autenticación moderna con tokens de acceso renovables

La consulta OData permite filtrar simultáneamente por:
- Colección (SENTINEL-2)
- Nivel de procesamiento (S2MSIL2A para reflectancia superficial)
- Rango de fechas (ContentDate/Start)
- Porcentaje máximo de nubosidad (Attribute cloudCover)
- Intersección geoespacial con el polígono del área de interés

### 4.3 Flujo del Script de Extracción

1. **Carga y validación de configuración** — Pydantic carga y valida todas las variables de entorno al iniciar. Si falta alguna credencial o el rango de fechas es inválido, el script falla inmediatamente con un mensaje descriptivo.

2. **Carga del polígono del área de interés** — El polígono GeoJSON que define la zona de SEV se carga desde un archivo o se pasa como argumento de línea de comandos. Se convierte a WKT para incluirlo en la consulta OData.

3. **Búsqueda paginada en el catálogo** — La API OData retorna resultados en páginas de máximo 100 elementos. El extractor itera sobre todas las páginas hasta alcanzar el máximo configurado de resultados.

4. **Filtrado de calidad** — Además del filtro de nubosidad aplicado en la consulta OData, se aplica un segundo filtro local que verifica el ratio de intersección espacial entre la escena y el área de interés, descartando escenas que cubran menos del umbral configurado del AOI.

5. **Descarga streaming** — Cada escena se descarga con streaming HTTP en chunks de 1MB para evitar cargar archivos de varios gigabytes en memoria RAM. Se muestra una barra de progreso por escena.

6. **Verificación de integridad** — Antes de subir a ADLS, se calcula el hash MD5 del archivo descargado y se adjunta como metadato del objeto en ADLS.

7. **Subida a ADLS Bronze** — El archivo comprimido y un archivo metadata.json con información de la escena se suben al contenedor Bronze con reintentos automáticos en caso de fallo.

8. **Idempotencia** — Antes de iniciar la descarga, el script consulta si el archivo ya existe en ADLS. Si existe, la escena se omite sin error. Esto permite re-ejecutar el pipeline sin duplicar datos ni ancho de banda.

### 4.4 Estructura del Módulo de Configuración

La configuración se divide en tres clases Pydantic independientes con responsabilidades separadas:

**AzureSettings** contiene todas las credenciales y parámetros de conexión a Azure: tenant ID, client ID, client secret, nombre de la cuenta ADLS, contenedor Bronze y ruta base. Incluye un validador que advierte si no se detectan credenciales de ningún tipo.

**CopernicusSettings** contiene las credenciales OAuth2 o usuario/contraseña de Copernicus, el porcentaje máximo de nubosidad, el nivel de procesamiento de Sentinel-2 y el rango de fechas. Incluye validadores que verifican que el rango de fechas sea coherente y que las credenciales estén completas.

**AppSettings** contiene configuración general de la aplicación: nivel de logging y directorio de logs.

Las tres clases usan `lru_cache` para implementar el patrón singleton, garantizando que las variables de entorno se lean una sola vez aunque se instancien múltiples veces en el código.

### 4.5 Cliente ADLS Gen2

El cliente ADLS implementa el patrón Context Manager (with statement) para garantizar que la conexión se cierre correctamente incluso si ocurre una excepción. Sus responsabilidades son:

- Construir la cadena de credenciales Azure en el orden correcto de prioridad
- Verificar y crear el contenedor Bronze si no existe (operación idempotente)
- Crear la estructura de directorios jerárquica en ADLS antes de subir archivos
- Adjuntar metadatos a cada objeto subido (fuente, MD5, timestamp de ingesta)
- Implementar reintentos con backoff exponencial usando Tenacity
- Exponer un método `file_exists()` para la verificación de idempotencia

---

## 5. Hito 2 — Preprocesamiento Distribuido (PySpark en Databricks)

### 5.1 Objetivo

Transformar los archivos .SAFE crudos de la capa Bronze en parches de 256×256 píxeles estructurados y listos para el entrenamiento del modelo UNet, almacenados en la capa Silver como una tabla Delta Lake.

### 5.2 Por qué 256×256 píxeles

El tamaño de 256×256 píxeles es estándar en arquitecturas UNet para segmentación semántica por varias razones:
- Es una potencia de 2, compatible con las operaciones de pooling y upsampling de la UNet.
- Con bandas de Sentinel-2 a 10m de resolución, un parche de 256×256 cubre 2.56×2.56 km, suficiente para capturar estructuras geológicas relevantes.
- Cabe cómodamente en la memoria de una GPU moderna (NVIDIA A100 con 40GB puede procesar batches de ~64 parches simultáneamente).

### 5.3 Estrategia de Tiling Distribuido

El tiling distribuido funciona así en PySpark:

Primero, se crea un RDD (Resilient Distributed Dataset) donde cada elemento es la ruta ADLS de un archivo .SAFE en la capa Bronze. Spark distribuye estos elementos entre los nodos del clúster.

En cada nodo, un ejecutor Spark usa rasterio para leer el GeoTIFF correspondiente a las bandas seleccionadas. Rasterio soporta lectura de archivos directamente desde ADLS mediante el protocolo ABFS (Azure Blob File System), configurado con las credenciales de Managed Identity del clúster Databricks.

Cada ejecutor divide la escena en una cuadrícula de parches de 256×256. Para escenas de 10980×10980 píxeles (tamaño estándar de un tile Sentinel-2 a 10m), esto genera aproximadamente 1849 parches. Los parches que tienen más del 20% de píxeles inválidos según la capa SCL se descartan.

Los parches válidos se convierten a arrays NumPy, se normalizan al rango [0, 1] dividiendo por el valor máximo de reflectancia (10000 para productos MSIL2A), y se serializan como filas de un DataFrame Spark. El DataFrame se escribe en la tabla Delta Lake Silver con particionamiento por tile_id y año-mes.

### 5.4 Control de Calidad de Parches

La capa SCL de Sentinel-2 asigna a cada píxel una de las siguientes clases:
- 0: Sin datos
- 1: Píxeles saturados o defectuosos
- 2: Sombras oscuras de áreas
- 3: Sombras de nubes
- 4: Vegetación
- 5: Suelo desnudo
- 6: Agua
- 7: Nubes de baja probabilidad
- 8: Nubes de media probabilidad
- 9: Nubes de alta probabilidad
- 10: Cirros
- 11: Nieve/hielo

Para análisis de aridez, los parches útiles son aquellos donde la mayoría de píxeles pertenecen a las clases 4, 5 (vegetación y suelo, los que queremos segmentar). Se descarta un parche si más del 20% de sus píxeles son de las clases 0, 1, 2, 3, 8, 9, 10, 11 (datos inválidos o nubes).

---

## 6. Hito 3 — Trazabilidad con MLflow

### 6.1 Objetivo

Registrar en MLflow las características del dataset generado en cada ejecución del pipeline de preprocesamiento, creando un linaje de datos que vincula cada versión del modelo UNet con el dataset exacto con el que fue entrenado.

### 6.2 Qué se Registra en MLflow

Cada ejecución del notebook de preprocesamiento registra un experimento MLflow con los siguientes datos:

**Parámetros** (inputs del proceso):
- Rango de fechas de las imágenes procesadas
- Porcentaje máximo de nubosidad permitido
- Tamaño del parche en píxeles (256)
- Umbral de descarte por calidad SCL (20%)
- Versión del script de preprocesamiento (hash de commit Git)

**Métricas** (outputs del proceso):
- Número total de escenas Bronze procesadas
- Número total de parches generados
- Número de parches descartados por calidad
- Porcentaje de cobertura temporal (qué meses están representados)
- Estadísticas de las bandas: media, desviación estándar por banda

**Artefactos** (archivos adjuntos):
- Histograma de distribución de nubosidad
- Mapa de cobertura espacial (qué tiles tienen parches)
- Número de versión de la tabla Delta Lake Silver (time travel reference)

### 6.3 Vinculación con el Entrenamiento del Modelo

Cuando se entrena el modelo UNet, se registra en MLflow el run_id del experimento de preprocesamiento que generó el dataset. Esto crea un grafo de linaje que permite responder preguntas como: ¿con qué imágenes satelitales, de qué fechas y con qué criterios de calidad se entrenó el modelo que está en producción actualmente?

---

## 7. Hito 4 — Orquestación con Azure Data Factory

### 7.1 Objetivo

Automatizar el pipeline completo (Hitos 1 y 2) como un flujo de trabajo reproducible en Azure, ejecutable bajo demanda o en horario programado.

### 7.2 Estructura del Pipeline ADF

El pipeline de ADF tiene la siguiente secuencia de actividades:

**Actividad 1 — Validación de Parámetros**: Una actividad de tipo "Web" que llama a un Azure Function que valida que el polígono del área de interés y el rango de fechas proporcionados son coherentes antes de iniciar el pipeline costoso.

**Actividad 2 — Extracción (Script Python)**: Una actividad de tipo "Azure Databricks" que ejecuta el script de extracción del Hito 1 como un job de Python en Databricks. Los parámetros (polígono, fechas, máximo de nubosidad) se pasan como argumentos de línea de comandos. Las credenciales de Copernicus y ADLS se recuperan de Azure Key Vault mediante linked services de ADF.

**Actividad 3 — Preprocesamiento (Notebook Databricks)**: Si la actividad anterior tiene éxito, se dispara una actividad de tipo "Notebook" que ejecuta el notebook de PySpark del Hito 2 en el mismo clúster Databricks. El notebook recibe como parámetro la lista de archivos Bronze recién descargados para procesar solo los nuevos.

**Actividad 4 — Registro MLflow**: Al completar el preprocesamiento, una actividad final ejecuta el código de registro MLflow del Hito 3, guardando las métricas del dataset generado.

### 7.3 Manejo de Errores en ADF

ADF implementa lógica de reintento a nivel de actividad (3 reintentos con 2 minutos entre intentos). Si el pipeline falla después de los reintentos, envía una notificación al equipo vía Azure Monitor Alert y un webhook a Microsoft Teams.

Los pipelines de ADF se definen en JSON y se versionan en el mismo repositorio Git que el código Python y los notebooks de Databricks, siguiendo un flujo GitOps donde los cambios se despliegan mediante Azure DevOps Pipelines.

---

## 8. Consideraciones de Seguridad

Todas las credenciales del sistema (Service Principal, credenciales de Copernicus) se almacenan en Azure Key Vault, nunca en texto claro en el código o en variables de entorno de los pipelines ADF. Los recursos Azure (ADLS Gen2, Databricks, Key Vault) se comunican entre sí mediante Managed Identities y Private Endpoints dentro de una Virtual Network privada, sin exponer tráfico a Internet público.

El acceso a ADLS Gen2 sigue el principio de mínimo privilegio: el Service Principal de extracción solo tiene rol de escritura en el contenedor Bronze. El clúster Databricks solo tiene rol de lectura en Bronze y escritura en Silver.

---

## 9. Glosario Técnico

**Sentinel-2**: Familia de satélites de observación terrestre de la ESA. Proporciona imágenes multiespectrales con 13 bandas, resolución de 10 a 60 metros y período de revisita de 5 días en la misma ubicación.

**GeoTIFF**: Formato de imagen ráster que incluye metadatos georreferenciados (CRS, transformación afín, bounding box). Estándar de facto para datos satelitales.

**Cloud-Optimized GeoTIFF (COG)**: Variante del GeoTIFF donde los datos se ordenan internamente para permitir lecturas parciales (solo el tile de interés) sin descargar el archivo completo.

**ADLS Gen2**: Azure Data Lake Storage Generation 2. Sistema de almacenamiento de objetos de Azure compatible con HDFS, con soporte para namespaces jerárquicos y control de acceso a nivel de directorio.

**Delta Lake**: Capa de almacenamiento transaccional open source sobre Parquet. Añade transacciones ACID, schema evolution y time travel a los data lakes.

**NDVI**: Normalized Difference Vegetation Index. Índice calculado como (B08 - B04) / (B08 + B04). Valores cercanos a 1 indican vegetación densa; cercanos a 0 o negativos indican suelo desnudo o árido.

**Medallion Architecture**: Patrón de organización de data lakes en tres capas: Bronze (datos crudos), Silver (datos limpios y estructurados) y Gold (features de negocio listos para consumo).

**Tiling / Patchification**: Proceso de dividir una imagen grande en parches más pequeños de tamaño fijo (256×256 px) para que puedan ser procesados por una GPU.

**UNet**: Arquitectura de red neuronal convolucional diseñada para segmentación semántica de imágenes. Consta de un encoder que extrae features y un decoder que reconstruye la máscara de segmentación a la resolución original. Fue propuesta originalmente para segmentación de imágenes biomédicas en 2015.

**SEV (Sondeo Eléctrico Vertical)**: Técnica geofísica que mide la resistividad eléctrica del subsuelo en profundidad mediante el envío de corriente eléctrica entre electrodos en superficie.

**OData**: Open Data Protocol. Estándar OASIS para APIs RESTful con capacidades de consulta declarativa (filtros, ordenamiento, paginación) sobre colecciones de datos.

**Managed Identity**: Mecanismo de Azure que asigna una identidad de Azure Active Directory a un recurso (VM, Databricks, ADF) sin necesidad de almacenar credenciales. La autenticación es gestionada automáticamente por la plataforma.

**Service Principal**: Identidad de aplicación en Azure Active Directory equivalente a un usuario de servicio. Se usa en desarrollo local para autenticarse con los mismos roles que tendría una Managed Identity en producción.

**MLflow**: Plataforma open source para gestión del ciclo de vida de experimentos de Machine Learning. Permite registrar parámetros, métricas y artefactos de cada experimento para reproducibilidad.
