Transformaciones que sí son tu responsabilidad (data engineering)

1. Nivel de procesamiento del producto
Chequeá si estás bajando L1C (top-of-atmosphere) o L2A (bottom-of-atmosphere, corregido atmosféricamente) desde CDSE. Si el modelo necesita reflectancia de superficie y estás bajando L1C, falta una corrección atmosférica (Sen2Cor u otra) antes de Silver — esto es grande, preguntalo en la reunión, no lo asumas.

2. Reproyección a CRS común
Ya lo tenías anotado en tu doc. Sentinel-2 viene en UTM por zona MGRS — si tu área de estudio cruza dos zonas UTM, vas a tener tiles con CRS distintos que necesitás unificar antes del tiling, o el modelo va a ver geometría inconsistente entre tiles vecinos.

3. Máscara de nubes/sombras/nieve — no solo el % agregado
El cloud_cover que filtraste en Bronze es un promedio de toda la escena. Una escena con 15% de nubosidad puede tener un tile específico 100% tapado. Necesitás aplicar la SCL band (Scene Classification Layer, viene en L2A) o una máscara de nubes a nivel de tile, no de escena — y descartar/flagear tiles individuales, no solo escenas completas.

4. Manejo de no-data / bordes
Los bordes de la escena satelital y las zonas de solape entre pasadas del satélite tienen píxeles no-data. Necesitás un criterio de % máximo de no-data por tile (ej. descartar tile si >X% es no-data) — esto es análogo al cloud_cover pero a nivel de tile.

5. Selección y resampling de bandas
Sentinel-2 trae bandas a distintas resoluciones nativas (10m, 20m, 60m). Si el UNet necesita todas las bandas a una resolución uniforme, hay que resamplear (nearest/bilinear/cubic) las de 20m/60m a 10m, o hacer lo inverso. El método de resampling importa para la calidad del dato — pero qué bandas usar y a qué resolución es decisión del equipo de ciencia, no tuya.

6. Normalización/escalado de valores de reflectancia
Sentinel-2 L2A viene con un BOA_QUANTIFICATION_VALUE (típicamente 10000) para convertir DN a reflectancia [0,1]. Confirmá que se está aplicando el offset correcto (Copernicus cambió el esquema de offset en procesamiento post-2022, baseline 04.00+) — si no, tenés corrimiento sistemático en los valores.

7. Tipo de dato y precisión
Decidir si Silver guarda float32 (reflectancia normalizada) o uint16 (DN crudo) — afecta tamaño de storage y si necesitás desnormalizar en tiempo de entrenamiento.

8. Deduplicación de solapes entre tiles/pasadas
Si el área de interés cae en el solape de dos swaths consecutivos del satélite, vas a tener cobertura duplicada. Definir criterio de cuál escena prevalece (¿la de menor cloud cover? ¿la más reciente?) si el modelo no debe ver el mismo terreno dos veces con distinta fecha en el mismo batch.

Lo que tenés que preguntar en la reunión (no lo decidas vos)
Tamaño de tile y overlap exacto que espera el input del UNet (ya está en tu lista de preguntas del doc, confirmalo).
% máximo de no-data y de nubosidad tolerado por tile — necesitás valores numéricos, no "poca nubosidad".
Compositing temporal: ¿el modelo consume una imagen por tile o un composite multi-fecha (ej. mediana de 3 meses para eliminar nubes residuales)? Esto cambia radicalmente el diseño de Silver.
Bandas + índices espectrales requeridos por el modelo — esto es científico, no lo definas vos, pero necesitás la lista concreta para saber qué columnas van en el schema de Silver.
Split train/val/test: ¿se define en Silver (flag por tile) o después, en el pipeline de entrenamiento? Si es en Silver, necesitás criterio (¿aleatorio? ¿por tile geográfico para evitar leakage espacial, que es el error más común en estos datasets?).

El punto más importante de esta lista para la reunión: el split train/val/test por tile geográfico — si no separás por ubicación espacial (y dejás que tiles vecinos/solapados caigan en train y val), vas a tener data leakage severo y métricas de validación infladas. Es un error clásico en segmentación de imágenes satelitales y vale la pena mencionarlo proactivamente aunque no te lo pregunten, porque si el equipo de ciencia no lo tiene resuelto, es la clase de cosa que "se subestima" (como dice tu propio doc) y que después es carísimo de corregir con el modelo ya entrenado sobre datos contaminados.
