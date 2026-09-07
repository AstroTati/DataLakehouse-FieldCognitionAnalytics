### Transformaciones

1. **Nivel de procesamiento**: si el modelo necesita reflectancia de superficie, asegurarse que tenemos la capa L1C (estoy bajando L2A), y hacer la corrección atmosférica (Sen2Cor u otra).

2. **Reproyección** a common reporting standard.

3. **Máscara de nubes/sombras/nieve**: las bandas indican el % de toda la escena (e.g., una escena con 15% de nubosidad puede tener un tile 100% tapado). Aplicar la capa SCL para flaggear (a nivel tile, no solo escenas completas).

4. **Manejo de no-data/bordes**: los bordes de la escena satelital y las zonas de overlap entre pasadas del satélite tienen píxeles no-data. Que % de tiles no-data usamos de tolerancia? (i.e., descartar tile si >X% es no-data.

5. **Selección y resampling de bandas**: Sentinel-2 tiene bandas de tres resoluciones (10m, 20m, 60m). Si el UNet necesita una resolución uniforme, hay que resamplear (nearest/bilinear/cubic) las de 10/20m a 60m.

6. **Normalización**: chequear que se está aplicando el offset correcto (Copernicus cambió el esquema de offset en procesamiento en 2022, baseline 04.00+) para evitar corrimiento sistemático en los valores.

7. **Tipo de dato y precisión**: float32? float64?.

8. **Deduplicación**: si el área de interés cae en el overlap entre dos swaths consecutivos del satélite, tendremos cobertura duplicada. Que criterio de escena prevalece (menor cloud cover? más reciente?) _si_ el modelo no debe ver el mismo terreno dos veces con distinta fecha en el mismo batch.



### Otras preguntas
* Tamaño de tile y overlap que espera el input del UNet. --> Esto creo que no lo sabemos todavia, elegir una (logica) para empezar.
* Compositing temporal: el modelo consumira una imagen por tile o un composite multi-fecha (e.g., media de 3 meses para eliminar nubes residuales)? 
* Bandas + índices espectrales requeridos por el modelo --> Tampoco lo sabemos aun creo, pero lo necesito para saber qué columnas van en la capa silver.
* Split train/val/test: se define en la capa silver (flag por tile) o después (en el pipeline de entrenamiento)? Si es la silver, cual seria el criterio? Las opciones son aleatorio o por tile geográfico. Si no separamos por tile geografico (y dejamos que tiles vecinos/solapados caigan en train y val), vamos a tener data leakage severo y métricas de validación infladas. Aparentemente es un error clásico en segmentación de imágenes satelitales.
