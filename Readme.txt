					
PROYECTO FINAL CIENCIA DE DATOS

ANALISIS DE LA EMPRESA OLIST

Desarrollamos este proyecto de análisis como grupo de 5 integrantes, Proyecto final de Henry. 

Utilizamos como conjunto de datos los provistos por la empresa, con datos entre 2016 y 2018. Con información sobre vendedores, clientes, productos, ventas, pagos y datos de geolocalización. 

En primer lugar se realizo un EDA exploratorio, observando en general buena calidad de datos. 

Definimos los objetivos de nuestro proyecto y el alcance que le daríamos, manteniéndonos dentro de la operación de la empresa en Brasil.

Luego comenzamos con el montado de la base de datos en la nube, eligiendo AWS como stack tecnológico. Realizamos la carga de datos sobre S3, procesamiento (ETL) con Glue mediante Jobs automatizados, y de la misma manera, la posterior carga de los datos ya transformados a Redshift. Logrando así un Datawarehouse relacional de tipo PostreSQL. 

Desarrollamos modelos de Machine Learning de manera local en python con Scikit learn y otras librerías Open Source, para luego montar en AWS SageMaker, un instancia de Jupiter Notebook que toma los datos directamente desde nuestro Datawarehouse (Redshift), los procesa, y vuelve a montar los datos sobre una nueva tabla, en la misma Base de Datos. 

Por ultimo, desarrollamos los Dashboard interactivos en Quicksight, que nos permite una fácil colección con Redshift.
 