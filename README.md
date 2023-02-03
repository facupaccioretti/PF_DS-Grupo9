# <h1 align=center> **INGENIERIA Y ANÁLISIS DE DATOS PARA OLIST** </h1>


</p>


## **Contexto:**

Olist es una empresa brasileña nacida en 2015, dedicada al e-commerce. Tiene como objetivo facilitar tanto a particulares como a pymes y grandes retailers, el acceso a las ventas online. Gestiona para sus vendedores logística, transporte y los ayuda a posicionarse dentro de los principales marketplaces.


## **Propósito del proyecto:**

Desarrollamos este proyecto con la idea de cumplir con todos los objetivos propuestos por Olist. Entre ellos:
+ Comprender la situación actual de Olist.
  
+ Automatización de la ingesta de datos, proceso ETL y transferencia a las herramientas de visualización y Machine Learning utilizadas, todo dentro del servicio de nube AWS.
+ Preparación de la base de datos para cargas incrementales futuras.
+ Creación e implementación de dos modelos de Machine Learning. El primero de análisis de sentimiento y el segundo de clasificación (topics principales por score) 
+ Brindar información relevante y hacer recomendaciones para los departamentos de Ventas, Marketing y Logística, mediante un exhaustivo análisis de datos.
 

## **Equipo de trabajo G9C:**
https://raw.githubusercontent.com/facupaccioretti/PF_DS-Grupo9/tree/semana2/assets/equipo.jpeg

## **Datos:**

Trabajamos con datos entre 2016 y 2018 provistos por Olist. Con información sobre órdenes, vendedores, clientes, productos, ventas, pagos, reviews de los compradores y datos de geolocalización


## **Desarrollo del proyecto:**

En primer lugar se realizó un EDA exploratorio y un análisis de la calidad de datos. Seguimos con la definición de los objetivos y el alcance del proyecto.
Comenzamos con Amazon S3, que funciona como tener un disco duro en la nube. Nos permite almacenar información y tenerla disponible para que se use en otros servicios de Amazon Web Services. 

Después de tener los datos cargados en Amazon S3, utilizamos AWS Glue para hacer el ETL, desde los datos en CSV. Los procesamos y limpiamos y los guardamos con un formato más amigable para nuestra base de datos. 

Luego, también usando Glue, cargamos estos datos en Amazon Redshift. Amazon Redshift nos permite crear una base de datos y ponerlos a disposición, como un data Warehouse. Amazon redshift tiene la ventaja de estar diseñada para el análisis de datos, enfocándose menos en las transacciones y más en las consultas. 

Para finalizar, utilizamos Amazon SageMaker para la implementación de nuestros modelos de Machine Learning y Amazon QuickSight para la visualización de los datos. Al ser todas estas herramientas proporcionadas por AWS, fue simple la conexión entre ellas.

Queda adjunta una imagen del workflow del proyecto, para facilitar el entendimiento del proceso:
https://raw.githubusercontent.com/facupaccioretti/PF_DS-Grupo9/tree/semana2/assets/workflow.jpeg

## **Análisis de datos:**
Planteamos KPIs asociados a los objetivos y los visualizamos en un Dashboard con una estructura que lo divide en los departamentos de Ventas, Marketing y Logística, dejando en claro cuál es el target de las distintas páginas del dashboard. El mismo, brinda información valiosa para facilitar en la empresa la toma de decisiones y el desarrollo de estrategias.

## IMAGEN EJEMPLO DASHBOARD 1
## IMAGEN EJEMPLO DASHBOARD 2
Obtuvimos insights e información valiosa que fue detallada en el siguiente documento: 
https://docs.google.com/document/d/1a2p8tULGtgddL_ryEYzGuHFVWnd9Uo383hDKvlv8VGc/edit?usp=sharing

## **Modelos de Machine Learning:**
Construimos dos modelos de ML, de Procesamiento de lenguaje y Análisis de Sentimientos,  ambos sobre los reviews de compra, con el objetivo de darle valor a la Voz de Cliente (VoC). 
Para el primero, de Clasificación, usamos Random Forest. Tiene como objetivo decidir si un comentario es positivo o negativo. Para el entrenamiento, se discretizaron los puntajes de reviews, obteniendo así Accuracy y Recall por encima de 0.80. 
Para el segundo modelo utilizamos LDA (Latent Dirichlet Allocation), de aprendizaje No Supervisado, el cual extrae de un grupo de documentos, los principales temas y términos dentro de el. Se analizan los comentarios por su review score, es decir, los agrupamos en 5 categorías. De esta manera, se tiene un control y monitoreo del cuerpo entero de mensajes, permitiendo saber fácilmente  cuales temas son los de mayor peso para los clientes, según puntuación de review. Estos resultados se visualizan dentro del dashboard de Marketing como una tabla de fácil lectura que agrupa los 3 principales topics, a su vez con los 3 principales 3-eneagramas para cada review score.

https://raw.githubusercontent.com/facupaccioretti/PF_DS-Grupo9/tree/semana2/assets/salidaML.jpeg
## **Recomendaciones y cierre:**

+ Departamento de ventas: Buscar contratos con nuevos vendedores en río de janeiro, que es la segunda economía estatal más grande de Brasil (el doble de grande que Paraná) y tiene un 40% menos de ingresos por ventas que Paraná.
+ Departamento de marketing: Proveer componentes necesarios en la base de datos, para completar un Embudo de ventas, y poder presentar detalladamente las etapas por las que pasa un cliente potencial desde el primer contacto con la empresa hasta el cierre de la venta (presupuesto de marketing, clics en la web).
+ Departamento de logística: Mejorar la estimación de las entregas(que en promedio se entregan 11 días antes de lo estimado). Lo ideal sería bajar algunos días este promedio, donde mantenes un margen de error en la estimación y no arriesgas a una caída del score.