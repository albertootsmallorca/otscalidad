#Librerias necesarias
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import json

from datetime import datetime

spark = SparkSession.builder.getOrCreate()

class CalidadBase: 
    def __init__(self, fuente='', esquema='', tabla=''): 
        self.lst_cambios = list()
        self.fuente = fuente
        self.esquema = esquema
        self.tabla = tabla
        self.registro_base = {
            'ID_CONTROL': '', 
            'FE_EJECUCION': '',
            'CHECK_EJECUCION': '',
            'RESULTADO_EJECUCION': '',
            'CRITICIDAD': '',
            'AMBITO': '',
            'ID_DOMINIO': '',
            'PIPELINE': '',
            'FUENTE': '', 
            'ESQUEMA': '',
            'TABLA': '',
            'CAMPO': '',
            'FE_CARGA': '',
            'FE_ULT_MODIF': '',
            'USER_MODIF': ''
        }
        
    def add_cambio(self, check, resultado, dimensiones): 
        registro_nuevo = self.registro_base.copy()
        dimensiones['FE_EJECUCION'] = datetime.today()    #.strftime("%Y-%m-%d %H:%M:%S")
        dimensiones['CHECK_EJECUCION'] = int(check)
        dimensiones['RESULTADO_EJECUCION'] = str(resultado)
        dimensiones['ESQUEMA'] = self.esquema
        dimensiones['FUENTE'] = self.fuente
        dimensiones['TABLA'] = self.tabla
        dimensiones['FE_CARGA'] = datetime.today()        #.strftime("%Y-%m-%d %H:%M:%S")
        dimensiones['FE_ULT_MODIF'] = datetime.today()    #.strftime("%Y-%m-%d %H:%M:%S")

        registro_nuevo.update(dimensiones.copy())
        self.lst_cambios.append(registro_nuevo)
        
    def save_report_delta_table(self, tabla_escritura='HEC_TRACKING', esquema_escritura="calidad", mode="append", deltaMount="/mnt/deltaMount/RCP"):
        """
        Guarda los cambios almacenados en self.lst_cambios en 2 repositorios: 
        1. hive_metastore.calidad.hec_tracking
        2. blob_storage:deltatest/RCP/HEC_TRACKING        
        """
        df = spark.createDataFrame(self.lst_cambios)
        df.write.format("delta").mode(mode).option("path", f"{deltaMount}/{tabla_escritura}").saveAsTable(f"{esquema_escritura}.{tabla_escritura}")    
    
    def multiple_update_df(self, seq_modifications): 
        """
        realiza multiples modificaciones sobre self.df incluyendo dentro de un bucle
        el listado de modificaciones y pasando una a una al metodo self.update_df()
        """
        for modification in seq_modifications: 
            self.update_df(modification)
            
    def get_reporte(self):
        """
        Return el listado de cambios
        """
        return self.lst_cambios

class CalidadGlobal(CalidadBase): 
    #     Esta clase sirve para aplicar cambios relacionados con la calidad
    #     entre varios dataframes de spark 
    #     y proporcionar registros de las modificaciones
    
    def __init__(self, fuente='', esquema='', tabla=''):
        super().__init__(fuente=fuente, esquema=esquema, tabla=tabla)
        
    def update_df(self, modificacion):
        dimensiones, parametros = modificacion.get('dimensiones', dict()), modificacion.get('parametros', dict())
        
        if dimensiones.get('ID_CONTROL', None) == 4:  # volumetria (ambas tablas en mismo entorno ADB)
            """
            Nivel 2: Controles técnicos y de Negocio
            Volumetría: Verificar el numero de registros que pasan de una tabla a otra. 
            Que la volumetría de registro cargados entre las tablas cuadre( Fichero RAW a BRONZE, de BRONZE a SILVER, etc…)
            
            dadas dos tablas comprobar su diferencia de registros     
            """
            tabla_origen = parametros.get('tabla_origen')
            tabla_destino = parametros.get('tabla_destino')    
            
            conteo_origen = spark.read.table(tabla_origen).count()
            conteo_destino = spark.read.table(tabla_destino).count()

            resultado = str(conteo_origen - conteo_destino) + ' registros diferentes'
            check = int(not bool(conteo_origen - conteo_destino))
            self.add_cambio(check, resultado, dimensiones)
            
        elif dimensiones.get('ID_CONTROL', None) == 7:  # integridad refeciaal (ambas tablas en mismo entorno ADB)
            """
            Nivel 2: Controles técnicos y de Negocio
            Integridad Referencial entre dos tablas HEC1-HEC2: I
            dentificar los valores huérfanos que hay en una tabla de HEC1 que no están presentes en la otra tabla de HEC2 utilizada como referencia.
            
            Es necesario eliminar los registros o solamente avisar??            
            """
            tabla_target = parametros.get('tabla_target')
            columna_target = parametros.get('columna_target')
            
            tabla_control = parametros.get('tabla_control')
            columna_control = parametros.get('columna_control')
            
            df_target = spark.read.table(tabla_target)
            df_control = spark.read.table(tabla_control)
            
            conteo_antes = df_target.count() 
            df_target = df_target.where(df_target[columna_target].isin(df_control.rdd.map(lambda x: x[columna_control]).collect()))
            conteo_despues = df_target.count()
            resultado = str(conteo_antes - conteo_despues) + ' no existen en la Dimensión correspondiente'
            check = int(not bool(conteo_antes - conteo_despues))
            self.add_cambio(check, resultado, dimensiones)
            
        elif dimensiones.get('ID_CONTROL', None) == 9:  # integridad refeciaal (ambas tablas en mismo entorno ADB)
            """
            Nivel 1: Limpieza y Homogeneización del Dato
            Campos que informan el mismo dato pero tienen un nombre diferentes 
            (Por ejemplo, busco un campo por nombre y reviso con el control que tengan el mismo tipo (int,varchar,Datetime, etc…)
            
            campos que informan: dada una lista de columnas, que busque en todas las tablas dadasy detecte si el tipo de dato es correcto o no 
            """
            seq_comprobaciones = parametros.get('comprobaciones')
            
            for comprobacion in seq_comprobaciones: 
                tabla_target = comprobacion.get("tabla_target", None)
                columna_target = comprobacion.get("columna_target", None)
                tipo_target  = comprobacion.get("tipo_target", None)
                
                df_target = spark.read.table(tabla_target)
                
                res = dict(df_target.dtypes).get(columna_target) == tipo_target
                resultado = f"Tabla {tabla_target}: Columna {columna_target}: Tipo {tipo_target}: {res}"                
                check = int(res)
                self.add_cambio(check, resultado, dimensiones)
        else: 
            check = int(False)
            resultado = f"CalidadGlobal. ID_CONTROL no registrado: {dimensiones.get('ID_CONTROL', None)}"            
            self.add_cambio(check, resultado, dimensiones)
    
        
class Calidad(CalidadBase): 
    #     Esta clase sirve para aplicar cambios relacionados con la calidad
    #     de un dataframe de spark 
    #     y proporcionar registros de las modificaciones
    def __init__(self, df=None, fuente='', esquema='', tabla=''): 
        super().__init__(fuente=fuente, esquema=esquema, tabla=tabla)
        self.df = df
        
    def update_df(self, modificacion):
            
        dimensiones, parametros = modificacion.get('dimensiones', dict()), modificacion.get('parametros', dict())
#         registro_nuevo = self.registro_base.copy()
        
        if self.df and dimensiones.get('ID_CONTROL', None) == 1:  # 1   
            """
            Nivel 1: Limpieza y Homogeneización del Dato
            Null. (PK, -NOT NULL etc..) Dado un conjunto de Columnas definidas como PK-NOT NULL o campos relevantes.
            
            Dado una lista de columnas, si el registro en esa dimensión columna es nulo
            elimina el registro del dataset
            """
            conteo_antes = self.df.count()
            columns = parametros.get('columnas', [])

            self.df = self.df.dropna(subset=columns)
            conteo_despues = self.df.count()
            resultado = str(conteo_antes - conteo_despues) + ' registros eliminados por nulos en columnas correspondientes'
            check = int(not bool(conteo_antes - conteo_despues))
            self.add_cambio(check, resultado, dimensiones)
            
        elif self.df and dimensiones.get('ID_CONTROL', None) == 8:  # 8   
            """
            Nivel 1: Limpieza y Homogeneización del Dato
            campos no informados: el dato es "" pero no null
            
            Dado una lista de columnas, si el registro en esa dimensión columna es nulo
            elimina el registro del dataset
            """
            conteo_antes = self.df.count()
            columns = parametros.get('columnas', [])

            for column in columns: 
                self.df = self.df.where(self.df[column] != "")            
            conteo_despues = self.df.count()
            resultado = str(conteo_antes - conteo_despues) + ' registros eliminados por campos no informados en columnas correspondientes'  
            check = int(not bool(conteo_antes - conteo_despues))
            self.add_cambio(check, resultado, dimensiones)
            
        elif self.df and dimensiones.get('ID_CONTROL', None) == 2:  # 2
            """
            Nivel 1: Limpieza y Homogeneización del Dato
            Duplicados en claves primarias PK.( Criterio de unicidad por fuente basado en una PK o clave especifica)​
            
            elimina registros del dataset que son duplicados. opciones: 'all' o listado de columnas []            
            """
            conteo_antes = self.df.count()
            columns = parametros.get('columnas', [])
            if columns == 'all': 
                self.df = self.df.drop_duplicates()
            else: 
                self.df = self.df.drop_duplicates(subset=columns)
            conteo_despues = self.df.count()
            resultado = str(conteo_antes - conteo_despues) + ' registros eliminados por duplicados'
            check = int(not bool(conteo_antes - conteo_despues))
            self.add_cambio(check, resultado, dimensiones)
            
        elif self.df and dimensiones.get('ID_CONTROL', None) == 3:  # integridad valores (ambas tablas en delta_tables DataBricks)
            """
            Nivel 2: Controles técnicos y de Negocio. Integridad Referencial entre dos tablas HEC-DIM: 
            Identificar los valores huérfanos que hay en la tabla de HEC y no están asociados a ninguna categoría de la DIM.
            
            un registro del dataset se eliminará si su valor no está incluido entre los de  
            la columna de la tabla de control            
            """
            columna_target = parametros.get('columna_target') 
            tabla_control = parametros.get('tabla_control')
            columna_control = parametros.get('columna_control')    
            
            df_control = spark.read.table(tabla_control)
            
            conteo_antes = self.df.count() 
            self.df = self.df.where(self.df[columna_target].isin(df_control.rdd.map(lambda x: x[columna_control]).collect()))
            conteo_despues = self.df.count()
            resultado = str(conteo_antes - conteo_despues) + ' no existen en la Dimensión correspondiente'
            check = int(not bool(conteo_antes - conteo_despues))
            self.add_cambio(check, resultado, dimensiones)
            
        elif dimensiones.get('ID_CONTROL', None) == 5:  # blancos (trim sobre columna)
            """
            Nivel 1: Limpieza y Homogeneización del Dato
            Blancos. (por IZQ y por DERECHA (Trim)) sobre campos relevantes.
            
            dada una tabla y una lista de columnas, hacer trim sobre los registros            
            """
            columnas = parametros.get('columnas', [])
            for column in columnas: 
                self.df = self.df.withColumn(column, F.trim(column).alias(column))
            resultado = 'registros modificados y ejecutado trim sobre las columnas correspondientes'
            check = int(True)
            self.add_cambio(check, resultado, dimensiones)

        elif dimensiones.get('ID_CONTROL', None) == 6:  # caracteres no validos
            """
            Nivel 1: Limpieza y Homogeneización del Dato
            Caracteres no válidos.(&%#$)
            
            dada una tabla y una lista de columnas, eliminar registros que contengan caracteres no validos        
            """
            conteo_antes = self.df.count() 
            columnas = parametros.get('columnas', [])
            regular_expression = parametros.get('regular_expression', str())
            for column in columnas: 
                self.df = self.df.filter(~self.df[column].rlike(regular_expression))
            conteo_despues = self.df.count() 

            resultado = f'{conteo_antes - conteo_despues} registros eliminados por contener caracteres invalidos sobre las columnas correspondientes: {regular_expression}'
            check = int(not bool(conteo_antes - conteo_despues))
            self.add_cambio(check, resultado, dimensiones)
            
        else: 
            check = int(False)
            resultado = f"Calidad. ID_CONTROL no registrado: {dimensiones.get('ID_CONTROL', None)}"
            self.add_cambio(check, resultado, dimensiones)
            
    
def read_json_as_dict(container, filename, BSC):
    blob_client = BSC.get_blob_client(container, filename)
    return json.loads(blob_client.download_blob().readall())