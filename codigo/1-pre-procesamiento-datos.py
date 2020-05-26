#!/usr/bin/env python
# coding: utf-8

# ## Pre procesamiento de datos COVID-Peru
# ### Datos 'crudos' a datos 'pre procesados' listos para análisis
#
# Creado : 25th, Mayo <br>
# Ultima modificacion: 24th Mayo <br>
# Autora: Joan Jennier Martínez P. (UC Berkeley)

# In[373]:


from __future__ import with_statement
import os
import pandas as pd
import numpy as np
import unidecode
import csv
import shutil

# Determining working path
PATH = "/Users/martinezp_jj/Dropbox/__Berkeley/11 Summer 2020/peru-covid19-datalab/"
data_output_path = "datos/procesados/"
data_path = "datos/sin-procesar/"
script_path = "codigo"


# Removing spanish accents data
#Source: https://gist.github.com/Gabriel-Chen/774801e1fc7a28265f57e5c3642b25dc
def remove_accent (feed):
    csv_f = open(feed, encoding='iso-8859-1', mode='r')
    csv_str = csv_f.read()
    csv_str_removed_accent = unidecode.unidecode(csv_str)
    csv_f.close()
    csv_f = open(feed, 'w')
    csv_f.write(csv_str_removed_accent)
    return True


# #### 1-minsa-epp

# In[480]:


## Loading data
fname = os.path.join(PATH, data_path , "1-minsa-epp/ADQUISICIONES-limpio.csv")
df = pd.read_csv(fname, sep=',', header=0,names= ['Fecha', 'RUC','NombreProveedor', 'Producto','Cantidad'])

# Rewrites file without accents
if __name__ == "__main__":
    remove_accent(fname)

# Loading view
pd.set_option('display.max_columns', 999)

# Inpect the df
df.shape   #Rows = 313, Cols = 5
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))

# Correccion de formato de variables
dfn = df.convert_dtypes()
dfn
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# Saving processed data
fname = os.path.join(PATH, data_output_path , "1-minsa-epp.csv")
dfn.to_csv(fname, index=False)


# #### 2-minsa-fallecidos-covid

# In[481]:


## Loading data
fname = os.path.join(PATH, data_path , "2-minsa-fallecidos-covid/FALLECIDOS_CDC-limpio.csv")

# Rewrites file without accents
if __name__ == "__main__":
    remove_accent(fname)

# Load Dataframe
df = pd.read_csv(fname, engine='python', header=0, sep=',',
                 names= ['UUID', 'FechaFallecimiento','Genero', 'FechaNacimiento','Departamento','Provincia','Distrito'])

# Inpect the df
df.shape   #Rows = 3456, Cols = 7
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))


# Genero
df['Genero'].values
df.replace({'Genero': {'MASCULINO': "Hombre", 'FEMENINO' : "Mujer"}}, inplace= True)

# Correccion de formato de variables
dfn = df.convert_dtypes()

# Correccion de fechas
dfn['FechaFallecimiento'] = pd.to_datetime(dfn['FechaFallecimiento'], infer_datetime_format=True)
dfn['FechaNacimiento'] = pd.to_datetime(dfn['FechaNacimiento'], infer_datetime_format=True)

#Creacion variable edad
ahora = pd.Timestamp('now')
dfn['Edad'] = (ahora - dfn['FechaNacimiento']).astype('<m8[Y]')

#Reporte de typos de variable
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# Saving processed data
fname = os.path.join(PATH, data_output_path , "2-minsa-fallecidos-covid.csv")
dfn.to_csv(fname, index=False)


# #### 3-minsa-sinadef

# In[511]:


fname = os.path.join(PATH, data_path , "3-minsa-sinadef/SINADEF_DATOS_ABIERTOS_23052020-limpio.csv")

# Rewrites file without accents
if __name__ == "__main__":
    remove_accent(fname)

df = pd.read_csv(fname, engine= 'python', sep= ';', skiprows= [0,1,2], error_bad_lines=False, usecols= range(1, 19),
                names= ['SeguroTipo', 'Genero', 'Edad', 'EdadUnidad', 'EstadoCivil',
       'NivelEducacion', 'UbigeoDomicilio', 'DomicilioPais',
       'DomicilioDepartamento', 'DomicilioProvincia', 'DomicilioDistrito',
       'DefuncionFecha', 'DefuncionAnio', 'DefuncionMes', 'DefuncionLugar', 'DefuncionInstitucion',
        'MuerteViolenta',
       'Necropsia'])

# Loading view
pd.set_option('display.max_columns', 999)

# Inpect the df
df.shape   #Rows = 119 959, Cols = 9
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))

# Correccion de valores de variables
# Genero
df.replace({'Genero': {'MASCULINO': "Hombre", 'FEMENINO' : "Mujer"}}, inplace= True)
# df.replace({'Necropsia': {'no se realizo necropsia ': "No", 'si se realizo necropsia' : "Si",
#                           'sin registro': 'sin registro'}}, inplace= True)

#A minusculas
df['SeguroTipo'] = df['SeguroTipo'].str.lower()
df['EdadUnidad'] = df['EdadUnidad'].str.lower()
df['NivelEducacion'] = df['NivelEducacion'].str.lower()
df['EstadoCivil'] = df['EstadoCivil'].str.lower()
df['DomicilioPais'] = df['DomicilioPais'].str.lower()
df['DomicilioDepartamento'] = df['DomicilioDepartamento'].str.lower()
df['DomicilioProvincia'] = df['DomicilioProvincia'].str.lower()
df['DomicilioDistrito'] = df['DomicilioDistrito'].str.lower()
df['DefuncionLugar'] = df['DefuncionLugar'].str.lower()
df['DefuncionInstitucion'] = df['DefuncionInstitucion'].str.lower()
df['MuerteViolenta'] = df['MuerteViolenta'].str.lower()
df['Necropsia'] = df['Necropsia'].str.lower()

# Correccion de formato de variables
dfn = df.convert_dtypes()
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# Correccion de fechas
dfn['DefuncionFecha'] = pd.to_datetime(dfn['DefuncionFecha'], infer_datetime_format=True)

# # Saving processed data
fname = os.path.join(PATH, data_output_path , "3-minsa-sinadef.csv")
dfn.to_csv(fname, index=False)


# #### 4-minsa-contagiados-covid

# In[502]:


## Loading data
fname = os.path.join(PATH, data_path , "4-minsa-contagiados-covid/DATOSABIERTOS_SISCOVID-limpio.csv")

# Rewrites file without accents
if __name__ == "__main__":
    remove_accent(fname)

df = pd.read_csv(fname, engine='python', sep=',', header=0,
                 names= ['UUID', 'Departamento','Provincia', 'Distrito','MetodoDiagnostico-abv','Edad','Genero', 'FechaDiagnostico'])

# Loading view
pd.set_option('display.max_columns', 999)

# Inpect the df
df.shape   #Rows = 119 959, Cols = 9
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))

# # Correccion de valores de variables

# Genero
df['Genero'].values
df.replace({'Genero': {'MASCULINO': "Hombre", 'FEMENINO' : "Mujer"}}, inplace= True)

#Metodos de diagnostico
df['MetodoDiagnostico']= df['MetodoDiagnostico-abv']
df.replace({'MetodoDiagnostico': {'PCR': "Prueba reacción en cadena de poliomerasa (PCR)",
                                  'PR' : "Prueba rápida"}}, inplace= True)

# Correccion de formato de variables
dfn = df.convert_dtypes()
dfn
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# Correccion de fechas
dfn['FechaDiagnostico'] = pd.to_datetime(dfn['FechaDiagnostico'], infer_datetime_format=True)

# Saving processed data
fname = os.path.join(PATH, data_output_path , "4-minsa-contagiados-covid.csv")
dfn.to_csv(fname, index=False)


# #### 5-pcm-mef-donaciones

# In[503]:


## Loading data (copy with accents)
fname = os.path.join(PATH, data_path , "5-pcm-mef-donaciones/pcm_donaciones-limpio.csv")

# Rewrites file without accents
if __name__ == "__main__":
    remove_accent(fname)

df = pd.read_csv(fname, engine='python', sep=',', header=0,
                 names=['AnioEje', 'Region', 'TipoGobiernoAbv', 'TipoGobierno', 'SectorCod', 'Sector', 'PliegoCod', 'Pliego', 'SecuenciaEjecutoraCod', 'EjecutoraCod', 'Ejecutora', 'AlmacenCod', 'SecuenciaAlmacenCod', 'Almacen', 'TipoTransaccionCod', 'TipoTransaccion', 'MovimientoID', 'OrdenCompraID', 'ProveedorCodigo', 'Proveedor', 'ObservacionDonacion', 'MovimientoFecha','MovimientoMesCod', 'MovimientoMes', 'TipoUsoCodigo', 'GuiaRemisionCodigo', 'FacturaNumero', 'EstadoKardex','FechaRegistro', 'ConformidadIngresoDocumento', 'ConformidadIngresoFecha', 'IngresoGlosa', 'BienTipoCod', 'BienGrupoCod', 'BienGrupo', 'BienClaseCod', 'BienClase', 'BienFamiliaCod', 'BienFamilia', 'BienItemCod', 'BienItem', 'BienUnidaMedidaCod','BienUnidadMedida', 'BienMarcaCod','BienMarca','BienCantidad', 'BienPrecioUnitario', 'BienValorTotal'])

# Inpect the df
df.shape   #Rows = 1 090, Cols = 48
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))

# # Correccion de valores de variables

# Correccion de formato de variables
dfn = df.convert_dtypes()
dfn
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# # Correccion de fechas
dfn['MovimientoFecha'] = pd.to_datetime(dfn['MovimientoFecha'], infer_datetime_format=True)
dfn['FechaRegistro'] = pd.to_datetime(dfn['FechaRegistro'], infer_datetime_format=True)
dfn['ConformidadIngresoFecha'] = pd.to_datetime(dfn['ConformidadIngresoFecha'], infer_datetime_format=True)

# Saving processed data
fname = os.path.join(PATH, data_output_path , "5-pcm-mef-donaciones.csv")
dfn.to_csv(fname, index=False)


# #### 6-pcm_ejecucion

# In[505]:


## Loading data
fname = os.path.join(PATH, data_path , "6-pcm_ejecucion/pcm_covid-limpio.csv")

## Removing accents
if __name__ == "__main__":
    remove_accent(fname)

df = pd.read_csv(fname, engine= 'python', sep= ',')

# Inpect the df
df.shape   #Rows = 1 090, Cols = 48
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))

# # Correccion de valores de variables

# Correccion de formato de variables
dfn = df.convert_dtypes()
dfn
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# Saving processed data
fname = os.path.join(PATH, data_output_path , "6-pcm_ejecucion.csv")
dfn.to_csv(fname, index=False)


# #### 7-osce-contrataciones

# In[506]:


fname = os.path.join(PATH, data_path , "7-osce-contrataciones/CONOSCE_CONTRATACIONDIRECTA-limpio.csv")

## Removing accents
if __name__ == "__main__":
    remove_accent(fname)

df = pd.read_csv(fname, engine= 'python', sep= ',')

# Inpect the df
df.shape   #Rows = 1 048 575, Cols = 10
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))

# Correccion de formato de variables
dfn = df.convert_dtypes()
dfn
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# Saving processed data
fname = os.path.join(PATH, data_output_path , "7-osce-contrataciones.csv")
dfn.to_csv(fname, index=False)


# #### 8-Bono

# In[507]:


## Loading data
fname = os.path.join(PATH, data_path , "8-bonos-covid/Bonos_universo-limpio.csv")
df = pd.read_csv(fname, engine='python', sep=';', header=0, names= ['CodigoHogar', 'Ubigeo','Departamento', 'Provincia','Distrito','Genero','Restriccion','AntiguoPadron',
                                                                         'Discapacidad','AdultoMayor'])
# Loading view
pd.set_option('display.max_columns', 999)

# Inpect the df
df.shape   #Rows = 1 048 575, Cols = 10
df.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(df.info(null_counts=True))

# Correccion de valores de variables

# Genero
df['Genero'].values
df.replace({'Genero': {1: "Hombre", 2 : "Mujer"}}, inplace= True)

#Indicador de antiguo padron
df['AntiguoPadron'].values
df.replace({'AntiguoPadron': {1: "Si", 0 : "No"}}, inplace= True)

# Discapacidad
df['AdultoMayor'].values
df.replace({'AdultoMayor': {1: "Si", 0 : "No"}}, inplace= True)

# Adulto mayor
df['Discapacidad'].values
df.replace({'Discapacidad': {1: "Si", 0 : "No"}}, inplace= True)

# Correccion de formato de variables
dfn = df.convert_dtypes()
dfn
dfn.dtypes
print("-----------Name of columns-----------", "\n", df.columns)
print("-----------Information of dataset----------")
print(dfn.info(null_counts=True))

# Saving processed data
fname = os.path.join(PATH, data_output_path , "8-bonos-covid.csv")
dfn.to_csv(fname, index=False)
