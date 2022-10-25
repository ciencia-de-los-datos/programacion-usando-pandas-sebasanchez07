"""
Laboratorio - Manipulación de Datos usando Pandas
-----------------------------------------------------------------------------------------

Este archivo contiene las preguntas que se van a realizar en el laboratorio.

Utilice los archivos `tbl0.tsv`, `tbl1.tsv` y `tbl2.tsv`, para resolver las preguntas.

"""
import pandas as pd
import numpy as np

tbl0 = pd.read_csv("tbl0.tsv", sep="\t")
tbl1 = pd.read_csv("tbl1.tsv", sep="\t")
tbl2 = pd.read_csv("tbl2.tsv", sep="\t")


def pregunta_01():
    solucion = tbl0.shape[0]

    return solucion


def pregunta_02():
    solucion = tbl0.shape[1]

    return solucion


def pregunta_03():
    
    return tbl0.groupby("_c1")["_c1"].size()


def pregunta_04():
    solucion = tbl0[["_c1", "_c2"]].groupby("_c1")["_c2"].mean()
    return solucion


def pregunta_05():
    solucion = tbl0[["_c1", "_c2"]].groupby("_c1")["_c2"].max()
    return solucion


def pregunta_06():
    solucion = list(np.sort(np.unique([i.upper() for i in tbl1["_c4"]])))
    return solucion


def pregunta_07():
    
    respuesta = tbl0.groupby('_c1')['_c2'].sum()
    

    """
    Calcule la suma de la _c2 por cada letra de la _c1 del archivo `tbl0.tsv`.

    Rta/
    _c1
    A    37
    B    36
    C    27
    D    23
    E    67
    Name: _c2, dtype: int64
    """
    return respuesta


def pregunta_08():
    
    respuesta = tbl0.copy()
    respuesta ['suma'] = respuesta['_c0'] + respuesta['_c2']

    """
    Agregue una columna llamada `suma` con la suma de _c0 y _c2 al archivo `tbl0.tsv`.

    Rta/
        _c0 _c1  _c2         _c3  suma
    0     0   E    1  1999-02-28     1
    1     1   A    2  1999-10-28     3
    2     2   B    5  1998-05-02     7
    ...
    37   37   C    9  1997-07-22    46
    38   38   E    1  1999-09-28    39
    39   39   E    5  1998-01-26    44

    """
    return respuesta


def pregunta_09():
    respuesta = tbl0.copy()
    respuesta ['year'] = respuesta ['_c3'].str.split('-')
    respuesta ['year'] = [row[0] for row in respuesta ['year']]
    """
    Agregue el año como una columna al archivo `tbl0.tsv`.

    Rta/
        _c0 _c1  _c2         _c3  year
    0     0   E    1  1999-02-28  1999
    1     1   A    2  1999-10-28  1999
    2     2   B    5  1998-05-02  1998
    ...
    37   37   C    9  1997-07-22  1997
    38   38   E    1  1999-09-28  1999
    39   39   E    5  1998-01-26  1998

    """
    return respuesta


def pregunta_10():
    
    from functools import reduce
    def sumar(series):
        return reduce(lambda x, y: str(x) + ':' + str(y), series)
    y = tbl0.sort_values('_c2')
    respuesta = y.groupby('_c1').agg({'_c2': sumar})

    """
    Construya una tabla que contenga _c1 y una lista separada por ':' de los valores de
    la columna _c2 para el archivo `tbl0.tsv`.

    Rta/
                                   _c1
      _c0
    0   A              1:1:2:3:6:7:8:9
    1   B                1:3:4:5:6:8:9
    2   C                    0:5:6:7:9
    3   D                  1:2:3:5:5:7
    4   E  1:1:2:3:3:4:5:5:5:6:7:8:8:9
    """
    return respuesta


def pregunta_11():
    
    respuesta = tbl1.copy()
    respuesta = respuesta.groupby('_c0', as_index=False).sum()
    respuesta ['_c4'] = [sorted(row) for row in respuesta ['_c4']]
    respuesta['_c4'] = respuesta['_c4'].transform(lambda x: ','.join(x))
    """
    Construya una tabla que contenga _c0 y una lista separada por ',' de los valores de
    la columna _c4 del archivo `tbl1.tsv`.

    Rta/
        _c0      _c4
    0     0    b,f,g
    1     1    a,c,f
    2     2  a,c,e,f
    3     3      a,b
    ...
    37   37  a,c,e,f
    38   38      d,e
    39   39    a,d,f
    """
    return respuesta


def pregunta_12():
    
    respuesta = tbl2.copy()
    respuesta["_c5"] = respuesta["_c5a"] + ":" + respuesta["_c5b"].map(str)
    respuesta = respuesta [['_c0', '_c5']]
    respuesta = respuesta.groupby(['_c0'], as_index = False).agg({'_c5': ','.join})
    respuesta['_c5'] = [row.split(",") for row in respuesta['_c5']]
    respuesta['_c5'] = [sorted(row) for row in respuesta['_c5']]
    respuesta['_c5'] = [",".join(row) for row in respuesta['_c5']]

    """
    Construya una tabla que contenga _c0 y una lista separada por ',' de los valores de
    la columna _c5a y _c5b (unidos por ':') de la tabla `tbl2.tsv`.

    Rta/
        _c0                                  _c5
    0     0        bbb:0,ddd:9,ggg:8,hhh:2,jjj:3
    1     1              aaa:3,ccc:2,ddd:0,hhh:9
    2     2              ccc:6,ddd:2,ggg:5,jjj:1
    ...
    37   37                    eee:0,fff:2,hhh:6
    38   38                    eee:0,fff:9,iii:2
    39   39                    ggg:3,hhh:8,jjj:5
    """
    return respuesta


def pregunta_13():
    
    respuesta = tbl0.merge(tbl2, how='inner', on='_c0')
    respuesta = respuesta[['_c1', '_c5b']]
    respuesta = respuesta.groupby('_c1').sum()
    respuesta = respuesta['_c5b']
    """
    Si la columna _c0 es la clave en los archivos `tbl0.tsv` y `tbl2.tsv`, compute la
    suma de tbl2._c5b por cada valor en tbl0._c1.

    Rta/
    _c1
    A    146
    B    134
    C     81
    D    112
    E    275
    Name: _c5b, dtype: int64
    """
    return respuesta
