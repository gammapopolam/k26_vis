# Визуализация общественного транспорта с помощью loom
# 
# Скрипт обрабатывает базу данных общественного транспорта в формате GTFS 
# путем выборки данных о каждом маршруте, проходящем через остановки сети.
# 
# 

import pandas as pd
import geopandas as gpd
import shapely.geometry
import math
import subprocess
import time


pd.options.mode.chained_assignment = None

def JoinDFs(st, t, r):
    return st.merge(t, on='trip_id', how='left').merge(r, on='route_id', how='left')

def RTSelection(trip_ids, t, r):
    # Выборка маршрутов, соответствующих выбранным поездкам

    # На вход: 
    # trip_ids - список идентификаторов поездок
    # t - dataframe всех поездок маршрутов
    # r - dataframe всех маршрутов 

    # Все поездки, соответствующие выборке
    t_selection=t[t.trip_id.isin(trip_ids)]
    # Список уникальных идентификаторов маршрутов
    route_ids=list(set(t_selection.route_id))
    # Выборка маршрутов по идентификаторам
    r_selection=r[r.route_id.isin(route_ids)]
    return t_selection, r_selection



def STSelection(stop_id, st):
    # Выборка поездок маршрутов, проходящих через остановку

    # На вход: 
    # stop_id - идентификатор остановки
    # st - dataframe всех поездок маршрутов

    # Все поездки, проходящие через остановку
    st_selection=st[st.stop_id==stop_id]

    # Пустой dataframe, в который будет внесены выборки поездок маршрутов
    st_empty=pd.DataFrame(columns=st.columns)
    for _, trip in st_selection.iterrows():
        # Выборка поездок от момента прибытия на остановку stop_id до конца поездки
        st_serving=st.loc[(trip.trip_id==st.trip_id) & (trip.stop_sequence<=st.stop_sequence)]
        # Обновление порядка остановок
        st_serving['stop_sequence']=st_serving['stop_sequence']-trip.stop_sequence+1
        # Конкатенация пустого dataframe с выборкой
        st_empty=pd.concat([st_empty, st_serving])
    return st_empty

def ShapeSelection(stop, shape_ids, shp):
    # Выборка геометрии маршрутов
    # 
    # На вход: 
    # stop - остановка с геометрией типа точка, 
    # shape_ids - выборка идентификаторов геометрии маршрутов,
    # shp - выборка геометрии маршрутов
    dist=math.inf
    seq=None
    stop_geom=stop.iloc[0].geometry
    # Пустой dataframe, в который будет внесены выборки геометри маршрутов
    shp_empty=gpd.GeoDataFrame(columns=shp.columns)
    for shape_id in shape_ids:
        # Выборка геометрии маршрута по shape_id
        shp_selection=shp[shp.shape_id==shape_id]
        # Нахождение точки маршрута, расстояние от которой до остановки наименьшее
        for  _, shape_point in shp_selection.iterrows():
            meas=stop_geom.distance(shape_point.geometry)
            if meas<=dist:
                dist=meas
                seq=shape_point.shape_pt_sequence
        # Найдена точка маршрута, расстояние от которой до остановки наименьшее.
        # Выборка из dataframe геометрии маршрута: seq - порядок прохождения точки маршрута.
        # Необходимо в пустой dataframe добавить все точки маршрута, которые по порядку будут не ниже, чем seq
        shp_serving=shp_selection[shp_selection.shape_pt_sequence>=seq]
        # Обновление порядка прохождения точек маршрута
        shp_serving['shape_pt_sequence']=shp_serving['shape_pt_sequence']-seq+1
        # Конкатенация пустого dataframe с выборкой
        shp_empty=pd.concat([shp_empty, shp_serving])
    return shp_empty


    
def NetFromStop(stop_id, s, st, t, shp, r):
    # Выборка базы данных для остановки
    
    stop_geom=s[s.stop_id==stop_id]
    # Выборка поездок маршрутов и их порядка следования
    st_selection=STSelection(stop_id, st)
    # Объединение dataframe порядка следования маршрутов и самих поездок
    st_selection_merge=st_selection.merge(t, on='trip_id', how='left')
    # Список уникальных поездок, проходящих через остановку
    trip_ids=list(set(st_selection_merge.trip_id))
    # Выборка поездок и самих маршрутов 
    t_selection, r_selection=RTSelection(trip_ids, t, r)
    # Список уникальных идентификаторов геометрий маршрутов
    shape_ids=list(set(st_selection_merge.shape_id))
    # Выборка геометрий маршрутов, проходящих через остановку
    shp_selection=ShapeSelection(stop_geom, shape_ids, shp)

    return [st_selection, t_selection, shp_selection, r_selection]

def IsZombie(stop_id, st):
    # Если через остановку не проходят поездки, тогда остановка считается пустой
    st_selection=st[st.stop_id==stop_id]
    #print(st_selection, 'Zombie')
    if st_selection.empty:
        return True
    else:
        return False
    
def GenTiles(stop_id, method='hillc'):
    # Генерация тайлов по выборке для остановки
    tiles=f'/home/gamma/k26_2/tiles/stop_{str(stop_id)}'
    print(subprocess.run(f'rm -rf /home/gamma/k26_2/tiles/stop_{str(stop_id)}/*', stderr=subprocess.STDOUT, shell=True))
    print(subprocess.run('mkdir -p /home/gamma/k26_2/tiles/stop_'+str(stop_id), stderr=subprocess.STDOUT, shell=True))
    print(subprocess.run('gtfs2graph /home/gamma/k26_2/gtfs_edited | topo --smooth=0 > /home/gamma/k26_2/k26_topo.json', shell=True))
    topograph_path = 'k26_topo.json'
    loom_path = 'k26_loom.json'
    
    loom = f'loom --optim-method={method} --ilp-num-threads=4 < /home/gamma/k26_2/{topograph_path} > /home/gamma/k26_2/{loom_path}'
    print(subprocess.run(loom, stderr=subprocess.STDOUT, shell=True))
    ts = f'transitmap --print-stats --outline-width=0 --labels --tight-stations --render-engine=mvt --line-width=4 --line-spacing=3 -z 12,13,14,15,16,17,18 --mvt-path={tiles} < /home/gamma/k26_2/{loom_path}'
    print(subprocess.run(ts, stderr=subprocess.STDOUT, shell=True))

# Входные данные в формате dataframe 
trips=pd.read_csv(r'/home/gamma/k26_2/gtfs_static/trips.txt', sep=',')
routes=pd.read_csv(r'/home/gamma/k26_2/gtfs_static/routes.txt', sep=',')
stoptimes=pd.read_csv(r'/home/gamma/k26_2/gtfs_static/stop_times.txt', sep=',')

# Для остановок и геометрии маршрутов необходимы геометрические объекты - точки - для вычисления ближайших
# stops, shapes - в формате geodataframe
stops=pd.read_csv(r'/home/gamma/k26_2/gtfs_static/stops.txt', sep=',')
stops_geom=gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops.stop_lat, stops.stop_lon))

shapes=pd.read_csv(r'/home/gamma/k26_2/gtfs_static/shapes.txt', sep=',')
shapes_geom=gpd.GeoDataFrame(shapes, geometry=gpd.points_from_xy(shapes.shape_pt_lat, shapes.shape_pt_lon))
class Evaluations:
    def __init__(self, method='hillc'):
        self.method=method
        self.logs=open(f'/home/gamma/k26_2/logs_{self.method}.txt', 'w')
        self.logs.close()
        self.type=type
    def AddLog(self, stop_id, delta_net, delta_loom):
        self.logs=open(f'/home/gamma/k26_2/logs_{self.method}.txt', 'a')
        self.logs.write(f'Stop ID: {stop_id} Net selection: {math.ceil(delta_net)} Tile generation: {math.ceil(delta_loom)}\n')
        self.logs.close()

# Цикл выборки базы данных для остановки и генерации тайлов для неё
eval=Evaluations(method='hillc')
for _, stop in stops.iterrows():
    stop_id=stop.stop_id
    if not IsZombie(stop_id, stoptimes):
        start_net=time.time()
        st_s, t_s, shp_s, r_s=NetFromStop(stop_id, stops_geom, stoptimes, trips, shapes_geom, routes)
        fin_net=time.time()
        time_net=fin_net-start_net
        st_s.to_csv(r'/home/gamma/k26_2/gtfs_edited/stop_times.txt', index=False, sep=',', header=True)
        t_s.to_csv(r'/home/gamma/k26_2/gtfs_edited/trips.txt', index=False, sep=',', header=True)
        r_s.to_csv(r'/home/gamma/k26_2/gtfs_edited/routes.txt', index=False, sep=',', header=True)
        shp_s.drop('geometry', axis=1).to_csv(r'/home/gamma/k26_2/gtfs_edited/shapes.txt', index=False, sep=',', header=True)
        print(f'Generating tiles for stop {stop_id}')
        start_gen=time.time()
        GenTiles(stop_id, method='hillc')
        fin_gen=time.time()
        time_gen=fin_gen-start_gen
        eval.AddLog(stop_id=stop_id, delta_net=time_net, delta_loom=time_gen)
    else:
        print('Zombie stop with id', stop_id)




