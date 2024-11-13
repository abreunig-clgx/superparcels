
from shapely.geometry import Polygon, LineString, Point
from shapely.geometry.base import BaseGeometry
import matplotlib.pyplot as plt
import logging
from typing import List, Tuple, Union, Optional, Dict
import random
import pandas as pd
import geopandas as gpd


class GeoDataFrame:
    def __init__(self, data_dict: pd.DataFrame, list_of_geometries: List[BaseGeometry]):
        self.data_dict = data_dict
        self.list_of_geometries = list_of_geometries
        
    def build(self):
        df = pd.DataFrame(self.data_dict)
        df['geometry'] = [poly.geometry for poly in self.list_of_geometries]
        return gpd.GeoDataFrame(df, geometry='geometry')

    
        
class Vector:
    def __init__(self, geometry: BaseGeometry):
        self.geometry = geometry

    @classmethod
    def polygons_to_gdf(cls, data_dict: Dict, polygons: List['mPolygon'], epsg: int = 4326) -> gpd.GeoDataFrame:
        """Converts a list of mPolygon objects to a GeoDataFrame."""

        df = pd.DataFrame(data_dict)
        df['geometry'] = [poly.geometry for poly in polygons]

        return gpd.GeoDataFrame(df, geometry='geometry', crs=epsg)

    def add_df(self, df: pd.DataFrame, epsg: int = 4326) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(df, geometry=[self.geometry], crs=epsg)

class mPolygon(Vector):
    def __init__(
        self, 
        origin: Optional[Tuple[float, float]] = None,
        coords: Optional[List[Tuple[float, float]]] = None,
        size: Optional[Tuple[float, float]] = None,
        color: Optional[str] = 'blue',
        alpha: Optional[float] = 0.5,
        **kwargs
        ):
        

        if origin is not None and size is not None:
            self.coords = [
                origin,
                (origin[0] + size[0], origin[1]),
                (origin[0] + size[0], origin[1] + size[1]),
                (origin[0], origin[1] + size[1]),
            ]
        elif coords is not None:
            self.coords = coords
        else:
            raise ValueError("Must provide either `origin` and `size`, or `coords`.")
            
        self.geometry = Polygon(self.coords)
        self.color = color
        self.alpha = alpha

        for key, value in kwargs.items():
            setattr(self, key, value)



class mLine(Vector):
    def __init__(
        self,
        coords: List[Tuple[float, float]],
        color: Optional[str] = 'blue',
        alpha: Optional[float] = 0.5,
        **kwargs
        ):
        self.coords = coords
        self.geometry = LineString(coords)
        self.color = color
        self.alpha = alpha

        for key, value in kwargs.items():
            setattr(self, key, value)

class mPoint(Vector):
    def __init__(
        self,
        coords: Tuple[float, float],
        color: Optional[str] = 'blue',
        alpha: Optional[float] = 0.5,
        **kwargs
        ):
        self.coords = coords
        self.geometry = Point(coords)
        self.color = color
        self.alpha = alpha

        for key, value in kwargs.items():
            setattr(self, key, value)


class Map():
    def __init__(
        self, 
        shapes: List[Vector] = None, # default empty list
        **kwargs
        ):
        if shapes is None:
            shapes = []
        self.shapes = shapes

        for key, value in kwargs.items():
            setattr(self, key, value)

    def plot(self):
        plt.figure()
        for obj in self.shapes:
            color = getattr(obj, 'color', 'blue')
            alpha = getattr(obj, 'alpha', 0.5)
            geom = getattr(obj, 'geometry', None)
            linewidth = getattr(obj, 'linewidth', 0.5)
            linestyle = getattr(obj, 'linestyle', '-')
            edgecolor = getattr(obj, 'edgecolor', 'black')
            label = getattr(obj, 'label', None)
            label_color = getattr(obj, 'label_color', 'black')
            

            if isinstance(geom, Polygon):
                x, y = geom.exterior.xy
                if alpha == 0:
                    plt.plot(x, y, color=edgecolor, linewidth=linewidth, linestyle=linestyle)

                plt.fill(x, y, facecolor=color, alpha=alpha, linewidth=linewidth, linestyle=linestyle, edgecolor=edgecolor)
                if label:
                    centroid = geom.centroid
                    plt.text(centroid.x, centroid.y, label, fontsize=10, color=label_color)

            elif isinstance(geom, LineString):
                x, y = geom.xy
                plt.plot(x, y, color=color, alpha=alpha)
                if label:
                    centroid = geom.centroid
                    midpoint = geom.interpolate(0.5, normalized=True)
                    plt.text(midpoint.x, midpoint.y, label, fontsize=10)

            elif isinstance(geom, Point):
                x, y = geom.xy
                plt.scatter(x, y, color=color, alpha=alpha)
                if label:
                    plt.text(x, y, label, fontsize=10)
            else:
                logging.error(f'Cannot plot {geom}')

    def add_shape(self, shape: BaseGeometry):
        self.shapes.append(shape)

    def add_gdf(
        self, 
        gdf: gpd.GeoDataFrame, 
        label: Optional[List[str]] = None,
        **kwargs
        ):
        for _, row in gdf.iterrows():
            geom = row.geometry
            shape = Vector(geom)

            # Apply styling from kwargs
            for key, value in kwargs.items():
                setattr(shape, key, value)

            # Set label if provided
            shape.label = row[label] if label else None

            # Add the shape to the collection
            self.add_shape(shape)

    def remove(
        self, 
        shape: BaseGeometry
        ):
        self.shapes.remove(shape)
    