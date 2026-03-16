"""
Geografische Hilfsfunktionen.
"""
from __future__ import annotations

from math import asin, cos, radians, sin, sqrt


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Berechnet die Distanz zwischen zwei Punkten auf der Erde (in km).

    Verwendet die Haversine-Formel für Großkreis-Distanz.
    """
    r = 6371.0  # Erdradius in km
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return r * c
