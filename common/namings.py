from enum import Enum, auto


# Enumeration for Nordic power and weather data categories
class NordicDataCategory(Enum):
    FINGRID_SERIES = auto()
    ENTSOE_PRICE = auto()
    WEATHER_SERIES = auto()
