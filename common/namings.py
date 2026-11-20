from enum import Enum, auto


# Enumeration for Finland power generation, price, and weather data categories
class NordicDataCategory(Enum):
    ENTSOE_UNIT_GENERATION = auto()
    ENTSOE_FUEL_GENERATION = auto()
    ENTSOE_PRICE = auto()
    WEATHER_SERIES = auto()
