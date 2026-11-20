from enum import Enum, auto


# Enumeration for Finland power generation and market data categories
class NordicDataCategory(Enum):
    ENTSOE_UNIT_GENERATION = auto()
    ENTSOE_FUEL_GENERATION = auto()
    ENTSOE_PRICE = auto()
    FINGRID_SERIES = auto()
    WEATHER_SERIES = auto()
