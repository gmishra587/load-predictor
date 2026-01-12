from django.contrib import admin
from power.models import DailyPredictionHistory, RegionHourlyLoad, StateDailyLoad, WeatherHourly

# Register your models here.
admin.site.register(RegionHourlyLoad)
admin.site.register(StateDailyLoad)
admin.site.register(DailyPredictionHistory)
admin.site.register(WeatherHourly)