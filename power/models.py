
from django.db import models


class RegionHourlyLoad(models.Model):
    region = models.CharField(max_length=5)
    datetime = models.DateTimeField()
    load_mw = models.FloatField()



class StateDailyLoad(models.Model):
    state = models.CharField(max_length=50)
    date = models.DateField()
    energy_mu = models.FloatField()



class WeatherHourly(models.Model):
    state = models.CharField(max_length=50)
    datetime = models.DateTimeField()
    temperature_c = models.FloatField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["state", "datetime"],
                name="unique_state_datetime_weather"
            )
        ]




class DailyPredictionHistory(models.Model):
    state = models.CharField(max_length=10)   # DL, MH, TN
    date = models.DateField()
    load_mw = models.FloatField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("state", "date")
        ordering = ["-date"]