def mu_per_day_to_average_mw(energy_mu_per_day: float) -> float:
    """
    Convert daily electricity consumption from MU/day
    to average load in MW for 24 hours.

    Formula:
        Average Load (MW) = (MU/day × 1000) / 24
    """
    if energy_mu_per_day < 0:
        raise ValueError("Energy (MU/day) cannot be negative")

    return round((energy_mu_per_day * 1000) / 24, 2)



def calculate_mape(actual: float | None, predicted: float) -> float | None:
    """
    Returns MAPE % if actual is available and non-zero,
    otherwise returns None.
    """
    if actual is None or actual == 0:
        return None

    return round(abs((actual - predicted) / actual) * 100, 2)



