import numpy as np

def int_from_discrete_gaussian_dist(mean: float = 5.0, std: float = 2.0) -> int:
    return abs(np.random.normal(mean, std, 1)[0].round())
