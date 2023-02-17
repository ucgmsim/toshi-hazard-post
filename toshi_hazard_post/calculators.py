"""A collection of calculation functions for hazard aggregation."""

import logging
import math
from typing import List, Tuple, Union

import numpy as np
import numpy.typing as npt
from numba import jit

log = logging.getLogger(__name__)


@jit(nopython=True)
def prob_to_rate(prob: npt.NDArray, inv_time: float) -> npt.NDArray:
    """Convert probability of exceedance to rate assuming Poisson distribution.

    Parameters
    ----------
    prob
        probability of exceedance
    inv_time
        time period for probability (e.g. 1.0 for annual probability)

    Returns
    -------
    rate
        return rate in inv_time
    """

    return -np.log(1.0 - prob) / inv_time


@jit(nopython=True)
def rate_to_prob(rate: npt.NDArray, inv_time: float) -> npt.NDArray:
    """Convert rate to probabiility of exceedance assuming Poisson distribution.

    Parameters
    ----------
    rate
        rate over inv_time
    inv_time
        time period of rate (e.g. 1.0 for annual rate)

    Returns
    -------
    prob
        probability of exceedance in inv_time
    """

    return 1.0 - np.exp(-inv_time * rate)

def get_axis(array: npt.NDArray) -> int:
    axis =  1 if len(array.shape) > 1 else 0
    return axis


def weighted_avg_and_std(values: npt.NDArray, weights: npt.NDArray) -> Tuple[np.double, float]:
    """Calculate weighted average and standard deviation of an array.

    Parameters
    ----------
    values
        array of values
    weights
        weights of values. Same length as values.

    Returns
    -------
    mean
        weighted mean
    std
        standard deviation
    """

    # values = np.array(values)
    # axis = get_axis(values)
    axis = 0
    average = np.average(values, weights=weights, axis=axis)
    # Fast and numerically precise:
    variance = np.average((values - average) ** 2, weights=weights, axis=axis)
    return (average, np.sqrt(variance))


def calculate_weighted_quantiles(
    values: npt.NDArray, weights: npt.NDArray, quantiles: Union[List[float], npt.NDArray]
) -> npt.NDArray:
    """Calculate weighed quantiles of array

    Parameters
    ----------
    values
        values of data
    weights
        weights of values. Same length as values
    quantiles
        quantiles to be found. Values should be in [0,1]

    Returns
    -------
    weighed_quantiles
        weighed quantiles
    """

    nbranches, nlevels = values.shape

    # TODO: this uses more memory, are we getting a speed improvement in return?
    weights = np.vstack([weights.reshape(nbranches,1)]*nlevels)

    axis = 0
    sorter = np.argsort(values, axis=axis) # .reshape((values.shape[1],))
    values = np.take_along_axis(values, sorter, axis=axis)
    weights = np.take_along_axis(weights, sorter, axis=axis)

    weighted_quantiles = np.cumsum(weights, axis = axis) - 0.5 * weights
    weighted_quantiles /= np.sum(weights, axis = axis)

    wq = np.empty((len(quantiles), values.shape[1]))
    for i in range(values.shape[1]):
        wq[:, i] = np.interp(quantiles, weighted_quantiles[:, i], values[:, i])

    return wq
