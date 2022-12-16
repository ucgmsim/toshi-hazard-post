"""A collection of calculation functions for hazard aggregation."""

import logging
import math
import time

from numba import jit
import numpy as np


log = logging.getLogger(__name__)

@jit(nopython=True)
def prob_to_rate(prob, inv_time):
    """Convert probability of exceedance to rate assuming Poisson distribution.
    
    Parameters
    ----------
    prob : numpy array_like
        probability of exceedance
    inv_time : float
        time period for probability (e.g. 1.0 for annual probability)
        
    Returns
    -------
    rate : numpy array_like
        return rate in inv_time
    """

    return -np.log(1 - prob) / inv_time


@jit(nopython=True)
def rate_to_prob(rate, inv_time):
    """Convert rate to probabiility of exceedance assuming Poisson distribution.
    
    Parameters
    ----------
    rate : numpy array_like
        rate over inv_time
    inv_time : float
        time period of rate (e.g. 1.0 for annual rate)
        
    Returns
    -------
    prob : numpy array_like
        probability of exceedance in inv_time
    """

    return 1.0 - np.exp(-inv_time * rate)


def weighted_avg_and_std(values, weights):
    """Calculate weighted average and standard deviation of an array.

    Parameters
    ----------
    values : numpy array_like
        array of values
    weights : Iterator[float]
        weights of values. Same length as values.

    Returns
    -------
    (mean, std) : (float, float)
        weighted mean, standard deviation
    """

    average = np.average(values, weights=weights)
    # Fast and numerically precise:
    variance = np.average((values - average) ** 2, weights=weights)
    return (average, math.sqrt(variance))


def calculate_mean(values, weight):
    """Calculate weighted mean
    
    Parameters
    ----------
    values : numpy array_like
        array of values
    weights : numpy array_like
        array of weights

    Returns
    -------
    mean : float
        weighted mean
    """

    return np.sum(weight * values)


def calculate_weighted_quantiles(values, weights, quantiles):
    """Calculate weighed quantiles of array

    Parameters
    ----------
    values : numpy array_like
        values of data
    weights : numpy array_like
        weights of values. Same length as values
    quantiles : Iterator[float]
        quantiles to be found. Values should be in [0,1]

    Returns
    -------
    weighed_quantiles : numpy array_like
        weighed quantiles
    """

    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    weighted_quantiles = np.cumsum(weights) - 0.5 * weights
    weighted_quantiles /= np.sum(weights)

    wq = np.interp(quantiles, weighted_quantiles, values)

    return wq
