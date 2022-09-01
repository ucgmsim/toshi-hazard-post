import numpy as np
import math
import logging

log = logging.getLogger(__name__)

def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    # Fast and numerically precise:
    variance = np.average((values-average)**2, weights=weights)
    return (average, math.sqrt(variance))


def weighted_quantile(values, quantiles, sample_weight=None, values_sorted=False, old_style=False):
    """Very close to numpy.percentile, but supports weights.
    NOTE: quantiles should be in [0, 1]!
    :param values: numpy.array with data
    :param quantiles: array-like with many quantiles needed. Can also be string 'mean' to calculate weighted mean
    :param sample_weight: array-like of the same length as `array`
    :param values_sorted: bool, if True, then will avoid sorting of
        initial array
    :param old_style: if True, will correct output to be consistent
        with numpy.percentile.
    :return: numpy.array with computed quantiles.
    """

    values = np.array(values)
    if sample_weight is None:
        sample_weight = np.ones(len(values))
    sample_weight = np.array(sample_weight)
    sample_weight = sample_weight / sum(sample_weight)

    get_mean = False
    get_std = False
    if 'mean' in quantiles:
        get_mean = True
        mean_ind = quantiles.index('mean')
        quantiles = quantiles[0:mean_ind] + quantiles[mean_ind + 1 :]
        mean = np.sum(sample_weight * values)
    if 'std' in quantiles:
        get_std = True
        std_ind = quantiles.index('std')
        quantiles = quantiles[0:std_ind] + quantiles[std_ind + 1 :]
        avg, std = weighted_avg_and_std(values, sample_weight)

    quantiles = np.array(
        [float(q) for q in quantiles]
    )  # TODO this section is hacky, need to tighten up API with typing
    # print(f'QUANTILES: {quantiles}')

    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), 'quantiles should be in [0, 1]'

    if not values_sorted:
        sorter = np.argsort(values)
        values = values[sorter]
        sample_weight = sample_weight[sorter]

    weighted_quantiles = np.cumsum(sample_weight) - 0.5 * sample_weight
    if old_style:
        # To be convenient with numpy.percentile
        weighted_quantiles -= weighted_quantiles[0]
        weighted_quantiles /= weighted_quantiles[-1]
    else:
        weighted_quantiles /= np.sum(sample_weight)

    wq = np.interp(quantiles, weighted_quantiles, values)
    if get_mean:
        wq = np.append(np.append(wq[0:mean_ind], np.array([mean])), wq[mean_ind:])
    if get_std:
        wq = np.append(np.append(wq[0:std_ind], np.array([std])), wq[std_ind:])

    return wq
