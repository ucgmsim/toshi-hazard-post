import csv
import io
import itertools
import logging
from collections import namedtuple
from zipfile import ZipFile
from enum import Enum
from operator import mul
from functools import reduce


import numpy as np
import pandas as pd
from nzshm_common.location.code_location import CodedLocation

log = logging.getLogger(__name__)

class Dimension(Enum):
   mag = 'Mag'
   dist = 'Dist'
   trt = 'TRT'
   eps = 'Eps'

def disagg_df(rlz_names, dimensions):

    dimensions = list(map(str.lower, dimensions))

    mags = np.arange(5.25, 10.0, 0.5)
    epss = np.arange(-3,5,2) # epss = np.arange(-3.75,4.0,.5)
    dists = np.arange(5, 550, 10)
    trts = ['Active Shallow Crust', 'Subduction Interface', 'Subduction Intraslab']

    bins = dict(
        mag = list(map('{:0.3f}'.format,mags)),
        dist = list(map('{:0.3f}'.format,dists)),
        trt = trts,
        eps = list(map('{:0.3f}'.format,epss)),
    )
    bins = {k:v for k,v in bins.items() if k in dimensions}
    for rlz_name in rlz_names:
        bins[rlz_name] = [0]

    return pd.MultiIndex.from_product(bins.values(), names=bins.keys()).to_frame(index=False)


def get_location(header):
    """
    get the location from the disagg csv
    this is terrible and hacky and only temporory until disagg data is stored by THS
    """

    info = header[-1]
    start_lon = info.index('lon=') + 4
    tail = info[start_lon:]
    try:
        end_lon = tail.index(',')
        lon = tail[:end_lon]
    except:
        lon = tail[:]

    start_lat = info.index('lat=') + 4
    tail = info[start_lat:]
    try:
        end_lat = tail.index(',')
        lat = tail[:end_lat]
    except:
        lat = tail[:]

    location = CodedLocation(float(lat), float(lon), 0.001).code

    return location


def match_index(disaggs, values):

    ind = pd.Series(data=True,index=range(len(disaggs)))
    for name, value in values.items():
        ind = ind & disaggs[name].isin([value])
    return ind


def get_values_from_csv(disagg_data):

    values = {}
    for field in disagg_data._fields:
        if field not in ['mag','dist','trt','eps']: continue
        if field == 'trt':
            values['trt'] = disagg_data.trt
        else:
            values[field] = f'{float(getattr(disagg_data,field)):0.3f}'
    return values


def get_disagg(csv_archive, deagg_dimensions):
    """
    get the disagg data from the csv archive
    this is terrible and hacky and only temporory until disagg data is stored by THS
    assuming only 1 location and 1 imt
    """

    deagg_dimensions = list(map(str.lower, deagg_dimensions))
    filename = '_'.join([d.value for d in Dimension if d.name in deagg_dimensions]) + '-0_1.csv'

    with ZipFile(csv_archive) as zipf:
        with io.TextIOWrapper(zipf.open(filename), encoding="utf-8") as disagg_file:
            disagg_reader = csv.reader(disagg_file)
            header0 = next(disagg_reader)
            location = get_location(header0)

            header = next(disagg_reader)
            DisaggData = namedtuple("DisaggData", header, rename=True)
            rlz_names = header[len(deagg_dimensions)+2:]
            disaggs = disagg_df(rlz_names, deagg_dimensions)

            i = len(deagg_dimensions)
            j = i+2
            for row in disagg_reader:
                disagg_data = DisaggData(*row)
                values = get_values_from_csv(disagg_data)
                imt = disagg_data.imt
                ind = match_index(disaggs, values)
                if not any(ind):
                    exc_text = f'no index found for {csv_archive} row: {row}'
                    exc_text += f'\nvalues: {values}'
                    raise Exception(exc_text)
                disaggs.iloc[ind, i:] = list(map(float, row[j:]))

    disaggs_dict = {}
    for rlz in rlz_names:
        disaggs_dict[rlz[3:]] = disaggs[rlz].to_numpy(dtype='float64')
    
    return disaggs_dict, location, imt


def save_deaggs(deagg_data, loc, imt, poe, vs30, model_id, deagg_dimensions):

    dim = '_'.join(deagg_dimensions)
    deagg_filename = f'deagg_{model_id}_{loc}_{vs30}_{imt}_{int(poe*100)}_{dim}.npy'
    # with open(deagg_filename,'w') as jsonfile:
    #     json.dump(deagg_data, jsonfile)
    np.save(deagg_filename, deagg_data)
    log.info(f'saved deagg results to {deagg_filename}')
