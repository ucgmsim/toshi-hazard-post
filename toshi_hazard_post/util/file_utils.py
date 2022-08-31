import csv
import io
import itertools
import json
import logging
import time
from collections import namedtuple
from zipfile import ZipFile

import numpy as np
import pandas as pd
from nzshm_common.location.code_location import CodedLocation

log = logging.getLogger(__name__)


def disagg_df(rlz_names):

    columns = ['imt', 'mag', 'dist', 'trt'] + rlz_names
    # imts = ['PGA']
    # mags = np.arange(5.1, 10.0, 0.2)
    mags = np.arange(5.25, 10.0, 0.5)
    dists = np.arange(5, 550, 10)
    trts = ['Active Shallow Crust', 'Subduction Interface', 'Subduction Intraslab']
    # index = range(len(imts) * len(mags) * len(dists) * len(trts))
    index = range(len(mags) * len(dists) * len(trts))
    disaggs = pd.DataFrame(columns=columns, index=index)
    # for i, (imt, mag, dist, trt) in enumerate(itertools.product(imts, mags, dists, trts)):
    for i, (mag, dist, trt) in enumerate(itertools.product(mags, dists, trts)):
        disaggs.loc[i, 'mag'] = f'{mag:0.3}'
        disaggs.loc[i, 'dist'] = f'{int(dist)}'
        disaggs.loc[i, 'trt'] = trt
        for rlz in rlz_names:
            disaggs.loc[i, rlz] = 0

    return disaggs


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


def get_disagg_mdt(csv_archive):
    """
    get the disagg data from the csv archive
    this is terrible and hacky and only temporory until disagg data is stored by THS
    assuming only 1 location and 1 imt
    """

    # because each hazard_id only has a single TRT, we must insert zeros for the other TRTs to treat all disagg arrays the same
    # assume the order and TRTs to be Active Shallow Crust, Subduction Interface, Subduction Intraslab
    TRT_ORDER = ['Active Shallow Crust', 'Subduction Interface', 'Subduction Intraslab']
    ntrt = len(TRT_ORDER)

    with ZipFile(csv_archive) as zipf:
        with io.TextIOWrapper(zipf.open('Mag_Dist_TRT-0_1.csv'), encoding="utf-8") as mag_dist_TRT_file:
            disagg_reader = csv.reader(mag_dist_TRT_file)
            header0 = next(disagg_reader)
            location = get_location(header0)

            header = next(disagg_reader)
            DisaggData = namedtuple("DisaggData", header, rename=True)
            rlz_names = header[5:]
            disaggs = disagg_df(rlz_names)

            for row in disagg_reader:
                disagg_data = DisaggData(*row)
                imt = disagg_data.imt
                mag = f'{float(disagg_data.mag):0.3}'
                dist = f'{int(float(disagg_data.dist))}'
                trt = disagg_data.trt
                # ind = (disaggs['imt'].isin([imt])) & (disaggs['mag'].isin([mag])) & (disaggs['dist'].isin([dist])) & (disaggs['trt'].isin([trt]))
                ind = (disaggs['mag'].isin([mag])) & (disaggs['dist'].isin([dist])) & (disaggs['trt'].isin([trt]))
                if not any(ind):
                    raise Exception(f'no index found for {csv_archive} row: {row}')
                disaggs.iloc[ind, 4:] = list(map(float, row[5:]))

    disaggs_dict = {}
    for rlz in rlz_names:
        disaggs_dict[rlz[3:]] = disaggs[rlz].to_numpy(dtype='float64')

    return disaggs_dict, location, imt


def save_deaggs(deagg_data, loc, imt, poe):

    deagg_filename = f'deagg_{loc}_{imt}_{int(poe*100)}.npy'
    # with open(deagg_filename,'w') as jsonfile:
    #     json.dump(deagg_data, jsonfile)
    np.save(deagg_filename, deagg_data)
    log.info(f'saved deagg results to {deagg_filename}')
