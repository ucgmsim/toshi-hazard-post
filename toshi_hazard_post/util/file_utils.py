import csv
import io
import itertools
import logging
from collections import namedtuple
from zipfile import ZipFile

import numpy as np
import pandas as pd
from nzshm_common.location.code_location import CodedLocation

log = logging.getLogger(__name__)


def disagg_df(rlz_names):

    columns = ['imt', 'mag', 'dist', 'trt', 'eps'] + rlz_names
    # imts = ['PGA']
    mags = np.arange(5.25, 10.0, 0.5)
    epss = np.arange(-3,5,2)
    dists = np.arange(5, 550, 10)
    trts = ['Active Shallow Crust', 'Subduction Interface', 'Subduction Intraslab']
    index = range(len(mags) * len(dists) * len(trts) * len(epss))
    disaggs = pd.DataFrame(columns=columns, index=index)
    for i, (mag, dist, trt, eps) in enumerate(itertools.product(mags, dists, trts, epss)):
        disaggs.loc[i, 'mag'] = f'{mag:0.3}'
        disaggs.loc[i, 'dist'] = f'{int(dist)}'
        disaggs.loc[i, 'trt'] = trt
        disaggs.loc[i, 'eps'] = f'{int(eps)}'
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


def get_disagg(csv_archive):
    """
    get the disagg data from the csv archive
    this is terrible and hacky and only temporory until disagg data is stored by THS
    assuming only 1 location and 1 imt
    """

    with ZipFile(csv_archive) as zipf:
        with io.TextIOWrapper(zipf.open('Mag_Dist_TRT_Eps-0_1.csv'), encoding="utf-8") as disagg_file:
            disagg_reader = csv.reader(disagg_file)
            header0 = next(disagg_reader)
            location = get_location(header0)

            header = next(disagg_reader)
            DisaggData = namedtuple("DisaggData", header, rename=True)
            rlz_names = header[6:]
            disaggs = disagg_df(rlz_names)

            for row in disagg_reader:
                disagg_data = DisaggData(*row)
                imt = disagg_data.imt
                mag = f'{float(disagg_data.mag):0.3}'
                dist = f'{int(float(disagg_data.dist))}'
                eps = f'{int(float(disagg_data.eps))}'
                trt = disagg_data.trt
                ind = (disaggs['mag'].isin([mag])) & (disaggs['dist'].isin([dist])) & (disaggs['trt'].isin([trt])) & (disaggs['eps'].isin([eps]))
                if not any(ind):
                    exc_text = f'no index found for {csv_archive} row: {row}'
                    exc_text += f'\nimt: {imt}, mag: {mag}, dist: {dist}, eps: {eps}, trt: {trt}'
                    raise Exception(exc_text)
                disaggs.iloc[ind, 5:] = list(map(float, row[6:]))

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
