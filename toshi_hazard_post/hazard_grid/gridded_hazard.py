# coding: utf-8

import itertools
import logging
import multiprocessing
from collections import namedtuple
from dataclasses import dataclass
from typing import Dict, Iterable, List

import numpy as np
from nzshm_common.grids import RegionGrid
from nzshm_common.location import CodedLocation
from pynamodb.exceptions import PutError, QueryError
from toshi_hazard_store import model, query_v3
from toshi_hazard_store.query.gridded_hazard_query import get_gridded_hazard

from .gridded_poe import compute_hazard_at_poe

log = logging.getLogger(__name__)

INVESTIGATION_TIME = 50
SPOOF_SAVE = False
COV_AGG_KEY = 'cov'

GridHazTaskArgs = namedtuple(
    "GridHazTaskArgs", "location_keys poe_levels location_grid_id hazard_model_id vs30 imt agg"
)


@dataclass
class DistributedGridTaskArguments:
    location_grid_id: str
    poe_levels: List[float]
    hazard_model_ids: List[str]
    vs30s: List[float]
    imts: List[str]
    aggs: List[str]
    filter_locations: List[CodedLocation]
    force: bool


def process_gridded_hazard(location_keys, poe_levels, location_grid_id, hazard_model_id, vs30, imt, agg):
    grid_accel_levels: Dict[float, List] = {poe: [None for i in range(len(location_keys))] for poe in poe_levels}
    for haz in query_v3.get_hazard_curves(location_keys, [vs30], [hazard_model_id], imts=[imt], aggs=[agg]):
        accel_levels = [float(val.lvl) for val in haz.values]
        poe_values = [float(val.val) for val in haz.values]
        index = location_keys.index(haz.nloc_001)
        for poe_lvl in poe_levels:
            try:
                grid_accel_levels[poe_lvl][index] = compute_hazard_at_poe(
                    poe_lvl, accel_levels, poe_values, INVESTIGATION_TIME
                )
            except ValueError as err:
                log.warning(
                    'Error in compute_hazard_at_poe: %s, poe_lvl %s, haz_mod %s, vs30 %s, imt %s, agg %s'
                    % (err, poe_lvl, hazard_model_id, vs30, imt, agg)
                )
                continue
            log.debug('replaced %s with %s' % (index, grid_accel_levels[poe_lvl][index]))

    if agg == 'mean':
        grid_covs: Dict[float, List] = {poe: [None for i in range(len(location_keys))] for poe in poe_levels}
        for cov in query_v3.get_hazard_curves(
            location_keys, [vs30], [hazard_model_id], imts=[imt], aggs=[COV_AGG_KEY]
        ):
            index = location_keys.index(cov.nloc_001)
            cov_values = [val.val for val in cov.values]
            for poe_lvl in poe_levels:
                # cov_accel_levels = [val.lvl for val in cov.values]
                grid_covs[poe_lvl][index] = np.exp(
                    np.interp(np.log(grid_accel_levels[poe_lvl][index]), np.log(accel_levels), np.log(cov_values))
                )

        for poe_lvl in poe_levels:
            yield model.GriddedHazard.new_model(
                hazard_model_id=hazard_model_id,
                location_grid_id=location_grid_id,
                vs30=vs30,
                imt=imt,
                agg=COV_AGG_KEY,
                poe=poe_lvl,
                grid_poes=grid_covs[poe_lvl],
            )

    for poe_lvl in poe_levels:
        yield model.GriddedHazard.new_model(
            hazard_model_id=hazard_model_id,
            location_grid_id=location_grid_id,
            vs30=vs30,
            imt=imt,
            agg=agg,
            poe=poe_lvl,
            grid_poes=grid_accel_levels[poe_lvl],
        )


class GriddedHAzardWorkerMP(multiprocessing.Process):
    """A worker that batches and saves records to DynamoDB."""

    def __init__(self, task_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue

    def run(self):
        log.info("worker %s running." % self.name)
        proc_name = self.name

        while True:
            nt = self.task_queue.get()
            if nt is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                log.info('%s: Exiting' % proc_name)
                break

            try:
                for ghaz in process_gridded_hazard(*nt):
                    try:
                        ghaz_old = next(model.GriddedHazard.query(ghaz.partition_key, model.GriddedHazard.sort_key==ghaz.sort_key))
                        ghaz_old.grid_poes = ghaz.grid_poes
                        ghaz_old.save()
                    except StopIteration:
                        ghaz.save()
                    log.info('save %s' % ghaz)
            except QueryError as e:
                log.warn('QueryError, queries likely being throttled, skipping task: %s' % nt)
                log.warn(e)

            self.task_queue.task_done()
            log.info('%s task done.' % self.name)


def calc_gridded_hazard(
    location_grid_id: str,
    poe_levels: Iterable[float],
    hazard_model_ids: Iterable[str],
    vs30s: Iterable[float],
    imts: Iterable[str],
    aggs: Iterable[str],
    num_workers: int,
    filter_locations: Iterable[CodedLocation] = None,
    iter_method: str = 'product',
    force: bool = False,
):

    log.info(
        'calc_gridded_hazard( grid: %s poes: %s models: %s vs30s: %s imts: %s aggs: %s'
        % (location_grid_id, poe_levels, hazard_model_ids, vs30s, imts, aggs)
    )
    count = 0
    grid = RegionGrid[location_grid_id]

    # print(grid.resolution)
    locations = list(
        map(lambda grd_loc: CodedLocation(grd_loc[0], grd_loc[1], resolution=grid.resolution), grid.load())
    )

    if filter_locations:
        locations = list(set(locations).intersection(set(filter_locations)))

    location_keys = [loc.resample(0.001).code for loc in locations]

    log.debug('location_keys: %s' % location_keys)

    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()

    log.info('Creating %d workers' % num_workers)
    workers = [GriddedHAzardWorkerMP(task_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    if iter_method == 'product':
        iterator = itertools.product(hazard_model_ids, vs30s, imts, aggs)
    elif iter_method == 'zip':
        iterator = zip(hazard_model_ids, vs30s, imts, aggs)
    for (hazard_model_id, vs30, imt, agg) in iterator:

        if not force:
            existing_poes = []
            for ghaz in get_gridded_hazard([hazard_model_id], [location_grid_id], [vs30], [imt], [agg], poe_levels):
                if (ghaz.hazard_model_id == hazard_model_id) & (ghaz.vs30 == vs30) & (ghaz.imt == imt) & (ghaz.agg == agg):
                    existing_poes.append(ghaz.poe)
            if set(existing_poes) == set(poe_levels):
                log.info(
                    'griddded hazard for %s, %s, %s, %s %s already exists, skipping.'
                    % (hazard_model_id, vs30, imt, agg, poe_levels)
                )
                continue
        log.info('putting task for %s, %s, %s, %s %s.' % (hazard_model_id, vs30, imt, agg, poe_levels))
        t = GridHazTaskArgs(location_keys, poe_levels, location_grid_id, hazard_model_id, vs30, imt, agg)
        task_queue.put(t)
        count += 1

    # Add a poison pill for each to signal we've done everything
    for i in range(num_workers):
        task_queue.put(None)

    # Wait for all of the tasks to finish
    task_queue.join()

    log.info('calc_gridded_hazard() produced %s gridded_hazard rows ' % count)
