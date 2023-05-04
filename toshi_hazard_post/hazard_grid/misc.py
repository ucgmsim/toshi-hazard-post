from nzshm_common.grids import RegionGrid
from toshi_hazard_store import model
from nzshm_common.location import CodedLocation



def get_site_lists():
    for rg in RegionGrid:
        yield rg

def migrate():
    model.migrate()

def get_filter_locations(filter_sites):
    filter_locations = (
        [CodedLocation(*[float(s) for s in site.split('~')], resolution=0.2) for site in filter_sites.split('')]
        if filter_sites
        else []
    )