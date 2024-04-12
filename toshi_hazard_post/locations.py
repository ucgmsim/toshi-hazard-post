import csv

from collections import namedtuple
from pathlib import Path
from typing import Dict, List, Tuple

from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATION_LISTS, location_by_id

from toshi_hazard_post.hazard_aggregation.aggregation_config import AggregationConfig


def lat_lon(id):
    return (location_by_id(id)['latitude'], location_by_id(id)['latitude'])


def stat_test_missing() -> List[Tuple[float, float]]:

    locations = [
        # (-38.100, 176.800),
        (-42.400, 171.200),
        # (-42.100, 171.900),
    ]

    return [(round(loc[0], 1), round(loc[1], 1)) for loc in locations]


def locations_from_csv(locations_filepath):

    locations = []
    locations_filepath = Path(locations_filepath)
    with locations_filepath.open('r') as locations_file:
        reader = csv.reader(locations_file)
        Location = namedtuple("Location", next(reader), rename=True)
        for row in reader:
            location = Location(*row)
            locations.append((float(location.lat), float(location.lon)))
    return locations


def transpower_locs() -> List[Tuple[float, float]]:

    return [
        (-44.2511114772, 170.799858272),
        (-43.5385280381, 172.603100709),
        (-36.7388439935, 174.689089064),
        (-42.948244399, 171.567956091),
        (-38.6164425189, 176.142886043),
        (-41.6652838293, 173.203497909),
        (-38.0716797943, 175.642699731),
        (-43.9419307374, 171.800589632),
        (-43.2626786177, 172.623614354),
        (-38.3932714961, 176.023553641),
        (-44.6580044245, 170.354125621),
        (-46.2279074479, 169.771804195),
        (-46.2250295849, 168.842227893),
        (-44.5697503994, 170.198845189),
        (-41.4983392253, 173.931920322),
        (-37.1881846326, 174.998914182),
        (-40.2805723867, 175.637724057),
        (-35.848634854, 174.4791339),
        (-39.8683752997, 175.048065485),
        (-43.5315966868, 172.697560325),
        (-45.9408566853, 170.101589276),
        (-37.8705174885, 175.482972172),
        (-43.2130510348, 171.72695799),
        (-45.058988678, 169.189270096),
        (-43.3646092335, 171.528314249),
        (-41.3004852768, 174.769221388),
        (-39.0812151975, 174.086362945),
        (-42.7176377709, 172.876703772),
        (-45.1875207502, 169.310536326),
        (-42.4526724545, 171.301185868),
        (-40.1902762567, 176.030424581),
        (-37.9873733508, 176.830306665),
        (-46.3106884722, 168.777457325),
        (-39.5985368165, 176.761328783),
        (-45.0113569942, 168.741473232),
        (-41.2404125361, 174.916423084),
        (-37.2055485335, 174.736334337),
        (-46.1144805513, 168.912302257),
        (-42.4546012377, 171.214131533),
        (-41.1174664314, 175.437806848),
        (-37.7808418159, 175.306144964),
        (-41.1491739179, 174.978864247),
        (-36.8397025957, 174.618687001),
        (-36.8869494391, 174.660165593),
        (-37.9201771872, 175.765465921),
        (-37.5439649689, 175.152469784),
        (-43.5448398669, 171.982306625),
        (-39.0433768621, 174.236045544),
        (-39.5666886471, 174.310310857),
        (-45.8545215192, 170.474251079),
        (-41.8564968402, 171.953024762),
        (-46.3948155207, 168.393188791),
        (-43.5388319022, 172.511299433),
        (-43.3814646042, 172.638854426),
        (-38.076120811, 176.722276469),
        (-41.6730978361, 172.876035688),
        (-38.2713947594, 175.882947031),
        (-35.3966898147, 173.809927501),
        (-37.9243881109, 175.5365861),
        (-37.1908934823, 175.601774813),
        (-42.6351845264, 171.195248206),
        (-41.2592355796, 174.790179981),
        (-38.1334299616, 175.824471861),
        (-44.9326119245, 170.635950981),
        (-40.4092008259, 175.635549451),
        (-45.5211481446, 167.276414117),
        (-38.1135194993, 176.815016065),
        (-41.7891088006, 172.336058652),
        (-35.8749083288, 174.466832679),
        (-40.5173691736, 175.752494905),
        (-40.5760602727, 175.44864883),
        (-41.2007026233, 174.916971464),
        (-36.9625145645, 174.828088484),
        (-38.9943141868, 174.287898853),
        (-35.7669600808, 174.207945529),
        (-40.9776068289, 175.610760489),
        (-37.677140196, 176.213361715),
        (-40.1030983177, 175.36114196),
        (-36.0864977749, 174.358338264),
        (-39.6595824504, 175.703998282),
        (-46.3073701347, 168.358404146),
        (-39.158318456, 175.399281775),
        (-45.0326937896, 170.092538641),
        (-45.0688535404, 170.917488841),
        (-44.2722195404, 170.052239765),
        (-44.3009704167, 170.113667572),
        (-44.3445823693, 170.179305106),
        (-38.4087260299, 176.086884157),
        (-38.0346829765, 176.337336111),
        (-38.5217954027, 176.294880969),
        (-39.4266047278, 175.368631905),
        (-38.745384598, 175.181245218),
        (-39.3926236832, 173.928042747),
        (-36.9560779526, 174.862172612),
        (-42.831322886, 171.564477285),
        (-38.1299648603, 176.322015635),
        (-36.9254125251, 174.908486668),
        (-36.9060612986, 174.818575162),
        (-41.1091965752, 174.918129263),
        (-40.9249197859, 175.013027936),
        (-39.5509614661, 176.821859618),
        (-36.913526045, 174.726318036),
        (-38.1479453677, 176.226377485),
        (-45.4763883453, 169.319202935),
        (-39.1528527354, 175.837049415),
        (-43.326584125, 172.598955048),
        (-45.8915129198, 170.508085435),
        (-39.3348709226, 174.317998471),
        (-43.63737158, 172.45049085),
        (-41.3136013014, 173.243287011),
        (-44.7315222466, 171.137251757),
        (-36.6226546224, 174.683646562),
        (-36.9274738304, 174.826476106),
        (-37.0387156801, 174.932448283),
        (-37.7269926836, 176.131882506),
        (-44.3728747096, 171.233149093),
        (-44.0137629461, 170.460047233),
        (-44.124139634, 170.213820958),
        (-37.7425658023, 177.685658567),
        (-41.156312553, 174.870972801),
        (-38.9830889033, 175.774331547),
        (-45.8526009352, 170.434949095),
        (-37.816589973, 176.338016967),
        (-44.2240052431, 171.292803176),
        (-38.8904859381, 175.328329643),
        (-38.0029367837, 175.318824556),
        (-39.4671995495, 175.574489528),
        (-38.0755620886, 176.159582637),
        (-38.8063665661, 177.150890652),
        (-46.5827931793, 168.394396178),
        (-44.2776921371, 170.099940871),
        (-41.0990464302, 175.094467652),
        (-38.0254176454, 177.19205314),
        (-40.3153370976, 175.856901342),
        (-36.2840063928, 174.514126075),
        (-37.5665074877, 175.148079052),
        (-39.9388323003, 175.118704796),
        (-39.3842653759, 176.887914708),
        (-37.5686953156, 175.683713629),
        (-41.2613484287, 174.758525518),
        (-36.9882787211, 174.882909973),
        (-38.4204491595, 175.800772871),
        (-37.4158421866, 175.774228513),
        (-41.7175080628, 171.756682715),
        (-38.2915009148, 175.680856721),
        (-43.0524999228, 172.756526411),
        (-41.7511862161, 171.507651096),
        (-39.9191490639, 176.447172807),
        (-38.9971104264, 177.407020996),
        (-38.6242442284, 176.102800205),
        (-44.6924750507, 170.427824364),
        (-39.6072568152, 176.881348354),
        (-39.7541419342, 174.634605252),
        (-36.7648365011, 174.542533004),
        (-37.1249813055, 174.962831197),
        (-37.3166470044, 175.067948128),
        (-37.4795954074, 175.289338172),
        (-37.7279306081, 175.130092523),
        (-37.7419801682, 176.170308394),
        (-38.2515760458, 175.17764437),
        (-41.270589, 174.678531),
        (-42.3165246279, 171.541281246),
        (-44.5644275779, 170.190117567),
        (-41.2994022648, 174.188056536),
        (-42.7245831235, 170.983289791),
        (-36.9733586712, 174.960160682),
        (-44.8867371391, 170.957267678),
        (-44.8878698699, 170.799216641),
        (-36.8454997074, 174.762636226),
        (-38.35603301, 175.745517289),
        (-38.6132790379, 176.184474676),
        (-37.6508136912, 175.560417181),
        (-38.6308840184, 176.04568007),
        (-41.7646051985, 171.611352862),
        (-42.082062895, 171.845416751),
        (-40.3541934307, 175.773821699),
        (-36.7799514789, 174.747205424),
        (-38.4144987541, 175.790203655),
        (-43.4561651395, 172.084365339),
        (-41.2255921691, 174.717470517),
        (-41.29352, 174.63),
        (-41.3153544884, 174.810575319),
        (-41.1515297882, 174.979128442),
        (-44.5709604935, 170.104561025),
        (-37.0314335396, 174.924585834),
        (-43.4926065257, 172.54523532),
        (-38.6180546201, 176.046504909),
        (-39.0947093439, 174.341406914),
        (-39.20881137, 176.68438958),
        (-43.60808618, 172.21575421),
        (-38.6659323, 176.15792422),
        (-40.3515780112181, 175.61885191894),
        (-41.2879319462066, 174.77395442541),
        (-37.8326770813717, 175.301710934626),
        (-43.5390340647917, 172.601318094595),
        (-44.083322, 171.310844),
    ]


def stat_test_64() -> List[Tuple[float, float]]:

    locations = [
        (-42.117, 171.86),
        (-38.114, 176.817),
        (-39.717, 175.138),
        (-41.802, 172.318),
        (-42.449, 171.211),
        (-42.718, 170.964),
        (-42.949, 171.568),
        (-43.274, 172.596),
        (-43.483, 172.53),
        (-42.813, 173.275),
        (-43.566, 172.624),
        (-40.899, 176.221),
        (-43.227, 171.724),
        (-43.312, 172.381),
        (-43.49, 172.102),
        (-45.902, 170.493),
        (-43.667, 172.198),
        (-40.649, 175.709),
        (-40.214, 175.573),
        (-44.099, 170.829),
        (-38.666, 178.023),
        (-43.883, 169.044),
        (-43.54, 171.96),
        (-42.523, 172.83),
        (-41.077, 175.23),
        (-41.197, 174.892),
        (-43.715, 169.423),
        (-45.521, 167.278),
        (-41.211, 175.461),
        (-41.508, 173.944),
        (-41.265, 174.706),
        (-42.088, 173.257),
        (-43.706, 172.654),
        (-41.271, 173.284),
        (-39.507, 176.897),
        (-43.996, 168.661),
        (-43.445, 172.661),
        (-43.326, 172.038),
        (-40.914, 175.005),
        (-43.924, 171.234),
        (-40.302, 176.612),
        (-45.032, 168.663),
        (-43.751, 172.023),
        (-46.147, 167.473),
        (-43.809, 172.252),
        (-43.675, 172.318),
        (-43.607, 172.645),
        (-43.369, 172.495),
        (-46.537, 169.139),
        (-45.417, 167.719),
        (-38.373, 178.301),
        (-41.21, 174.276),
        (-37.633, 178.365),
        (-40.849, 172.821),
        (-38.807, 177.15),
        (-41.28, 174.778),
        (-41.231, 174.931),
        (-41.756, 171.6),
        (-41.827, 174.138),
        (-40.338, 175.87),
        (-41.284, 174.768),
        (-42.701, 172.8),
        (-39.944, 176.584),
        (-41.62, 173.351),
    ]

    return [(round(loc[0], 1), round(loc[1], 1)) for loc in locations]


def get_locations(config: AggregationConfig) -> List[Tuple[float, float]]:
    """Get list of locations.

    Parameters
    ----------
    config : AggregationConfig
        job configuration

    Returns
    -------
    locations : List[(float,float)]
        list of (latitude, longitude)
    """

    def lat_lon(id):
        return (location_by_id(id)['latitude'], location_by_id(id)['longitude'])

    locations: List[Tuple[float, float]] = []
    for location_spec in config.locations:
        if '~' in location_spec:
            locations.append(tuple(map(float, location_spec.split('~'))))  # type: ignore
        elif '_intersect_' in location_spec:
            spec0, spec1 = location_spec.split('_intersect_')
            loc0 = set(load_grid(spec0))
            loc1 = set(load_grid(spec1))
            loc01 = list(loc0.intersection(loc1))
            loc01.sort()
            locations += loc01
        elif '_diff_' in location_spec:
            spec0, spec1 = location_spec.split('_diff_')
            loc0 = set(load_grid(spec0))
            loc1 = set(load_grid(spec1))
            loc01 = list(loc0.difference(loc1))
            loc01.sort()
            locations += loc01
        elif location_by_id(location_spec):
            locations.append(lat_lon(location_spec))
        elif LOCATION_LISTS.get(location_spec):
            location_ids = LOCATION_LISTS[location_spec]["locations"]
            locations += [lat_lon(id) for id in location_ids]
        elif location_spec == "STAT_TEST_64":
            locations += stat_test_64()
        elif location_spec == 'STAT_TEST_MISSING':
            locations += stat_test_missing()
        elif location_spec == 'TP':
            locations += transpower_locs()
        elif Path(location_spec).exists():
            locations += locations_from_csv(location_spec)
        else:
            locations += load_grid(location_spec)
    if config.location_limit:
        locations = locations[: config.location_limit]
    return locations


def locations_by_degree(
    grid_points: List[Tuple[float, float]], grid_res: float, point_res: float
) -> Dict[str, List[CodedLocation]]:
    """Produce a dict of key_location:"""

    binned: Dict[str, CodedLocation] = dict()
    for pt in grid_points:
        bc = CodedLocation(*pt, point_res).downsample(grid_res).code
        if not binned.get(bc):
            binned[bc] = []
        binned[bc].append(CodedLocation(*pt, point_res).downsample(point_res))
    return binned


def locations_by_chunk(
    grid_points: List[Tuple[float, float]], point_res: float, chunk_size: int
) -> Dict[int, List[CodedLocation]]:
    chunked = dict()
    # chunk_size=25
    # for n in range(int(len(grid_points)/chunk_size) +2):
    #     chunk = grid_points[n * chunk_size: max(len(grid_points) - n+(n*chunk_size) , n+(n*chunk_size))]
    #     print(n, chunk)
    #     chunked[n] = [CodedLocation(*pt).downsample(point_res).code for pt in chunk]

    for ni in range(int((len(grid_points) - 1) / chunk_size) + 1):
        pts = grid_points[ni * chunk_size : ni * chunk_size + chunk_size]
        coded_pts = []
        if pts:
            for pt in pts:
                coded_pts.append(CodedLocation(*pt, point_res).downsample(point_res))
        chunked[ni] = coded_pts
    return chunked


def locations_nzpt2_and_nz34_binned(grid_res: float = 1.0, point_res: float = 0.001) -> Dict[str, List[CodedLocation]]:

    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    nz_0_2 = load_grid("NZ_0_2_NB_1_1")
    # nz34 = [(o['latitude'], o['longitude']) for o in LOCATIONS_BY_ID.values()]
    nz34 = [lat_lon(id) for id in LOCATION_LISTS['NZ']['locations']]

    grid_points = nz34 + nz_0_2
    return locations_by_degree(grid_points, grid_res, point_res)


def locations_nzpt2_chunked(
    grid_res: float = 1.0, point_res: float = 0.001, range: List[int] = []
) -> Dict[int, List[CodedLocation]]:

    chunk_size = 25
    grid_points = load_grid("NZ_0_2_NB_1_1")
    lbc = {}
    if range:
        for i, (k, v) in enumerate(locations_by_chunk(grid_points, point_res, chunk_size).items()):
            if (i >= range[0]) & (i <= range[1]):
                lbc[k] = v
    else:
        lbc = locations_by_chunk(grid_points, point_res, chunk_size)
    return lbc


def locations_nzpt2_and_nz34_chunked(grid_res: float = 1.0, point_res: float = 0.001) -> Dict[int, List[CodedLocation]]:

    chunk_size = 25
    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    nz_0_2 = load_grid("NZ_0_2_NB_1_1")
    nz34 = [lat_lon(id) for id in LOCATION_LISTS['NZ']['locations']]
    grid_points = nz34 + nz_0_2
    return locations_by_chunk(grid_points, point_res, chunk_size)


def locations_nz34_chunked(grid_res: float = 1.0, point_res: float = 0.001) -> Dict[int, List[CodedLocation]]:

    chunk_size = 2
    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    nz34 = [lat_lon(id) for id in LOCATION_LISTS['NZ']['locations']]
    grid_points = nz34
    return locations_by_chunk(grid_points, point_res, chunk_size)


def locations_nz2_chunked(grid_res: float = 1.0, point_res: float = 0.001) -> Dict[int, List[CodedLocation]]:
    '''used for testing'''

    chunk_size = 1
    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    cities = ['WLG', 'CHC', 'KBZ']
    nz34 = [lat_lon(id) for id in cities]
    grid_points = nz34
    return locations_by_chunk(grid_points, point_res, chunk_size)


if __name__ == "__main__":

    # For NZ_0_2: binning 1.0 =>  66 bins max 25 pts
    # For NZ 0_2: binning 0.5 => 202 bins max 9 pts

    # Settings for the THS rlz_query
    for key, locs in locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001).items():
        print(f"{key} => {locs}")
