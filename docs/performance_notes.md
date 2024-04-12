## Testing Speedup of different ways to represent the HazardBranch in the ValueStore

### Input File
```
[general]
compatibility_key = "A_A"
hazard_model_id = "SMALL_DEMO_MODEL"

[logic_trees]
# model_version = "NSHM_v1.0.4"

# alternativly, specify a path to logic tree files
srm_file = "demo/srm_logic_tree.json"
gmcm_file = "demo/gmcm_logic_tree_medium.json"

[site]
vs30s = [275]
# locations = ["WLG", "SRWG214", "-41.000~174.700", "myfile.csv"]
locations = ["-34.500~173.000"]

# alternativly specify a file with locations and vs30 values
# site_file = "sites.csv"

[calculation]
imts = ["PGA", "SA(1.0)"]
agg_types = ["mean", "cov", "std", "0.005", "0.01", "0.025"]
```

### 1. encode json asdict
```
chrisdc@hutl24256:~/.../CALCULATION/toshi-hazard-post$ time NZSHM22_THS_REPO=/home/chrisdc/NSHM/THS/pq-CDC2/ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2.toml
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation
2024-04-13 07:15:58,975 - toshi_hazard_post.version2.aggregation - INFO - getting sites
2024-04-13 07:15:58,975 - toshi_hazard_post.version2.aggregation - INFO - getting logic trees
2024-04-13 07:15:59,012 - toshi_hazard_post.version2.aggregation - INFO - building hazard logic tree
2024-04-13 07:15:59,012 - toshi_hazard_post.version2.aggregation - INFO - getting weights
2024-04-13 07:15:59,832 - toshi_hazard_post.version2.aggregation - INFO - time to calculate weights 0.82 seconds
2024-04-13 07:15:59,832 - toshi_hazard_post.version2.aggregation - INFO - getting levels
2024-04-13 07:15:59,832 - toshi_hazard_post.version2.aggregation - INFO - starting aggregation for 1 sites and 2 imts
2024-04-13 07:15:59,832 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: PGA
2024-04-13 07:15:59,832 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:15:59,840 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:16:00,635 - toshi_hazard_post.version2.data - INFO - loaded 425 realizations
2024-04-13 07:16:00,636 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.80 seconds
2024-04-13 07:16:00,636 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:16:21,143 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 20.51 seconds
2024-04-13 07:16:21,143 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:16:21,575 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 0.43 seconds
2024-04-13 07:16:21,575 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:16:21,689 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 21.86 seconds
2024-04-13 07:16:21,689 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: SA(1.0)
2024-04-13 07:16:21,689 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:16:21,697 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:16:22,305 - toshi_hazard_post.version2.data - INFO - loaded 425 realizations
2024-04-13 07:16:22,306 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.62 seconds
2024-04-13 07:16:22,306 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:16:43,542 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 21.24 seconds
2024-04-13 07:16:43,542 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:16:43,924 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 0.38 seconds
2024-04-13 07:16:43,924 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:16:43,924 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 22.23 seconds

real    0m45.931s
user    0m47.206s
sys     0m1.563s
```

### 2. asdict
```
    def registry_identity(self) -> str:
        return repr(asdict(self))
```
```
chrisdc@hutl24256:~/.../CALCULATION/toshi-hazard-post$ time NZSHM22_THS_REPO=/home/chrisdc/NSHM/THS/pq-CDC2/ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2.toml
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation
2024-04-13 07:21:19,578 - toshi_hazard_post.version2.aggregation - INFO - getting sites
2024-04-13 07:21:19,578 - toshi_hazard_post.version2.aggregation - INFO - getting logic trees
2024-04-13 07:21:19,613 - toshi_hazard_post.version2.aggregation - INFO - building hazard logic tree
2024-04-13 07:21:19,614 - toshi_hazard_post.version2.aggregation - INFO - getting weights
2024-04-13 07:21:20,365 - toshi_hazard_post.version2.aggregation - INFO - time to calculate weights 0.75 seconds
2024-04-13 07:21:20,365 - toshi_hazard_post.version2.aggregation - INFO - getting levels
2024-04-13 07:21:20,365 - toshi_hazard_post.version2.aggregation - INFO - starting aggregation for 1 sites and 2 imts
2024-04-13 07:21:20,365 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: PGA
2024-04-13 07:21:20,366 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:21:20,373 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:21:21,114 - toshi_hazard_post.version2.data - INFO - loaded 425 realizations
2024-04-13 07:21:21,114 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.75 seconds
2024-04-13 07:21:21,114 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:21:37,948 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 16.83 seconds
2024-04-13 07:21:37,948 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:21:38,270 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 0.32 seconds
2024-04-13 07:21:38,270 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:21:38,374 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 18.01 seconds
2024-04-13 07:21:38,374 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: SA(1.0)
2024-04-13 07:21:38,374 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:21:38,381 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:21:38,844 - toshi_hazard_post.version2.data - INFO - loaded 425 realizations
2024-04-13 07:21:38,844 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.47 seconds
2024-04-13 07:21:38,844 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:21:53,901 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 15.06 seconds
2024-04-13 07:21:53,901 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:21:54,218 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 0.32 seconds
2024-04-13 07:21:54,218 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:21:54,218 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 15.84 seconds

real    0m35.570s
user    0m36.480s
sys     0m1.407s
```

### 3. branch registry_identidies
```
    def registry_identity(self) -> str:
        return (
            self.source_branch.registry_identity +
            '|'.join([branch.registry_identity for branch in self.gmcm_branches])
        )
```
```
chrisdc@hutl24256:~/.../CALCULATION/toshi-hazard-post$ time NZSHM22_THS_REPO=/home/chrisdc/NSHM/THS/pq-CDC2/ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2.toml
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation
2024-04-13 07:25:40,280 - toshi_hazard_post.version2.aggregation - INFO - getting sites
2024-04-13 07:25:40,280 - toshi_hazard_post.version2.aggregation - INFO - getting logic trees
2024-04-13 07:25:40,328 - toshi_hazard_post.version2.aggregation - INFO - building hazard logic tree
2024-04-13 07:25:40,328 - toshi_hazard_post.version2.aggregation - INFO - getting weights
2024-04-13 07:25:41,225 - toshi_hazard_post.version2.aggregation - INFO - time to calculate weights 0.90 seconds
2024-04-13 07:25:41,225 - toshi_hazard_post.version2.aggregation - INFO - getting levels
2024-04-13 07:25:41,225 - toshi_hazard_post.version2.aggregation - INFO - starting aggregation for 1 sites and 2 imts
2024-04-13 07:25:41,225 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: PGA
2024-04-13 07:25:41,225 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:25:41,234 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:25:42,197 - toshi_hazard_post.version2.data - INFO - loaded 425 realizations
2024-04-13 07:25:42,197 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.97 seconds
2024-04-13 07:25:42,197 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:25:51,306 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 9.11 seconds
2024-04-13 07:25:51,306 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:25:52,858 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 1.55 seconds
2024-04-13 07:25:52,859 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:25:53,387 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 12.16 seconds
2024-04-13 07:25:53,387 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: SA(1.0)
2024-04-13 07:25:53,387 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:25:53,416 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:25:55,606 - toshi_hazard_post.version2.data - INFO - loaded 425 realizations
2024-04-13 07:25:55,607 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 2.22 seconds
2024-04-13 07:25:55,607 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:26:03,834 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 8.23 seconds
2024-04-13 07:26:03,835 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:26:05,407 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 1.57 seconds
2024-04-13 07:26:05,407 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:26:05,409 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 12.02 seconds

real    0m26.767s
user    0m29.751s
sys     0m2.448s
```

### Use 3 with full LT
```
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation
2024-04-13 07:27:04,085 - toshi_hazard_post.version2.aggregation - INFO - getting sites
2024-04-13 07:27:04,086 - toshi_hazard_post.version2.aggregation - INFO - getting logic trees
2024-04-13 07:27:04,289 - toshi_hazard_post.version2.aggregation - INFO - building hazard logic tree
2024-04-13 07:27:04,291 - toshi_hazard_post.version2.aggregation - INFO - getting weights
2024-04-13 07:27:51,191 - toshi_hazard_post.version2.aggregation - INFO - time to calculate weights 46.90 seconds
2024-04-13 07:27:51,192 - toshi_hazard_post.version2.aggregation - INFO - getting levels
2024-04-13 07:27:51,192 - toshi_hazard_post.version2.aggregation - INFO - starting aggregation for 1 sites and 2 imts
2024-04-13 07:27:51,192 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: PGA
2024-04-13 07:27:51,192 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:27:51,226 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:27:55,896 - toshi_hazard_post.version2.data - INFO - loaded 912 realizations
2024-04-13 07:27:55,897 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 4.71 seconds
2024-04-13 07:27:55,898 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:29:48,908 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 113.01 seconds
2024-04-13 07:29:48,908 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:29:53,409 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 4.50 seconds
2024-04-13 07:29:53,409 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:29:53,520 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 122.33 seconds
2024-04-13 07:29:53,520 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: SA(1.0)
2024-04-13 07:29:53,520 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations
2024-04-13 07:29:53,527 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 07:29:54,113 - toshi_hazard_post.version2.data - INFO - loaded 912 realizations
2024-04-13 07:29:54,114 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.59 seconds
2024-04-13 07:29:54,114 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates
2024-04-13 07:30:13,827 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 19.71 seconds
2024-04-13 07:30:13,828 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates
2024-04-13 07:30:18,392 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 4.56 seconds
2024-04-13 07:30:18,392 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result
2024-04-13 07:30:18,403 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 24.88 seconds

real    3m18.219s
user    3m18.289s
sys     0m4.423s
```