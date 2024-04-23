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

### Old THP on glacier
```
chrisdc@glacier:~/.../DEV/toshi-hazard-post$ time NZSHM22_HAZARD_POST_WORKERS=34 poetry run python scripts/cli.py aggregate /home/chrisdc/NSHM/nzshm-model/Configs/slt_v9.toml
logging config from: toshi_hazard_post/logging.yaml                                                                                                                                                                logging config from: toshi_hazard_post/logging.yaml
Hazard post-processing pipeline as serverless AWS infrastructure.                                                                                                                                                  
mode: LOCAL                                                                                                                                                                                                        
{'hazard_model_id': 'TEST', 'stage': 'PROD', 'vs30s': [750], 'imts': ['PGA'], 'aggs': ['mean', 'cov', 'std', '0.005', '0.01', '0.025', '0.05', '0.1', '0.2', '0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '0.95
', '0.975', '0.99', '0.995'], 'locations': ['/home/chrisdc/NSHM/nzshm-model/WeakMotionSiteLocs_one.csv'], 'logic_tree_file': '/home/chrisdc/NSHM/nzshm-model/SRM_LTs/python/SLT_v9p0p0.py', 'gtids': ['R2VuZXJhbFRh
c2s6NjgwMjYyOA==']}                                                                                                                                                                                                
2024-04-12 21:55:25,631 - toshi_hazard_post.logic_tree.branch_combinator - INFO - built FlattenedSourceLogicTree                                                                                                   
2024-04-12 21:55:28,822 - toshi_hazard_post.logic_tree.branch_combinator - INFO - built HazardLogicTree                                                                                                            
2024-04-12 21:55:28,822 - toshi_hazard_post.logic_tree.branch_combinator - INFO - hazard ids: ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4OQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgyMg==', 'T3BlbnF1YWtlSGF6YXJ
kU29sdXRpb246NjgwMjc2OA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc3NQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc5OQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgwMA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMj
czNA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgwNA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgwOQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgxNg==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4NA==', 'T3BlbnF1Y
WtlSGF6YXJkU29sdXRpb246NjgwMjczNw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjcyNw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjcxMQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgwMw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRp
b246NjgwMjc5Mw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjczMg==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgxNQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc5Nw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgxMg==', 
'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjcwOQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgyMQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4Nw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgxNw==', 'T3BlbnF1YWtlSGF6YX
JkU29sdXRpb246NjgwMjgyMw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc0Mg==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc5MQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc5MA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwM
jgxMQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc5NQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4NQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgwNQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgwNw==', 'T3BlbnF1
YWtlSGF6YXJkU29sdXRpb246NjgwMjc5Mg==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4OA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgyNA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc5Ng==', 'T3BlbnF1YWtlSGF6YXJkU29sdXR
pb246NjgwMjgyMA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc1OQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjcwOA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4Ng==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc5OA==',
 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgwMg==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgxMw==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjczMQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgxOQ==', 'T3BlbnF1YWtlSGF6Y
XJkU29sdXRpb246NjgwMjgwMQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjgxOA==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjczNg==']                                                                                      
2024-04-12 21:55:28,836 - botocore.credentials - INFO - Found credentials in shared credentials file: ~/.aws/credentials                                                                                           
2024-04-12 21:55:28 INFO     Found credentials in shared credentials file: ~/.aws/credentials                                                                                                                      
time to load metadata 2.9059813669882715 seconds                                                                                                                                                                   
2024-04-12 21:55:31,728 - toshi_hazard_post.logic_tree.branch_combinator - INFO - loaded metadata                                                                                                                  
2024-04-12 21:55:33,790 - toshi_hazard_post.logic_tree.branch_combinator - INFO - set gmcm branches      
time to set gmcm branches 2.0616297251544893 seconds                                                     
2024-04-12 21:55:33,790 - toshi_hazard_post.hazard_aggregation.aggregation - INFO - finished building logic trees                                                                                                  
2024-04-12 21:55:33,792 - toshi_hazard_post.data_functions - INFO - get_levels locs[0]: -36.600~174.832 vs30: 750, id T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4OQ==                                                 
2024-04-12 21:55:33 INFO     get_levels locs[0]: -36.600~174.832 vs30: 750, id T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMjc4OQ==                                                                                        
2024-04-12 21:55:33,792 - toshi_hazard_store.query.hazard_query - INFO - hash_key -36.6~174.8            
2024-04-12 21:55:33 INFO     hash_key -36.6~174.8                                                        
2024-04-12 21:55:33,799 - botocore.credentials - INFO - Found credentials in shared credentials file: ~/.aws/credentials                                                                                           
2024-04-12 21:55:33 INFO     Found credentials in shared credentials file: ~/.aws/credentials            
2024-04-12 21:55:34,237 - toshi_hazard_post.hazard_aggregation.aggregation - INFO - get values for 1 locations and 49 hazard_solutions                                                                             
2024-04-12 21:55:34,238 - toshi_hazard_post.data_functions - INFO - loading 49 hazard IDs ...            
2024-04-12 21:55:34 INFO     loading 49 hazard IDs ...                                                   
2024-04-12 21:55:34,238 - toshi_hazard_store.query.hazard_query - INFO - hash_key -36.6~174.8            
2024-04-12 21:55:34 INFO     hash_key -36.6~174.8   
2024-04-12 21:56:26,582 - toshi_hazard_store.query.hazard_query - INFO - hash_key -36.6~174.8 has 912 hits                                                                                                         
2024-04-12 21:56:26 INFO     hash_key -36.6~174.8 has 912 hits                                           
2024-04-12 21:56:26,583 - toshi_hazard_store.query.hazard_query - INFO - Total 912 hits                  
2024-04-12 21:56:26 INFO     Total 912 hits                                                              
2024-04-12 21:56:26,697 - toshi_hazard_post.hazard_aggregation.aggregation - INFO - working on imt: PGA                                                                                                            
2024-04-12 21:56:26,697 - toshi_hazard_post.hazard_aggregation.aggregation - INFO - working on loc -36.600~174.832                                                                                                 
2024-04-12 21:56:38,181 - toshi_hazard_post.hazard_aggregation.aggregation - INFO - time to calculate hazard for one stride 11.484137553256005 seconds                                                             
2024-04-12 21:56:38,181 - toshi_hazard_post.hazard_aggregation.aggregation - INFO - imt: PGA took 11.484 secs                                                                                                      
2024-04-12 21:56:38,181 - toshi_hazard_post.hazard_aggregation.aggregation - INFO - process_location_list took 63.943 secs 

real    1m19.145s                                   
user    0m21.596s                                   
sys     0m3.779s  
```

### New THP on glacier
```
chrisdc@glacier:~/.../DEV/toshi-hazard-post$ time NZSHM22_THS_REPO=/home/chrisdc/NSHM/THS/pq-CDC2/ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2.toml
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation
2024-04-13 10:32:05,148 - toshi_hazard_post.version2.aggregation - INFO - getting sites . . .
2024-04-13 10:32:05,149 - toshi_hazard_post.version2.aggregation - INFO - getting logic trees . . . 
2024-04-13 10:32:05,321 - toshi_hazard_post.version2.aggregation - INFO - building hazard logic tree . . .
2024-04-13 10:32:05,323 - toshi_hazard_post.version2.aggregation - INFO - calculating weights . . . 
2024-04-13 10:32:14,000 - toshi_hazard_post.version2.aggregation - INFO - time to calculate weights 8.68 seconds
2024-04-13 10:32:14,000 - toshi_hazard_post.version2.aggregation - INFO - getting levels . . .
2024-04-13 10:32:14,000 - toshi_hazard_post.version2.aggregation - INFO - starting aggregation for 1 sites and 1 imts . . . 
2024-04-13 10:32:14,000 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: PGA
2024-04-13 10:32:14,000 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations . . .
2024-04-13 10:32:14,009 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-13 10:32:18,822 - toshi_hazard_post.version2.data - INFO - loaded 912 realizations
2024-04-13 10:32:18,822 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 4.82 seconds
2024-04-13 10:32:18,822 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates . . . 
2024-04-13 10:32:40,173 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 21.35 seconds
2024-04-13 10:32:40,173 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates . . . 
2024-04-13 10:32:45,161 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 4.99 seconds
2024-04-13 10:32:45,161 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result . . . 
2024-04-13 10:32:45,279 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 31.28 seconds

real    1m1.418s
user    0m45.976s
sys     0m4.532s
```

### Mini baseline
```
chrisdc@hutl24256:~/.../CALCULATION/toshi-hazard-post$ NZSHM22_THS_REPO=/home/chrisdc/NSHM/THS/pq-CDC2 poetry run thp aggregate demo/hazard_v2_mini.toml
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation OG
==============================================
2024-04-23 13:56:48,109 - toshi_hazard_post.version2.aggregation - INFO - getting sites . . .
2024-04-23 13:56:48,109 - toshi_hazard_post.version2.aggregation - INFO - getting logic trees . . . 
2024-04-23 13:56:48,199 - toshi_hazard_post.version2.aggregation - INFO - building hazard logic tree . . .
2024-04-23 13:56:48,200 - toshi_hazard_post.version2.aggregation - INFO - original method
2024-04-23 13:56:48,200 - toshi_hazard_post.version2.aggregation - INFO - starting aggregation for 1 sites and 1 imts . . . 
2024-04-23 13:56:48,200 - toshi_hazard_post.version2.aggregation - INFO - calculating weights . . . 
2024-04-23 13:56:48,603 - toshi_hazard_post.version2.aggregation - INFO - time to calculate weights 0.40 seconds
16200
2024-04-23 13:56:48,603 - toshi_hazard_post.version2.aggregation - INFO - getting levels . . .
2024-04-23 13:56:48,603 - toshi_hazard_post.version2.aggregation - INFO - site: Site(location=CodedLocation(lat=-34.5, lon=173.0, resolution=0.001), vs30=275), imt: PGA
2024-04-23 13:56:48,603 - toshi_hazard_post.version2.aggregation_calc - INFO - loading realizations . . .
2024-04-23 13:56:48,626 - toshi_hazard_post.version2.ths_mock - INFO - reading from local dataset
2024-04-23 13:56:51,941 - toshi_hazard_post.version2.data - INFO - loaded 420 realizations and 420 entries
2024-04-23 13:56:51,941 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 3.34 seconds
2024-04-23 13:56:51,941 - toshi_hazard_post.version2.aggregation_calc - INFO - building branch rates . . . 
2024-04-23 13:56:53,155 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 1.21 seconds
2024-04-23 13:56:53,156 - toshi_hazard_post.version2.aggregation_calc - INFO - calculating aggregates . . . 
2024-04-23 13:56:53,156 - toshi_hazard_post.version2.aggregation_calc - DEBUG - branch_rates with shape (16200, 44)
2024-04-23 13:56:53,156 - toshi_hazard_post.version2.aggregation_calc - DEBUG - weights with shape (16200,)
2024-04-23 13:56:53,156 - toshi_hazard_post.version2.aggregation_calc - DEBUG - agg_types ['mean', 'cov', 'std', '0.005', '0.01', '0.025']
2024-04-23 13:56:53,372 - toshi_hazard_post.version2.aggregation_calc - DEBUG - agg with shape (44, 6)
2024-04-23 13:56:53,373 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 0.22 seconds
2024-04-23 13:56:53,373 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result . . . 
2024-04-23 13:56:53,778 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 5.17 seconds
2024-04-23 13:56:53,778 - toshi_hazard_post.version2.aggregation - INFO - total OG time: 5.57852

```

### Using pyarrow.Table for the datastore
- no change to when data are loaded
- using python looping for building rates
```
2024-04-23 13:57:27,668 - toshi_hazard_post.version2.aggregation_calc_arrow - INFO - building branch rates for 16200 composite branches
2024-04-23 13:59:33,579 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to build_ranch_rates() 125.91 seconds

```
100x slower!

### Using pandas.DataFrame with the concat hash as the index
```
2024-04-23 16:21:24,202 - toshi_hazard_post.version2.aggregation_calc_arrow - INFO - building branch rates for 16200 composite branches
2024-04-23 16:21:24,859 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to build_ranch_rates() 0.66 seconds
```
