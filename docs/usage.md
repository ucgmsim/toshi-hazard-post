# Usage

## Configuration

toshi-hazard-post requires some configuration to run. This can either be done via a combination of environment variables or a configuration file. Environment variables will override settings in the configuration file.

- THP_NUM_WORKERS: number of parallel processes to run (default: 1)
- THP_{RLZ|AGG}_FS: the filesystem to use for the {realization or aggregate} datastore (default: "LOCAL")
- THP_{RLZ|AGG}_LOCAL_DIR: the path to the local {realization or aggregate} datastore
- THP_{RLZ|AGG}_S3_BUCKET: the S3 bucket where the {realization or aggregate} datastore is kept
- THP_{RLZ|AGG}_AWS_REGION: the AWS region for {realization or aggregate} if using S3 datastore

Values for the filesystem variables can be `"LOCAL"` or `"AWS"` indicating the parquet files are stored on a local disk or in an S3 bucket.

## Using an input file to run the calculation

The standard way to run toshi-hazard-post is to use the `thp` command.

```console
$ thp aggregate [--config-file PATH] INPUT_FILE
```
The `--config-file` option allows users to specify a file to set the [configuration](#Configuration) variables.

The input file is a [toml](https://toml.io/en/) file that specifies the calculation arguments. 

```
[general]
compatibility_key = "A_A"
hazard_model_id = "DEMO_MODEL"

[logic_trees]
model_version = "NSHM_v1.0.4"

# alternativly, specify a path to logic tree files
# srm_file = "demo/srm_logic_tree_no_slab.json"
# gmcm_file = "demo/gmcm_logic_tree_medium.json"

[site]
vs30s = [275, 400]
locations = ["WLG", "SRWG214", "-41.000~174.700", "myfile.csv"]


[calculation]
imts = ["PGA", "SA(0.2)", "SA(0.5)", "SA(1.5)", "SA(3.0)", "SA(5.0)"]
agg_types = ["mean", "cov", "std", "0.1", "0.005", "0.01", "0.025"]
```

### [general]

- compatibility_key: this is a string used to identify entries in the realization database that were created using a compatible hazard engine, i.e. all hazard curves created with the same compatibility key can be directly compared to each other. Differences will be due to changes in e.g. location, ground motion models, sources, etc. Differences will not be due to the hazard calculation algorithm. 
- hazard_model_id: used to identify the model in the output, aggregation database

### [logic_trees]
Logic trees can be specified in one of two ways:

1. Use the logic trees (both SRM and GMCM) from an official New Zealand NSHM model. See the [nzhsm-model package documentation](https://gns-science.github.io/nzshm-model/usage/) for details.
2. Specify a path to SRM and GMCM logic tree files. See the [nzhsm-model documentation](https://gns-science.github.io/nzshm-model/file-format/) for the file format.

### [site]

### Manipulating calculation arguments programmatically